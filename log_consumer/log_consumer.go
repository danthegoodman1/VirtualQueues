package log_consumer

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/danthegoodman1/VirtualQueues/partitions"
	"github.com/danthegoodman1/VirtualQueues/syncx"
	"golang.org/x/sync/errgroup"
	"sync"
	"time"

	"github.com/danthegoodman1/VirtualQueues/gologger"
	"github.com/danthegoodman1/VirtualQueues/utils"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/tlscfg"
)

var (
	logger = gologger.NewLogger()
)

type (
	// LogConsumer is a single consumer of log, belonging to a single consumer group.
	// It also manages gossip participation
	LogConsumer struct {
		InstanceID    string
		ConsumerGroup string

		// MyPartitions are the partitions that are managed on this node
		MyPartitions *partitions.Map
		// PartitionMap is all partitions
		// Map of remote addresses for a given partition
		remotePartitions map[int32]string
		remotePartMu     *sync.RWMutex
		Client           *kgo.Client
		Ready            bool

		shuttingDown bool

		seedBrokers   []string
		topic         string
		advertiseAddr string

		// Map of consumers to their offsets. Key is consumerKey
		consumerOffsets   map[consumerKey]*ConsumerOffset
		consumerOffsetsMu *sync.Mutex

		// Map of consumer to partition, used for clearing consumer offsets when a partition is dropped. Key is consumerKey
		partitionConsumers syncx.Map[consumerKey, int32]
	}

	PartitionMessage struct {
		NumPartitions int64
	}

	PartitionError struct {
		Topic     string
		Partition int32
		Err       error
	}

	ConsumerOffset struct {
		Offset int64
	}
)

var ErrPollFetches = errors.New("error polling fetches")

// sessionMS must be above 2 seconds (default 60_000)
func NewLogConsumer(instanceID, consumerGroup, topic, advertiseAddr string, seeds []string, sessionMS int64, partitionsMap *partitions.Map) (*LogConsumer, error) {
	consumer := &LogConsumer{
		MyPartitions:       partitionsMap,
		ConsumerGroup:      consumerGroup,
		InstanceID:         instanceID,
		seedBrokers:        seeds,
		topic:              topic,
		consumerOffsets:    map[consumerKey]*ConsumerOffset{},
		consumerOffsetsMu:  &sync.Mutex{},
		partitionConsumers: syncx.Map[consumerKey, int32]{},
		remotePartMu:       &sync.RWMutex{},
		remotePartitions:   map[int32]string{},
		advertiseAddr:      advertiseAddr,
	}
	logger.Debug().Msgf("using partition log %s for seeds %+v", topic, seeds)
	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.ClientID("virtual_queues"),
		kgo.InstanceID(instanceID),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(topic),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)), // force murmur2, same as in utils
		kgo.SessionTimeout(time.Millisecond * time.Duration(sessionMS)),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // always reset offsets on connect
		kgo.OnPartitionsAssigned(consumer.partitionAssigned),
		kgo.OnPartitionsRevoked(consumer.partitionRemoved),
		kgo.OnPartitionsLost(consumer.partitionRemoved),
	}
	if utils.Env_KafkaUsername != "" && utils.Env_KafkaPassword != "" {
		logger.Debug().Msg("using kafka auth")
		opts = append(opts, kgo.SASL(scram.Auth{
			User: utils.Env_KafkaUsername,
			Pass: utils.Env_KafkaPassword,
		}.AsSha256Mechanism()))
	}
	if utils.Env_KafkaTLS {
		if utils.Env_KafkaTLSCAPath != "" {
			logger.Debug().Msgf("using kafka TLS with CA path %s", utils.Env_KafkaTLSCAPath)
			tlsCfg, err := tlscfg.New(
				tlscfg.MaybeWithDiskCA(utils.Env_KafkaTLSCAPath, tlscfg.ForClient),
			)
			if err != nil {
				return nil, fmt.Errorf("error in kgo.NewClient (mutations.tls): %w", err)
			}
			opts = append(opts, kgo.DialTLSConfig(tlsCfg))
		} else {
			logger.Debug().Msg("using kafka TLS")
			opts = append(opts, kgo.DialTLSConfig(&tls.Config{}))
		}
	}
	cl, err := kgo.NewClient(
		opts...,
	)
	if err != nil {
		return nil, fmt.Errorf("error in kgo.NewClient: %w", err)
	}
	consumer.Client = cl

	go consumer.launchPollRecordLoop()

	return consumer, nil
}

func (lc *LogConsumer) Shutdown() error {
	logger.Info().Msg("shutting down log consumer")
	lc.shuttingDown = true
	lc.Client.CloseAllowingRebalance()
	return nil
}

func (lc *LogConsumer) partitionAssigned(ctx context.Context, client *kgo.Client, added map[string][]int32) {
	for _, partitions := range added {
		for _, partition := range partitions {
			lc.MyPartitions.Store(partition, true)
		}
	}
}

func (lc *LogConsumer) partitionRemoved(ctx context.Context, client *kgo.Client, lost map[string][]int32) {
	for _, partitions := range lost {
		for _, partition := range partitions {
			lc.MyPartitions.Delete(partition)
			lc.DropPartitionConsumers(partition)
		}
	}
}

// launchPollRecordLoop is launched in a goroutine
func (lc *LogConsumer) launchPollRecordLoop() {
	for !lc.shuttingDown {
		err := lc.pollConsumerOffsets(context.Background())
		if err != nil {
			logger.Error().Err(err).Msg("error polling for records")
		}
	}
}

// pollConsumerOffsets is where we poll for cacheable consumer offsets
func (lc *LogConsumer) pollConsumerOffsets(c context.Context) error {
	ctx, cancel := context.WithTimeout(c, time.Second*5)
	defer cancel()
	fetches := lc.Client.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		if len(errs) == 1 {
			if errors.Is(errs[0].Err, context.DeadlineExceeded) {
				logger.Debug().Msg("got no records")
				return nil
			}
			if errors.Is(errs[0].Err, kgo.ErrClientClosed) {
				return nil
			}
		}
		return fmt.Errorf("got errors when fetching: %+v :: %w", errs, ErrPollFetches)
	}

	g := errgroup.Group{}
	fetches.EachPartition(func(part kgo.FetchTopicPartition) {
		g.Go(func() error {
			consumerMap := map[string]ConsumerOffsetRecord{}
			for _, record := range part.Records {
				var consumerRecord ConsumerOffsetRecord
				err := json.Unmarshal(record.Value, &consumerRecord)
				if err != nil {
					logger.Debug().Msg("failed to unmarshal consumer record, continuing")
				}

				if consumerRecord.Consumer != "" && consumerRecord.Queue != "" && consumerRecord.Offset > 0 {
					// This is probably a consumer record
					consumerMap[consumerRecord.Consumer] = consumerRecord
				}
			}

			// Set the consumer offsets
			lc.consumerOffsetsMu.Lock()
			defer lc.consumerOffsetsMu.Unlock()
			for _, offsetRecord := range consumerMap {
				ck := createConsumerKey(offsetRecord.Queue, offsetRecord.Consumer)
				lc.consumerOffsets[ck] = &ConsumerOffset{
					Offset: offsetRecord.Offset,
				}
			}

			return nil
		})
	})
	err := g.Wait()
	return err
}

func (lc *LogConsumer) GetPartitionsMap() map[int32]string {
	partMap := map[int32]string{}
	lc.remotePartMu.RLock()
	defer lc.remotePartMu.RUnlock()
	for part, addr := range lc.remotePartitions {
		partMap[part] = addr
	}

	// Remote partitions does not include self, copy that in
	for _, partition := range partitions.ListPartitions(lc.MyPartitions) {
		partMap[partition] = lc.advertiseAddr
	}

	return partMap
}
