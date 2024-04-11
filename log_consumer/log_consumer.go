package log_consumer

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/danthegoodman1/VirtualQueues/partitions"
	"github.com/rs/zerolog"
	"time"

	"github.com/danthegoodman1/VirtualQueues/gologger"
	"github.com/danthegoodman1/VirtualQueues/internal"
	"github.com/danthegoodman1/VirtualQueues/utils"
	"github.com/samber/lo"
	"github.com/twmb/franz-go/pkg/kadm"
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
		Client       *kgo.Client
		AdminClient  *kadm.Client
		AdminTicker  *time.Ticker
		Ready        bool

		shuttingDown bool
		closeChan    chan struct{}
	}

	PartitionMessage struct {
		NumPartitions int64
	}

	PartitionError struct {
		Topic     string
		Partition int32
		Err       error
	}
)

// sessionMS must be above 2 seconds (default 60_000)
func NewLogConsumer(instanceID, consumerGroup, topic string, seeds []string, sessionMS int64, partitionsMap *partitions.Map) (*LogConsumer, error) {
	consumer := &LogConsumer{
		MyPartitions:  partitionsMap,
		closeChan:     make(chan struct{}, 1),
		ConsumerGroup: consumerGroup,
		InstanceID:    instanceID,
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
		// kgo.DisableAutoCommit(), // TODO: See comment, need listeners
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
	consumer.AdminClient = kadm.NewClient(cl)
	consumer.AdminTicker = time.NewTicker(time.Second * 2)

	logger.Debug().Msg("sleeping to let consumer group to register")
	time.Sleep(time.Second)

	go consumer.pollTopicInfo()

	return consumer, nil
}

func (lc *LogConsumer) Shutdown() error {
	logger.Info().Msg("shutting down log consumer")
	lc.shuttingDown = true
	lc.AdminTicker.Stop()
	lc.closeChan <- struct{}{}
	lc.AdminClient.Close()
	lc.Client.CloseAllowingRebalance() // TODO: Maybe we want to manually mark something as going away if we are killing like this?
	return nil
}

func (lc *LogConsumer) pollTopicInfo() {
	// Get the actual partitions
	for {
		select {
		case <-lc.AdminTicker.C:
			lc.topicInfoLoop()
		case <-lc.closeChan:
			logger.Debug().Msg("poll topic info received on close chan, exiting")
			return
		}
	}
}

func (lc *LogConsumer) topicInfoLoop() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	logger := zerolog.Ctx(ctx)
	logger.UpdateContext(func(c zerolog.Context) zerolog.Context {
		return c.Str("instanceID", lc.InstanceID)
	})
	resp, err := lc.AdminClient.DescribeGroups(ctx, lc.ConsumerGroup)
	if err != nil {
		logger.Error().Err(err).Msg("error describing groups")
		return
	}
	memberID, _ := lc.Client.GroupMetadata()
	if len(resp.Sorted()) == 0 {
		logger.Warn().Msg("did not get any groups yet for group metadata")
		return
	}
	member, ok := lo.Find(resp.Sorted()[0].Members, func(item kadm.DescribedGroupMember) bool {
		return item.MemberID == memberID
	})
	if !ok {
		logger.Debug().Interface("resp", resp).Msg("Got admin describe groups response")
		logger.Warn().Msg("did not find myself in group metadata, cannot continue with partition mappings until I know what partitions I have")
		return
	}

	var partitionCount int64 = 0
	resp.AssignedPartitions().Each(func(t string, p int32) {
		partitionCount++
	})

	// TODO: Add topic change abort back in
	// currentVal := atomic.LoadInt64(&consumer.NumPartitions)
	// if currentVal != partitionCount {
	//	// We can't continue now
	//	logger.Fatal().Msgf("number of partitions changed in Kafka topic! I have %d, but topic has %d aborting so it's not longer safe!!!!!", consumer.NumPartitions, partitionCount)
	//	atomic.StoreInt64(&consumer.NumPartitions, partitionCount)
	// }

	assigned, _ := member.Assigned.AsConsumer()
	if len(assigned.Topics) == 0 {
		lc.Ready = false
		logger.Warn().Interface("assigned", assigned).Msg("did not find any assigned topics, can't make changes")
		return
	}
	myPartitions := assigned.Topics[0].Partitions
	news, gones := lo.Difference(myPartitions, partitions.ListPartitions(lc.MyPartitions))
	logger.Debug().Msgf("total partitions (%d),  my partitions (%d)", partitionCount, len(myPartitions))
	if len(news) > 0 {
		logger.Info().Msgf("got new partitions: %+v", news)
		for _, np := range news {
			lc.MyPartitions.Store(np, true)
		}
	}
	if len(gones) > 0 {
		logger.Info().Msgf("dropped partitions: %+v", gones)
	}

	for _, gonePart := range gones {
		lc.MyPartitions.Delete(gonePart)
	}

	// Set the current partitions
	internal.Metric_Partitions.Set(float64(len(myPartitions)))
}
