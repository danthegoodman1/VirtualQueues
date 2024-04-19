package log_consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
)

func (lc *LogConsumer) DropPartitionConsumers(partition int32) {
	var consumersToDrop []consumerKey
	lc.partitionConsumers.Range(func(ck consumerKey, p int32) bool {
		if partition == p {
			consumersToDrop = append(consumersToDrop, ck)
		}
		return true
	})

	if len(consumersToDrop) == 0 {
		return
	}

	lc.consumerOffsetsMu.Lock()
	defer lc.consumerOffsetsMu.Unlock()
	for _, consumer := range consumersToDrop {
		lc.partitionConsumers.Delete(consumer)
		delete(lc.consumerOffsets, consumer)
	}
}

type VirtualRecord struct {
	Queue string `json:"q"`
	// base64 encoded bytes
	Record string `json:"r"`
}

func (vr VirtualRecord) MustEncode() []byte {
	b, err := json.Marshal(vr)
	if err != nil {
		panic(err)
	}

	return b
}

type VirtualRecordWithOffset struct {
	VirtualRecord
	Offset int64
}

func (lc *LogConsumer) ConsumeQueueFromOffset(ctx context.Context, queue string, partition int32, offset, maxRecords int64, recordHandler func(record VirtualRecordWithOffset) error) error {
	// Create new dynamic client
	client, err := kgo.NewClient(
		kgo.SeedBrokers(lc.seedBrokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			lc.dataTopic: {
				partition: kgo.NewOffset().At(offset),
			},
		}),
	)
	if err != nil {
		return fmt.Errorf("error in kgo.NewClient: %w", err)
	}

	fetches := client.PollFetches(ctx)
	err = fetches.Err()
	if err != nil {
		return fmt.Errorf("error in fetches.Err(): %w", err)
	}

	iter := fetches.RecordIter()
	i := int64(0)
	for !iter.Done() && i < maxRecords {
		rec := iter.Next()

		var vr VirtualRecord
		err := json.Unmarshal(rec.Value, &vr)
		if err != nil {
			logger.Debug().Msg("failed to unmarshal consumer record, continuing")
		}

		if vr.Queue == queue && vr.Record != "" {
			// This is probably a virtual record
			err = recordHandler(VirtualRecordWithOffset{
				VirtualRecord: vr,
				Offset:        rec.Offset,
			})
			if err != nil {
				return fmt.Errorf("error in recordHandler: %w", err)
			}
			// Only count if this is a record from their queue
			i++
		}

	}

	return nil
}

type ConsumerOffsetRecord struct {
	Offset   int64  `json:"o"`
	Consumer string `json:"c"`
	Queue    string `json:"q"`
}

func (c ConsumerOffsetRecord) MustEncode() []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}

	return b
}

func (lc *LogConsumer) WritePartitionConsumerOffset(ctx context.Context, partition int32, queue, consumer string, offset int64) error {
	offsetRecord := ConsumerOffsetRecord{
		Consumer: consumer,
		Offset:   offset,
		Queue:    queue,
	}
	record := &kgo.Record{
		Key:   []byte(queue),
		Value: offsetRecord.MustEncode(),
		Topic: lc.offsetTopic,
	}
	res := lc.DataClient.ProduceSync(ctx, record)
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("error in ProduceSync: %w", err)
	}

	// Store it in mem cache
	ck := createConsumerKey(queue, consumer)
	_, exists := lc.partitionConsumers.LoadOrStore(ck, partition)

	lc.consumerOffsetsMu.Lock()
	defer lc.consumerOffsetsMu.Unlock()

	if !exists {
		// New consumer
		lc.consumerOffsets[ck] = &ConsumerOffset{
			Offset: offset,
		}
	} else {
		lc.consumerOffsets[ck].Offset = offset
	}
	return nil
}

func (lc *LogConsumer) GetConsumerOffset(queue, consumer string) *ConsumerOffset {
	ck := createConsumerKey(queue, consumer)
	lc.consumerOffsetsMu.Lock()
	defer lc.consumerOffsetsMu.Unlock()
	if offset, exists := lc.consumerOffsets[ck]; exists {
		return offset
	}

	return nil
}
