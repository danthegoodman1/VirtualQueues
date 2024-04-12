package log_consumer

import (
	"context"
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

func (lc *LogConsumer) SetPartitionConsumerOffset(partition int32, queue, consumer string, offset int64) {
	ck := createConsumerKey(queue, consumer)
	_, stored := lc.partitionConsumers.LoadOrStore(ck, partition)

	lc.consumerOffsetsMu.Lock()
	defer lc.consumerOffsetsMu.Unlock()

	if stored {
		// New consumer
		lc.consumerOffsets[ck] = &ConsumerOffset{
			Offset: offset,
		}
	} else {
		lc.consumerOffsets[ck].Offset = offset
	}
}

func (lc *LogConsumer) ConsumePartitionFromOffset(ctx context.Context, partition int32, offset, maxRecords int64, recordHandler func(record *kgo.Record)) error {
	// Create new dynamic client
	client, err := kgo.NewClient(
		kgo.SeedBrokers(lc.seedBrokers...),
	)
	if err != nil {
		return fmt.Errorf("error in kgo.NewClient: %w", err)
	}

	client.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		lc.topic: {
			partition: kgo.NewOffset().At(offset),
		},
	})

	fetches := client.PollFetches(ctx)
	err = fetches.Err()
	if err != nil {
		return fmt.Errorf("error in fetches.Err(): %w", err)
	}

	iter := fetches.RecordIter()
	i := int64(0)
	for !iter.Done() && i < maxRecords {
		recordHandler(iter.Next())
		i++
	}

	return nil
}
