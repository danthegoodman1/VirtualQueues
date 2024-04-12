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

func (lc *LogConsumer) ConsumePartitionFromOffset(ctx context.Context, partition int32, offset, maxRecords int64, recordHandler func(record *kgo.Record) error) error {
	// Create new dynamic client
	client, err := kgo.NewClient(
		kgo.SeedBrokers(lc.seedBrokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			lc.topic: {
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
		err = recordHandler(iter.Next())
		if err != nil {
			return fmt.Errorf("error in recordHandler: %w", err)
		}
		i++
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
		Value: offsetRecord.MustEncode(), // TODO: special encoding so first few bytes we can tell if consumer offset record earier
		Topic: lc.topic,
	}
	res := lc.Client.ProduceSync(ctx, record)
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
