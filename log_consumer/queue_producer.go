package log_consumer

import (
	"context"
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"strings"
)

var (
	ErrQueueNameHasReservedSeq = errors.New("cannot use '::' in queue name")
)

func (lc *LogConsumer) PublishRecord(ctx context.Context, queue string) error {
	// Ensure we don't break serialization of consumerKeys
	if strings.Contains(queue, "::") {
		return ErrQueueNameHasReservedSeq
	}

	// TODO: do

	return nil
}

func (lc *LogConsumer) WritePartitionConsumerOffset(ctx context.Context, partition int32, queue, consumer string, offset int64) {
	// TODO: write record to kafka
	record := &kgo.Record{
		Key:   []byte(queue),
		Value: jsonB, // TODO: special encoding so first few bytes we can tell if consumer offset record
		Topic: lc.topic,
	}
	lc.Client.ProduceSync(ctx)
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
