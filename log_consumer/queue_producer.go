package log_consumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"strings"
)

var (
	ErrQueueNameHasReservedSeq = errors.New("cannot use '::' in queue name")
)

func (lc *LogConsumer) PublishRecord(ctx context.Context, queue string, recordVal string) error {
	// Ensure we don't break serialization of consumerKeys
	if strings.Contains(queue, "::") {
		return ErrQueueNameHasReservedSeq
	}

	record := &kgo.Record{
		Key:   []byte(queue),
		Value: []byte(recordVal),
		Topic: lc.topic,
	}

	res := lc.Client.ProduceSync(ctx, record)
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("error in ProductSync: %w", err)
	}

	return nil
}
