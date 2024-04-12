package log_consumer

import (
	"context"
	"errors"
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
