package log_consumer

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/twmb/franz-go/pkg/kadm"
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

	vr := VirtualRecord{
		Queue:  queue,
		Record: base64.StdEncoding.EncodeToString([]byte(recordVal)),
	}

	record := &kgo.Record{
		Key:   []byte(queue),
		Value: vr.MustEncode(),
		Topic: lc.dataTopic,
	}

	res := lc.DataClient.ProduceSync(ctx, record)
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("error in ProductSync: %w", err)
	}

	return nil
}

func (lc *LogConsumer) DeleteQueue(ctx context.Context, queue string) error {
	// TODO: Scan offsets for a queue from the data partition
	// TODO: In batches, delete data from queue
	adm := kadm.NewClient(lc.DataClient)
	adm.DeleteRecords(ctx, map[string]map[int32]kadm.Offset{})

	// TODO: Get offsets for consumers
	// TODO: Delete consumers
}
