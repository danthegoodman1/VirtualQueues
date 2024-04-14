package api

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/danthegoodman1/VirtualQueues/log_consumer"
	"github.com/danthegoodman1/VirtualQueues/partitions"
	"github.com/danthegoodman1/VirtualQueues/utils"
	"github.com/samber/lo"
	"testing"
	"time"
)

func TestSimpleSequence(t *testing.T) {
	topic := "testing"
	partitionsTopic := "testing"
	pm1 := partitions.Map{}
	pm2 := partitions.Map{}
	lc1, err := log_consumer.NewLogConsumer("lc1", "cg1", topic, partitionsTopic, "localhost:11000", []string{"localhost:19092", "localhost:29092", "localhost:39092"}, 60000, &pm1)
	if err != nil {
		t.Fatal(err)
	}
	defer lc1.Shutdown()

	lc2, err := log_consumer.NewLogConsumer("lc2", "cg1", topic, partitionsTopic, "localhost:12000", []string{"localhost:19092", "localhost:29092", "localhost:39092"}, 60000, &pm2)
	if err != nil {
		t.Fatal(err)
	}
	defer lc2.Shutdown()

	time.Sleep(time.Second * 3)
	t.Log("lc1", partitions.ListPartitions(lc1.MyPartitions))
	t.Log("lc2", partitions.ListPartitions(lc2.MyPartitions))
	t.Log("lc1 partition map", lc1.GetPartitionsMap())
	t.Log("lc2 partition map", lc2.GetPartitionsMap())
	if len(lc1.GetPartitionsMap()) != 4 {
		t.Fatal("lc1 did not have 4 partitions, did you put in 4 partitions?")
	}
	if len(lc2.GetPartitionsMap()) != 4 {
		t.Fatal("lc2 did not have 4 partitions, did you put in 4 partitions?")
	}

	// Product record
	var part0LC *log_consumer.LogConsumer
	if lo.Contains(partitions.ListPartitions(lc1.MyPartitions), 0) {
		part0LC = lc1
	} else {
		part0LC = lc2
	}

	queue := "blah12" // gives partition 0 when 4 partitions
	partition := utils.GetPartition(queue, 4)
	if partition != 0 {
		t.Fatal("queue was not partition 0")
	}

	// Verify we can't use conflicting name
	err = part0LC.PublishRecord(context.Background(), "something::yeah", `{"hey":"ho"}`)
	if !errors.Is(err, log_consumer.ErrQueueNameHasReservedSeq) {
		t.Fatal("was not ErrQueueNameHasReservedSeq, got", err)
	}

	// Correctly publish a record
	err = part0LC.PublishRecord(context.Background(), queue, `{"hey":"ho"}`)
	if err != nil {
		t.Fatal(err)
	}

	// Consume some foundRecords
	foundRecords := 0
	err = part0LC.ConsumeQueueFromOffset(context.Background(), queue, partition, 0, 2, func(record log_consumer.VirtualRecordWithOffset) error {
		b, err := base64.StdEncoding.DecodeString(record.Record)
		if err != nil {
			return fmt.Errorf("error in DecodeString: %w", err)
		}
		t.Log("got record", record.Offset, string(b))
		foundRecords++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if foundRecords == 0 {
		t.Fatal("did not find any records when consuming from the partition from offset 0")
	}

	// Write the consumer offset
	consumer := "test_consumer"
	err = part0LC.WritePartitionConsumerOffset(context.Background(), partition, queue, consumer, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Write more records
	err = part0LC.PublishRecord(context.Background(), queue, `{"hey":"ho"}`)
	if err != nil {
		t.Fatal(err)
	}

	// Consume from the offset
	consumerOffset := part0LC.GetConsumerOffset(queue, consumer)
	if consumerOffset == nil {
		t.Fatal("got nil consumer offset")
	}

	foundRecords = 0
	err = part0LC.ConsumeQueueFromOffset(context.Background(), queue, partition, consumerOffset.Offset, 2, func(record log_consumer.VirtualRecordWithOffset) error {
		b, err := base64.StdEncoding.DecodeString(record.Record)
		if err != nil {
			return fmt.Errorf("error in DecodeString: %w", err)
		}
		t.Log("got record", record.Offset, string(b))
		foundRecords++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if foundRecords == 0 {
		t.Fatal("did not find any records when consuming from the partition from the consumer offset record")
	}
}
