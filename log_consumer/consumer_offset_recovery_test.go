package log_consumer

import (
	"context"
	"github.com/danthegoodman1/VirtualQueues/partitions"
	"github.com/danthegoodman1/VirtualQueues/utils"
	"testing"
	"time"
)

func TestConsumerOffsetRecovery(t *testing.T) {
	topic := "testing"
	offsetsTopic := "testing_offsets"
	partitionsTopic := "testing_p"
	numPartitions := uint32(4)
	pm1 := partitions.Map{}
	lc1, err := NewLogConsumer("lc1", "cg1", topic, offsetsTopic, partitionsTopic, "localhost:11000", []string{"localhost:19092", "localhost:29092", "localhost:39092"}, 60000, &pm1, numPartitions)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)

	queue := "blah12" // gives partition 0 when 4 partitions
	partition := utils.GetPartition(queue, 4)
	if partition != 0 {
		t.Fatal("queue was not partition 0")
	}

	// Write the consumer offset
	t.Log("writing consumer offset")
	consumer := "test_consumer_recovery"
	err = lc1.WritePartitionConsumerOffset(context.Background(), partition, queue, consumer, 1)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("shutting down consumer")

	// Stop it
	err = lc1.Shutdown()
	if err != nil {
		t.Fatal(err)
	}

	t.Log("restarting consumer")
	pm1 = partitions.Map{}
	lc1, err = NewLogConsumer("lc1", "cg1", topic, offsetsTopic, partitionsTopic, "localhost:12000", []string{"localhost:19092", "localhost:29092", "localhost:39092"}, 60000, &pm1, numPartitions)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * 3)

	// Verify that it has recovered
	t.Log("checking consumer offsets")
	consumerOffset := lc1.GetConsumerOffset(queue, consumer)
	if consumerOffset == nil {
		t.Fatal("did not get consumer offset")
	}

	if consumerOffset.Offset != 1 {
		t.Fatal("did not get expected consumer offset")
	}
}
