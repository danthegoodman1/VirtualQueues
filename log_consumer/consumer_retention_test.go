package log_consumer

import (
	"context"
	"github.com/danthegoodman1/VirtualQueues/partitions"
	"github.com/danthegoodman1/VirtualQueues/utils"
	"testing"
	"time"
)

func TestConsumerRetention(t *testing.T) {
	t.Log("Test short retention")
	topic := "testing"
	offsetsTopic := "testing_offsets"
	partitionTopic := "testing_p"
	pm1 := partitions.Map{}
	lc1, err := NewLogConsumer("lc1", "cg1", topic, offsetsTopic, partitionTopic, "localhost:11000", []string{"localhost:19092", "localhost:29092", "localhost:39092"}, 60000, &pm1, ConsumerRetention(time.Second, time.Millisecond*100, 0))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	queue := "blah12" // gives partition 0 when 4 partitions
	partition := utils.GetPartition(queue, 4)

	// Write the consumer offset
	t.Log("writing consumer offset")
	consumer := "test_consumer_recovery"
	err = lc1.WritePartitionConsumerOffset(context.Background(), partition, queue, consumer, 1)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * 2)

	// Verify that it has expired
	t.Log("checking consumer offsets")
	consumerOffset := lc1.GetConsumerOffset(queue, consumer)
	if consumerOffset != nil {
		t.Fatal("got consumer offset even though it expired")
	}

	// Try restarting it
	err = lc1.WritePartitionConsumerOffset(context.Background(), partition, queue, consumer, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Stop it
	t.Log("shutting down to test recovery consumer expiry")
	err = lc1.Shutdown()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)

	pm1 = partitions.Map{}
	lc1, err = NewLogConsumer("lc1", "cg1", topic, offsetsTopic, partitionTopic, "localhost:11000", []string{"localhost:19092", "localhost:29092", "localhost:39092"}, 60000, &pm1, ConsumerRetention(time.Second, time.Millisecond*100, 0))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * 3)

	// Verify that it has expired
	t.Log("checking consumer offsets")
	consumerOffset = lc1.GetConsumerOffset(queue, consumer)
	if consumerOffset != nil {
		t.Fatal("got consumer offset even though it expired")
	}
}
