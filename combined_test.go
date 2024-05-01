package main

import (
	"github.com/danthegoodman1/VirtualQueues/log_consumer"
	"github.com/danthegoodman1/VirtualQueues/partitions"
	"testing"
	"time"
)

func TestCombined(t *testing.T) {
	topic := "testing"
	offsetsTopic := "testing_offsets"
	partitionsTopic := "testing_p"
	pm1 := partitions.Map{}
	pm2 := partitions.Map{}
	numPartitions := uint32(4)
	lc1, err := log_consumer.NewLogConsumer("lc1", "cg1", topic, offsetsTopic, partitionsTopic, "localhost:11000", []string{"localhost:19092", "localhost:29092", "localhost:39092"}, 60000, &pm1, numPartitions)
	if err != nil {
		t.Fatal(err)
	}
	defer lc1.Shutdown()

	lc2, err := log_consumer.NewLogConsumer("lc2", "cg1", topic, offsetsTopic, partitionsTopic, "localhost:12000", []string{"localhost:19092", "localhost:29092", "localhost:39092"}, 60000, &pm2, numPartitions)
	if err != nil {
		t.Fatal(err)
	}
	defer lc2.Shutdown()

	time.Sleep(time.Second * 5)
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
}
