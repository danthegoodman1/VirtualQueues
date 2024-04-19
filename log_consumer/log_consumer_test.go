package log_consumer

import (
	"github.com/danthegoodman1/VirtualQueues/partitions"
	"testing"
	"time"
)

func TestGettingPartitions(t *testing.T) {
	topic := "testing"
	offsetsTopic := "testing_offsets"
	partitionTopic := "testing_p"
	pm1 := partitions.Map{}
	pm2 := partitions.Map{}
	lc1, err := NewLogConsumer("lc1", "cg1", topic, offsetsTopic, partitionTopic, "localhost:11000", []string{"localhost:19092", "localhost:29092", "localhost:39092"}, 60000, &pm1)
	if err != nil {
		t.Fatal(err)
	}

	lc2, err := NewLogConsumer("lc2", "cg1", topic, offsetsTopic, partitionTopic, "localhost:12000", []string{"localhost:19092", "localhost:29092", "localhost:39092"}, 60000, &pm2)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * 5)
	t.Log(partitions.ListPartitions(lc1.MyPartitions))
	t.Log(partitions.ListPartitions(lc2.MyPartitions))
	t.Log(lc1.partitions)
	t.Log(lc2.partitions)
}
