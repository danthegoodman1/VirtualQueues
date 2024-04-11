package main

import (
	"github.com/danthegoodman1/VirtualQueues/gossip"
	"github.com/danthegoodman1/VirtualQueues/log_consumer"
	"github.com/danthegoodman1/VirtualQueues/partitions"
	"testing"
	"time"
)

func TestLogConsumerGossipManager(t *testing.T) {
	topic := "testing"
	pm1 := partitions.Map{}
	pm2 := partitions.Map{}
	lc1, err := log_consumer.NewLogConsumer("lc1", "cg1", topic, []string{"localhost:19092", "localhost:29092", "localhost:39092"}, 60000, &pm1)
	if err != nil {
		t.Fatal(err)
	}
	defer lc1.Shutdown()

	lc2, err := log_consumer.NewLogConsumer("lc2", "cg1", topic, []string{"localhost:19092", "localhost:29092", "localhost:39092"}, 60000, &pm2)
	if err != nil {
		t.Fatal(err)
	}
	defer lc2.Shutdown()

	gm1, err := gossip.NewGossipManager(&pm1, "localhost:10100", "", 11000)
	if err != nil {
		t.Fatal(err)
	}

	gm2, err := gossip.NewGossipManager(&pm2, "localhost:10200", "localhost:11000", 12000)
	if err != nil {
		t.Fatal(err)
	}
	defer gm2.Shutdown()
	defer gm1.Shutdown()

	time.Sleep(time.Second * 5)
	t.Log("lc1", partitions.ListPartitions(lc1.MyPartitions))
	t.Log("lc2", partitions.ListPartitions(lc2.MyPartitions))
	t.Log("gm1 partition map", gm1.GetPartitionsMap())
	t.Log("gm2 partition map", gm2.GetPartitionsMap())
}
