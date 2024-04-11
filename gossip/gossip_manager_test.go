package gossip

import (
	"github.com/danthegoodman1/VirtualQueues/partitions"
	"testing"
	"time"
)

func TestGossipManager(t *testing.T) {
	pm1 := partitions.Map{}
	gm1, err := NewGossipManager(&pm1, "localhost:10100", "", 11000)
	if err != nil {
		t.Fatal(err)
	}

	pm2 := partitions.Map{}
	gm2, err := NewGossipManager(&pm2, "localhost:10200", "localhost:11000", 12000)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * 5)

	t.Log(gm1.Node)
	t.Log(gm2.Node)
}
