package gossip

import (
	"encoding/json"
	"sync"
)

type delegate struct {
	GossipManager *Manager
	mu            *sync.RWMutex
	items         map[string]string
}

func (d *delegate) NodeMeta(limit int) []byte {
	return []byte{}
}

func (d *delegate) NotifyMsg(b []byte) {
	go handleDelegateMsg(d, b)
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.GossipManager.broadcasts.GetBroadcasts(overhead, limit)
}

func (d *delegate) LocalState(join bool) []byte {
	d.mu.RLock()
	m := d.items
	d.mu.RUnlock()
	b, _ := json.Marshal(m)
	return b
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	logger.Debug().Msg("merging remote state")
	if len(buf) == 0 {
		return
	}
	if !join {
		return
	}
	var m map[string]string
	if err := json.Unmarshal(buf, &m); err != nil {
		return
	}
	d.mu.Lock()
	for k, v := range m {
		d.items[k] = v
	}
	d.mu.Unlock()
}

func handleDelegateMsg(d *delegate, b []byte) {
	logger.Trace().Str("nodeID", d.GossipManager.Node.ID).Str("broadcast", string(b)).Msg("Got msg")
	if len(b) == 0 {
		return
	}

	var msg Message
	err := json.Unmarshal(b, &msg)
	if err != nil {
		logger.Error().Err(err).Str("msg", string(b)).Msg("error unmarshaling gossip message")
		return
	}

	switch msg.MsgType {
	case AdvertiseMessage:
		d.GossipManager.setRemotePartitions(msg.Partitions, msg.Addr)
	default:
		logger.Error().Str("msg", string(b)).Msg("unknown gossip message")
	}
}
