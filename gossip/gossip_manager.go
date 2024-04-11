package gossip

import (
	"encoding/json"
	"fmt"
	"github.com/danthegoodman1/VirtualQueues/gologger"
	"github.com/danthegoodman1/VirtualQueues/partitions"
	"github.com/danthegoodman1/VirtualQueues/utils"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

var (
	logger = gologger.NewLogger()
)

type Manager struct {
	// The partition ID
	Node       *Node
	broadcasts *memberlist.TransmitLimitedQueue

	MemberList *memberlist.Memberlist

	Partitions *partitions.Map

	// Map of remote addresses for a given partition
	remotePartitions map[int32]string
	remotePartMu     *sync.RWMutex

	broadcastTicker *time.Ticker
	closeChan       chan struct{}
}

type Node struct {
	// The partition ID
	ID               string
	AdvertiseAddress string
	AdvertisePort    string
	LastUpdated      time.Time
}

func NewGossipManager(pm *partitions.Map) (gm *Manager, err error) {
	if utils.Env_AdvertiseAddr == "" {
		logger.Warn().Msg("ADVERTISE_ADDR not provided, disabling gossip")
		return nil, nil
	}
	advertiseHost, advertisePort, err := net.SplitHostPort(utils.Env_AdvertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("error splitting advertise address: %w", err)
	}

	myNode := &Node{
		ID:               utils.Env_AdvertiseAddr,
		AdvertiseAddress: advertisePort,
		AdvertisePort:    advertiseHost,
		LastUpdated:      time.Now(),
	}

	gm = &Manager{
		Node:             myNode,
		Partitions:       pm,
		closeChan:        make(chan struct{}, 1),
		broadcastTicker:  time.NewTicker(time.Millisecond * time.Duration(utils.Env_GossipBroadcastMS)),
		remotePartitions: map[int32]string{},
		remotePartMu:     &sync.RWMutex{},
	}

	var config *memberlist.Config
	if strings.Contains(utils.Env_AdvertiseAddr, "localhost") {
		config = memberlist.DefaultLocalConfig()
	} else {
		config = memberlist.DefaultLANConfig()
	}

	config.BindPort = int(utils.Env_GossipPort)
	config.Events = &eventDelegate{
		gm: gm,
	}
	if !utils.Env_GossipDebug {
		config.Logger = nil
		config.LogOutput = VoidWriter{}
	}
	config.Delegate = &delegate{
		GossipManager: gm,
		mu:            &sync.RWMutex{},
		items:         map[string]string{},
	}
	config.Name = utils.Env_AdvertiseAddr

	gm.MemberList, err = memberlist.Create(config)
	if err != nil {
		logger.Error().Err(err).Msg("Error creating memberlist")
		return nil, err
	}

	existingMembers := strings.Split(utils.Env_GossipPeers, ",")
	if len(existingMembers) > 0 && existingMembers[0] != "" {
		// Join existing nodes
		joinedHosts, err := gm.MemberList.Join(existingMembers)
		if err != nil {
			return nil, fmt.Errorf("error in MemberList.Join: %w", err)
		}
		logger.Info().Int("joinedHosts", joinedHosts).Msg("Successfully joined gossip cluster")
	} else {
		logger.Info().Msg("Starting new gossip cluster")
	}

	gm.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return gm.MemberList.NumMembers()
		},
		RetransmitMult: 3,
	}

	node := gm.MemberList.LocalNode()
	logger.Info().Str("name", node.Name).Str("addr", node.Address()).Int("port", int(node.Port)).Msg("Node started")

	gm.broadcastAdvertiseMessage()
	go gm.startBroadcastLoop()

	return gm, nil
}

func (gm *Manager) broadcastAdvertiseMessage() {
	b, err := json.Marshal(Message{
		Addr:       utils.Env_AdvertiseAddr,
		Partitions: partitions.ListPartitions(gm.Partitions),
		MsgType:    AdvertiseMessage,
	})
	if err != nil {
		logger.Fatal().Err(err).Msg("error marshaling advertise message, exiting")
	}
	gm.broadcasts.QueueBroadcast(&broadcast{
		msg:    b,
		notify: nil,
	})
}

func (gm *Manager) startBroadcastLoop() {
	logger.Debug().Msg("starting broadcast loop")
	for {
		select {
		case <-gm.broadcastTicker.C:
			gm.broadcastAdvertiseMessage()
		case <-gm.closeChan:
			logger.Debug().Msg("broadcast ticker received on close channel, exiting")
			return
		}
	}
}

func (gm *Manager) Shutdown() error {
	gm.broadcastTicker.Stop()
	gm.closeChan <- struct{}{}
	err := gm.MemberList.Leave(time.Second * 10)
	if err != nil {
		return fmt.Errorf("error in MemberList.Leave: %w", err)
	}
	err = gm.MemberList.Shutdown()
	if err != nil {
		return fmt.Errorf("error in MemberList.Shutdown: %w", err)
	}
	return nil
}

// setRemotePartitions sets the current partitions for an address, cleaning any changes
func (gm *Manager) setRemotePartitions(newPartitions []int32, newAddr string) {
	logger.Debug().Msgf("adding remote partitions %+v at %s", newPartitions, newAddr)

	gm.remotePartMu.Lock()
	defer gm.remotePartMu.Unlock()

	// Temp map for O(1) lookup when iterating
	newPartitionsMap := map[int32]bool{}
	for _, part := range newPartitions {
		newPartitionsMap[part] = true
		gm.remotePartitions[part] = newAddr
	}

	// anything we find that we need to remove
	var toRemove []int32

	for partition, addr := range gm.remotePartitions {
		if addr == newAddr {
			if _, exists := newPartitionsMap[partition]; !exists {
				toRemove = append(toRemove, partition)
			}
		}
	}

	logger.Debug().Str("newAddr", newAddr).Ints32("toRemove", toRemove).Msg("removing partitions from map from gossip update")
	for _, partition := range toRemove {
		delete(gm.remotePartitions, partition)
	}
}

func (gm *Manager) removePartitionsForAddr(addr string) {
	logger.Debug().Msgf("removing all partitions at %s", addr)
	gm.remotePartMu.Lock()
	defer gm.remotePartMu.Unlock()
	// not sure if we can modify a map in place so being safe
	var parts []int32
	for part, addrs := range gm.remotePartitions {
		if addrs == addr {
			parts = append(parts, part)
		}
	}
	for _, part := range parts {
		delete(gm.remotePartitions, part)
	}
}

func (gm *Manager) GetRemotePartitionAddr(partition int32) (addr string, exists bool) {
	gm.remotePartMu.RLock()
	defer gm.remotePartMu.RUnlock()
	addr, exists = gm.remotePartitions[partition]
	return
}
