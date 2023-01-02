package cluster

import (
	"github.com/hdt3213/godis/config"
	"sort"
	"sync"
	"time"
)

const slotCount int = 16384

type Gossip struct {
	Cluster      *Cluster
	mu           sync.Mutex
	selfNodeID   string
	selfEpoch    uint64
	clusterEpoch uint64
	// slotId -> hostingNode
	slots          []*Slot
	nodes          map[string]*Node
	lastHeardTimes map[string]time.Time // nodeId -> last time of receive peer's ping
}

// PickNode returns the node id hosting the given slot.
// If the slot is migrating, return the node which is importing the slot
func (gossip *Gossip) PickNode(slotID uint32) *Node {
	slot := gossip.slots[int(slotID)]
	return gossip.nodes[slot.NodeID]
}

// NewNode creates a new Node when a node request self node for joining cluster
func (gossip *Gossip) NewNode(addr string) *Node {
	// todo: avoid duplicate address
	node := &Node{
		ID:    addr,
		Addr:  addr,
		Slots: nil,
		Flags: 0,
	}
	gossip.nodes[node.ID] = node
	return node
}

func (gossip *Gossip) GetTopology() map[string]*Node {
	return gossip.nodes
}

// StartAsSeed starts cluster as seed node
func (gossip *Gossip) StartAsSeed(addr string) (string, error) {
	selfNodeID := config.Properties.AnnounceAddress()
	gossip.slots = make([]*Slot, slotCount)
	// claim all slots
	for i := range gossip.slots {
		gossip.slots[i] = &Slot{
			ID:     uint32(i),
			NodeID: selfNodeID,
		}
	}
	gossip.selfNodeID = selfNodeID
	gossip.nodes = make(map[string]*Node)
	gossip.nodes[selfNodeID] = &Node{
		ID:    selfNodeID,
		Addr:  addr,
		Slots: gossip.slots,
		Flags: 0,
	}
	return selfNodeID, nil
}

// LoadTopology loads topology, used in join cluster
func (gossip *Gossip) LoadTopology(selfNodeId string, nodes map[string]*Node) {
	// make sure gossip.slots and node.Slots is the same object
	gossip.selfNodeID = selfNodeId
	gossip.slots = make([]*Slot, slotCount)
	for _, node := range nodes {
		for _, slot := range node.Slots {
			gossip.slots[int(slot.ID)] = slot
		}
	}
	gossip.nodes = nodes
}

// SetSlotMigrating set a hosting slot as migrating state
func (gossip *Gossip) SetSlotMigrating(slotID uint32, newNodeID string) {
	slot := gossip.slots[int(slotID)]
	slot.Flags |= slotFlagMigrating
	slot.OldNodeID = slot.NodeID
	slot.NodeID = newNodeID
	// gossip.nodes.Slots is the same object with gossip.slots, no need to update
}

func (gossip *Gossip) FinishSlotMigrate(slotID uint32) {
	slot := gossip.slots[int(slotID)]
	oldNode := gossip.nodes[slot.OldNodeID]
	// remove from old oldNode
	for i, s := range oldNode.Slots {
		if s.ID == slot.ID {
			copy(oldNode.Slots[i:], oldNode.Slots[i+1:])
			oldNode.Slots = oldNode.Slots[:len(oldNode.Slots)-1]
			break
		}
	}
	// add into new node
	newNode := gossip.nodes[slot.NodeID]
	newNode.Slots = append(newNode.Slots, slot)
	slot.Flags &= ^slotFlagMigrating
	slot.OldNodeID = ""
}

func (gossip *Gossip) GetSlots() []*Slot {
	return gossip.slots
}

// GetSelfNodeID returns node id of current node
func (gossip *Gossip) GetSelfNodeID() string {
	return gossip.selfNodeID
}

func (gossip *Gossip) sendPing() error {
	type nodeTimePair struct {
		nodeId   string
		lastTime time.Time
	}
	var nodes []*nodeTimePair
	for nodeId, t := range gossip.lastHeardTimes {
		nodes = append(nodes, &nodeTimePair{
			nodeId:   nodeId,
			lastTime: t,
		})
		if len(nodes) == 5 {
			break
		}
	}
	if len(nodes) == 0 {
		return nil
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].lastTime.Before(nodes[j].lastTime)
	})
	//gossip.Cluster.getPeerClient()
	return nil
}
