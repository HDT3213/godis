package cluster

import (
	"github.com/hdt3213/godis/redis/protocol"
	"sync"
)

// fixedTopology is a fixed cluster topology, used for test
type fixedTopology struct {
	mu         sync.RWMutex
	nodeMap    map[string]*Node
	slots      []*Slot
	selfNodeID string
}

func (fixed *fixedTopology) GetSelfNodeID() string {
	return fixed.selfNodeID
}

func (fixed *fixedTopology) GetNodes() []*Node {
	fixed.mu.RLock()
	defer fixed.mu.RUnlock()
	result := make([]*Node, 0, len(fixed.nodeMap))
	for _, v := range fixed.nodeMap {
		result = append(result, v)
	}
	return result
}

func (fixed *fixedTopology) GetNode(nodeID string) *Node {
	fixed.mu.RLock()
	defer fixed.mu.RUnlock()
	return fixed.nodeMap[nodeID]
}

func (fixed *fixedTopology) GetSlots() []*Slot {
	return fixed.slots
}

func (fixed *fixedTopology) StartAsSeed(addr string) protocol.ErrorReply {
	return nil
}

func (fixed *fixedTopology) LoadConfigFile() protocol.ErrorReply {
	return nil
}

func (fixed *fixedTopology) Join(seed string) protocol.ErrorReply {
	return protocol.MakeErrReply("fixed topology does not support join")
}

func (fixed *fixedTopology) SetSlot(slotIDs []uint32, newNodeID string) protocol.ErrorReply {
	return protocol.MakeErrReply("fixed topology does not support set slots")
}

func (fixed *fixedTopology) Close() error {
	return nil
}
