package cluster

import (
	"github.com/hdt3213/godis/redis/protocol"
	"hash/crc32"
	"strings"
	"time"
)

// Slot represents a hash slot,  used in cluster internal messages
type Slot struct {
	// ID is uint between 0 and 16383
	ID uint32
	// NodeID is id of the hosting node
	// If the slot is migrating, NodeID is the id of the node importing this slot (target node)
	NodeID string
	// Flags stores more information of slot
	Flags uint32
}

// getPartitionKey extract hashtag
func getPartitionKey(key string) string {
	beg := strings.Index(key, "{")
	if beg == -1 {
		return key
	}
	end := strings.Index(key, "}")
	if end == -1 || end == beg+1 {
		return key
	}
	return key[beg+1 : end]
}

func getSlot(key string) uint32 {
	partitionKey := getPartitionKey(key)
	return crc32.ChecksumIEEE([]byte(partitionKey)) % uint32(slotCount)
}

// Node represents a node and its slots, used in cluster internal messages
type Node struct {
	ID        string
	Addr      string
	Slots     []*Slot // ascending order by slot id
	Flags     uint32
	lastHeard time.Time
}

type topology interface {
	GetSelfNodeID() string
	GetNodes() []*Node // return a copy
	GetNode(nodeID string) *Node
	GetSlots() []*Slot
	StartAsSeed(addr string) protocol.ErrorReply
	SetSlot(slotIDs []uint32, newNodeID string) protocol.ErrorReply
	LoadConfigFile() protocol.ErrorReply
	Join(seed string) protocol.ErrorReply
	Close() error
}
