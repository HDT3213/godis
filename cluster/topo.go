package cluster

import (
	"fmt"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/client"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
	"hash/crc32"
	"net"
	"sort"
	"strconv"
)

type Topology interface {
	// StartAsSeed starts cluster as seed
	StartAsSeed(addr string) (string, error)
	// LoadTopology loads topology, used in join cluster
	LoadTopology(selfNodeId string, nodes map[string]*Node)
	// NewNode creates a new Node and put it into the topology when a node request self node for joining cluster
	NewNode(addr string) *Node
	// GetSelfNodeID returns node id of current node
	GetSelfNodeID() string
	// GetTopology returns cluster topology (in current node's view)
	GetTopology() map[string]*Node
	// PickNode returns the node id hosting the given slot.
	// If the slot is migrating, return the node which is importing the slot
	PickNode(slotID uint32) *Node
	// SetSlotMigrating set a hosting slot as migrating to targetNode state
	SetSlotMigrating(slotID uint32, targetNodeID string)
	// FinishSlotMigrate marks a slot has migrated to current node or moved out from current node
	FinishSlotMigrate(slotID uint32)
	// GetSlots returns information of given slot
	GetSlots() []*Slot
}

// Node represents a node and its slots, used in cluster internal messages
type Node struct {
	ID    string
	Addr  string
	Slots []*Slot // ascending order by slot id
	Flags uint32
}

const (
	slotFlagMigrating uint32 = 1 << iota
)

// Slot represents a hash slot,  used in cluster internal messages
type Slot struct {
	// ID is uint between 0 and 16383
	ID uint32
	// NodeID is id of the hosting node
	// If the slot is migrating, NodeID is the id of the node importing this slot (target node)
	NodeID string
	// OldNodeID is the node which is moving out this slot
	// only valid during slot is migrating
	OldNodeID string
	// Flags stores more information of slot
	Flags uint32
}

func (slot *Slot) IsMigrating() bool {
	return slot.Flags&slotFlagMigrating > 0
}

func getSlot(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key)) % uint32(slotCount)
}

func (cluster *Cluster) startUpAsSeed() error {
	selfNodeId, err := cluster.topology.StartAsSeed(config.Properties.AnnounceAddress())
	if err != nil {
		return err
	}
	for i := 0; i < slotCount; i++ {
		cluster.setSlot(uint32(i), slotStateHost)
	}
	cluster.self = selfNodeId
	return nil
}

// findSlotsForNewNode try to find slots for new node, but do not actually migrate
func (cluster *Cluster) findSlotsForNewNode() []*Slot {
	nodeMap := cluster.topology.GetTopology() // including the new node
	avgSlot := slotCount / len(nodeMap)
	nodes := make([]*Node, 0, len(nodeMap))
	for _, node := range nodeMap {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return len(nodes[i].Slots) > len(nodes[j].Slots)
	})
	result := make([]*Slot, 0, avgSlot)
	// there are always some nodes has more slots than avgSlot
	for _, node := range nodes {
		if len(node.Slots) <= avgSlot {
			// nodes are in decreasing order by len(node.Slots)
			// if len(node.Slots) < avgSlot, then all following nodes has fewer slots than avgSlot
			break
		}
		n := 2*avgSlot - len(result)
		if n < len(node.Slots) {
			// n - len(result) - avgSlot = avgSlot - len(result)
			// now len(result) == avgSlot
			result = append(result, node.Slots[avgSlot:n]...)
		} else {
			result = append(result, node.Slots[avgSlot:]...)
		}
		if len(result) >= avgSlot {
			break
		}
	}
	return result
}

// Join send `gcluster join` to node in cluster to join
func (cluster *Cluster) Join(seed string) protocol.ErrorReply {
	cli, err := client.MakeClient(seed)
	if err != nil {
		return protocol.MakeErrReply("connect with seed failed: " + err.Error())
	}
	cli.Start()
	ret := cli.Send(utils.ToCmdLine("gcluster", "join", config.Properties.AnnounceAddress()))
	if protocol.IsErrorReply(ret) {
		return ret.(protocol.ErrorReply)
	}
	topology, ok := ret.(*protocol.MultiBulkReply)
	if !ok {
		return protocol.MakeErrReply("ERR gcluster join returns wrong reply")
	}
	if len(topology.Args) == 0 {
		return protocol.MakeErrReply("ERR gcluster join returns wrong reply")
	}
	selfNodeId := string(topology.Args[0])
	nodes, err := unmarshalTopology(topology.Args[1:])
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	cluster.topology.LoadTopology(selfNodeId, nodes)
	cluster.self = selfNodeId
	// asynchronous migrating slots
	go func() {
		//defer func() {
		//	if e := recover(); e != nil {
		//		logger.Error(e)
		//	}
		//}()

		slots := cluster.findSlotsForNewNode()
		// serial migrations to avoid overloading the cluster
		for _, slot := range slots {
			if slot.IsMigrating() {
				continue
			}
			logger.Info("start import slot ", slot.ID)
			err = cluster.importSlot(slot)
			if err != nil {
				logger.Error("import slot %d error: %d", slot.ID, err)
				// todo: delete all keys in slot
				continue
			}
			logger.Info("finish import slot", slot.ID)
		}
	}()
	return nil
}

func (cluster *Cluster) importSlot(slot *Slot) error {
	fakeConn := connection.NewFakeConn()
	node := cluster.topology.PickNode(slot.ID)
	conn, err := net.Dial("tcp", node.Addr)
	if err != nil {
		return fmt.Errorf("connect with %s(%s) error: %v", node.ID, node.Addr, err)
	}
	nodeChan := parser.ParseStream(conn)
	send2node := func(cmdLine CmdLine) redis.Reply {
		req := protocol.MakeMultiBulkReply(cmdLine)
		_, err := conn.Write(req.ToBytes())
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		resp := <-nodeChan
		if resp.Err != nil {
			return protocol.MakeErrReply(resp.Err.Error())
		}
		return resp.Data
	}
	selfNodeID := cluster.topology.GetSelfNodeID()

	cluster.setSlot(slot.ID, slotStateImporting) // prepare host slot before send `set slot`
	cluster.topology.SetSlotMigrating(slot.ID, cluster.self)
	ret := send2node(utils.ToCmdLine(
		"gcluster", "setslot", strconv.Itoa(int(slot.ID)), selfNodeID))
	if !protocol.IsOKReply(ret) {
		return fmt.Errorf("set slot %d error: %v", slot.ID, err)
	}
	req := protocol.MakeMultiBulkReply(utils.ToCmdLine(
		"gcluster", "migrate", strconv.Itoa(int(slot.ID)), selfNodeID))
	_, err = conn.Write(req.ToBytes())
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
slotLoop:
	for proto := range nodeChan {
		if proto.Err != nil {
			return fmt.Errorf("set slot %d error: %v", slot.ID, err)
		}
		switch reply := proto.Data.(type) {
		case *protocol.MultiBulkReply:
			// todo: handle exec error
			_ = cluster.db.Exec(fakeConn, reply.Args)
			keys, _ := database.GetRelatedKeys(reply.Args)
			for _, key := range keys {
				cluster.setImportedKey(key)
			}
		case *protocol.StatusReply:
			if protocol.IsOKReply(reply) {
				break slotLoop
			}
		}
	}
	cluster.slots[slot.ID].importedKeys = nil
	cluster.slots[slot.ID].state = slotStateHost
	cluster.topology.FinishSlotMigrate(slot.ID)
	send2node(utils.ToCmdLine("gcluster", "migrate-done", strconv.Itoa(int(slot.ID))))
	return nil
}
