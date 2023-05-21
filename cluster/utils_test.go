package cluster

import (
	"github.com/hdt3213/godis/config"
	database2 "github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/idgenerator"
	"github.com/hdt3213/godis/redis/protocol"
	"math/rand"
	"sync"
)

// mockClusterNodes creates a fake cluster for test
// timeoutFlags should have the same length as addresses, set timeoutFlags[i] == true could simulate addresses[i] timeout
func mockClusterNodes(addresses []string, timeoutFlags []bool) []*Cluster {
	nodes := make([]*Cluster, len(addresses))
	relay := func(cluster *Cluster, node string, c redis.Connection, cmdLine CmdLine) redis.Reply {
		if node == cluster.self {
			return cluster.db.Exec(c, cmdLine)
		}
		for i, n := range nodes {
			if n.self == node {
				if timeoutFlags[i] {
					return protocol.MakeErrReply("ERR timeout")
				}
				return n.Exec(c, cmdLine)
			}
		}
		return protocol.MakeErrReply("unknown node: " + node)
	}
	// build fixedTopology
	slots := make([]*Slot, slotCount)
	nodeMap := make(map[string]*Node)
	for _, addr := range addresses {
		nodeMap[addr] = &Node{
			ID:    addr,
			Addr:  addr,
			Slots: nil,
		}
	}
	for i := range slots {
		addr := addresses[i%len(addresses)]
		slots[i] = &Slot{
			ID:     uint32(i),
			NodeID: addr,
			Flags:  0,
		}
		nodeMap[addr].Slots = append(nodeMap[addr].Slots, slots[i])
	}
	for i, addr := range addresses {
		topo := &fixedTopology{
			mu:         sync.RWMutex{},
			nodeMap:    nodeMap,
			slots:      slots,
			selfNodeID: addr,
		}
		nodes[i] = &Cluster{
			self:            addr,
			db:              database2.NewStandaloneServer(),
			transactions:    dict.MakeSimple(),
			nodeConnections: dict.MakeConcurrent(1),
			idGenerator:     idgenerator.MakeGenerator(config.Properties.Self),
			relayImpl:       relay,
			topology:        topo,
		}
	}
	return nodes
}

var addresses = []string{"127.0.0.1:6399", "127.0.0.1:7379"}
var timeoutFlags = []bool{false, false}
var testCluster = mockClusterNodes(addresses, timeoutFlags)

func toArgs(cmd ...string) [][]byte {
	args := make([][]byte, len(cmd))
	for i, s := range cmd {
		args[i] = []byte(s)
	}
	return args
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
