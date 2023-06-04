package cluster

import (
	"errors"
	"github.com/hdt3213/godis/config"
	database2 "github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/idgenerator"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
	"math/rand"
	"sync"
)

type testClientFactory struct {
	nodes        []*Cluster
	timeoutFlags []bool
}

type testClient struct {
	targetNode  *Cluster
	timeoutFlag *bool
	conn        redis.Connection
}

func (cli *testClient) Send(cmdLine [][]byte) redis.Reply {
	if *cli.timeoutFlag {
		return protocol.MakeErrReply("ERR timeout")
	}
	return cli.targetNode.Exec(cli.conn, cmdLine)
}

func (factory *testClientFactory) GetPeerClient(peerAddr string) (peerClient, error) {
	for i, n := range factory.nodes {
		if n.self == peerAddr {
			cli := &testClient{
				targetNode:  n,
				timeoutFlag: &factory.timeoutFlags[i],
				conn:        connection.NewFakeConn(),
			}
			if config.Properties.RequirePass != "" {
				cli.Send(utils.ToCmdLine("AUTH", config.Properties.RequirePass))
			}
			return cli, nil
		}
	}
	return nil, errors.New("peer not found")
}

type mockStream struct {
	targetNode *Cluster
	ch         <-chan *parser.Payload
}

func (s *mockStream) Stream() <-chan *parser.Payload {
	return s.ch
}

func (s *mockStream) Close() error {
	return nil
}

func (factory *testClientFactory) NewStream(peerAddr string, cmdLine CmdLine) (peerStream, error) {
	for _, n := range factory.nodes {
		if n.self == peerAddr {
			conn := connection.NewFakeConn()
			if config.Properties.RequirePass != "" {
				n.Exec(conn, utils.ToCmdLine("AUTH", config.Properties.RequirePass))
			}
			result := n.Exec(conn, cmdLine)
			conn.Write(result.ToBytes())
			ch := parser.ParseStream(conn)
			return &mockStream{
				targetNode: n,
				ch:         ch,
			}, nil
		}
	}
	return nil, errors.New("node not found")
}

func (factory *testClientFactory) ReturnPeerClient(peer string, peerClient peerClient) error {
	return nil
}

func (factory *testClientFactory) Close() error {
	return nil
}

// mockClusterNodes creates a fake cluster for test
// timeoutFlags should have the same length as addresses, set timeoutFlags[i] == true could simulate addresses[i] timeout
func mockClusterNodes(addresses []string, timeoutFlags []bool) []*Cluster {
	nodes := make([]*Cluster, len(addresses))
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
	factory := &testClientFactory{
		nodes:        nodes,
		timeoutFlags: timeoutFlags,
	}
	for i, addr := range addresses {
		topo := &fixedTopology{
			mu:         sync.RWMutex{},
			nodeMap:    nodeMap,
			slots:      slots,
			selfNodeID: addr,
		}
		nodes[i] = &Cluster{
			self:          addr,
			db:            database2.NewStandaloneServer(),
			transactions:  dict.MakeSimple(),
			idGenerator:   idgenerator.MakeGenerator(config.Properties.Self),
			topology:      topo,
			clientFactory: factory,
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
