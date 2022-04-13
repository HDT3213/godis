package cluster

import (
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
	"math/rand"
	"strings"
)

var testNodeA, testNodeB *Cluster
var simulateATimout, simulateBTimout *bool

type mockPicker struct {
	nodes []string
}

func (picker *mockPicker) AddNode(keys ...string) {
	picker.nodes = append(picker.nodes, keys...)
}

func (picker *mockPicker) PickNode(key string) string {
	for _, n := range picker.nodes {
		if strings.Contains(key, n) {
			return n
		}
	}
	return picker.nodes[0]
}

func makeMockRelay(peer *Cluster) (*bool, func(cluster *Cluster, node string, c redis.Connection, cmdLine CmdLine) redis.Reply) {
	simulateTimeout0 := false
	simulateTimeout := &simulateTimeout0
	return simulateTimeout, func(cluster *Cluster, node string, c redis.Connection, cmdLine CmdLine) redis.Reply {
		if len(cmdLine) == 0 {
			return protocol.MakeErrReply("ERR command required")
		}
		if node == cluster.self {
			// to self db
			cmdName := strings.ToLower(string(cmdLine[0]))
			if cmdName == "prepare" {
				return execPrepare(cluster, c, cmdLine)
			} else if cmdName == "commit" {
				return execCommit(cluster, c, cmdLine)
			} else if cmdName == "rollback" {
				return execRollback(cluster, c, cmdLine)
			}
			return cluster.db.Exec(c, cmdLine)
		}
		if *simulateTimeout {
			return protocol.MakeErrReply("ERR timeout")
		}
		cmdName := strings.ToLower(string(cmdLine[0]))
		if cmdName == "prepare" {
			return execPrepare(peer, c, cmdLine)
		} else if cmdName == "commit" {
			return execCommit(peer, c, cmdLine)
		} else if cmdName == "rollback" {
			return execRollback(peer, c, cmdLine)
		}
		return peer.db.Exec(c, cmdLine)
	}
}

func init() {
	if config.Properties == nil {
		config.Properties = &config.ServerProperties{}
	}
	addrA := "127.0.0.1:6399"
	addrB := "127.0.0.1:7379"
	config.Properties.Self = addrA
	config.Properties.Peers = []string{addrB}
	testNodeA = MakeCluster()
	config.Properties.Self = addrB
	config.Properties.Peers = []string{addrA}
	testNodeB = MakeCluster()

	simulateBTimout, testNodeA.relayImpl = makeMockRelay(testNodeB)
	testNodeA.peerPicker = &mockPicker{}
	testNodeA.peerPicker.AddNode(addrA, addrB)
	simulateATimout, testNodeB.relayImpl = makeMockRelay(testNodeA)
	testNodeB.peerPicker = &mockPicker{}
	testNodeB.peerPicker.AddNode(addrB, addrA)
}

func MakeTestCluster(peers []string) *Cluster {
	if config.Properties == nil {
		config.Properties = &config.ServerProperties{}
	}
	config.Properties.Self = "127.0.0.1:6399"
	config.Properties.Peers = peers
	return MakeCluster()
}

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
