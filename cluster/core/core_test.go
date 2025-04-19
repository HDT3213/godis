package core

import (
	"os"
	"testing"
	"time"

	"github.com/hdt3213/godis/cluster/raft"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
)

func TestClusterBootstrap(t *testing.T) {
	// start leader
	leaderDir := "test/0"
	os.RemoveAll(leaderDir)
	os.MkdirAll(leaderDir, 0777)
	defer func() {
		os.RemoveAll(leaderDir)
	}()
	RegisterDefaultCmd("get")
	RegisterDefaultCmd("set")

	// connection stub
	connections := NewInMemConnectionFactory()
	leaderCfg := &Config{
		RaftConfig: raft.RaftConfig{
			RedisAdvertiseAddr: "127.0.0.1:6399",
			RaftListenAddr:     "127.0.0.1:16666",
			RaftAdvertiseAddr:  "127.0.0.1:16666",
			Dir:                leaderDir,
		},
		StartAsSeed:    true,
		connectionStub: connections,
		noCron:         true,
	}
	leader, err := NewCluster(leaderCfg)
	if err != nil {
		t.Error(err)
		return
	}
	connections.nodes[leaderCfg.RedisAdvertiseAddr] = leader

	// set key-values for test
	testEntries := make(map[string]string)
	c := connection.NewFakeConn()
	for i := 0; i < 1000; i++ {
		key := utils.RandString(10)
		value := utils.RandString(10)
		testEntries[key] = value
		result := leader.Exec(c, utils.ToCmdLine("set", key, value))
		if !protocol.IsOKReply(result) {
			t.Errorf("command [set] failed: %s", string(result.ToBytes()))
			return
		}
	}

	// start follower
	followerDir := "test/1"
	os.RemoveAll(followerDir)
	os.MkdirAll(followerDir, 0777)
	defer func() {
		os.RemoveAll(followerDir)
	}()
	followerCfg := &Config{
		RaftConfig: raft.RaftConfig{
			RedisAdvertiseAddr: "127.0.0.1:6499",
			RaftListenAddr:     "127.0.0.1:16667",
			RaftAdvertiseAddr:  "127.0.0.1:16667",
			Dir:                followerDir,
		},
		StartAsSeed:    false,
		JoinAddress:    leaderCfg.RedisAdvertiseAddr,
		connectionStub: connections,
		noCron:         true,
	}
	follower, err := NewCluster(followerCfg)
	if err != nil {
		t.Error(err)
		return
	}
	connections.nodes[followerCfg.RedisAdvertiseAddr] = follower

	_ = follower.SelfID()
	// check nodes
	joined := false
	for i := 0; i < 10; i++ {
		nodes, err := leader.raftNode.GetNodes()
		if err != nil {
			t.Log(err)
			continue
		}
		if len(nodes) == 2 {
			t.Log("join success")
			joined = true
			break
		}
		time.Sleep(time.Second)
	}
	if !joined {
		t.Error("join failed")
		return
	}

	// rebalance
	leader.doRebalance()
	time.Sleep(2 * time.Second)
	for i := 0; i < 1000; i++ {
		leaderSuccess := false
		leader.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
			leaderSlots := len(fsm.Node2Slot[leaderCfg.RedisAdvertiseAddr])
			followerSlots := len(fsm.Node2Slot[followerCfg.RedisAdvertiseAddr])
			if len(fsm.Migratings) == 0 && leaderSlots > 0 && followerSlots > 0 {
				leaderSuccess = true
			}
		})
		if leaderSuccess {
			t.Log("rebalance success")
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	// set key-values for test
	for key, value := range testEntries {
		c := connection.NewFakeConn()
		result := leader.Exec(c, utils.ToCmdLine("get", key))
		result2 := result.(*protocol.BulkReply)
		if string(result2.Arg) != value {
			t.Errorf("command [get] failed: %s", string(result.ToBytes()))
			return
		}
	}
}

func TestFailover(t *testing.T) {
	// start leader
	leaderDir := "test/0"
	os.RemoveAll(leaderDir)
	os.MkdirAll(leaderDir, 0777)
	defer func() {
		os.RemoveAll(leaderDir)
	}()
	RegisterCmd("slaveof", func(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
		return protocol.MakeOkReply()
	})

	// connection stub
	connections := NewInMemConnectionFactory()
	leaderCfg := &Config{
		RaftConfig: raft.RaftConfig{
			RedisAdvertiseAddr: "127.0.0.1:6399",
			RaftListenAddr:     "127.0.0.1:26666",
			RaftAdvertiseAddr:  "127.0.0.1:26666",
			Dir:                leaderDir,
		},
		StartAsSeed:    true,
		connectionStub: connections,
		noCron:         true,
	}
	leader, err := NewCluster(leaderCfg)
	if err != nil {
		t.Error(err)
		return
	}
	connections.nodes[leaderCfg.RedisAdvertiseAddr] = leader

	// start follower
	followerDir := "test/1"
	os.RemoveAll(followerDir)
	os.MkdirAll(followerDir, 0777)
	defer func() {
		os.RemoveAll(followerDir)
	}()
	followerCfg := &Config{
		RaftConfig: raft.RaftConfig{
			RedisAdvertiseAddr: "127.0.0.1:6499",
			RaftListenAddr:     "127.0.0.1:26667",
			RaftAdvertiseAddr:  "127.0.0.1:26667",
			Dir:                followerDir,
		},
		StartAsSeed:    false,
		JoinAddress:    leaderCfg.RedisAdvertiseAddr,
		connectionStub: connections,
		noCron:         true,
		Master:         leader.SelfID(),
	}
	follower, err := NewCluster(followerCfg)
	if err != nil {
		t.Error(err)
		return
	}
	connections.nodes[followerCfg.RedisAdvertiseAddr] = follower

	_ = follower.SelfID()
	// check nodes
	joined := false
	for i := 0; i < 10; i++ {
		nodes, err := leader.raftNode.GetNodes()
		if err != nil {
			t.Log(err)
			continue
		}
		if len(nodes) == 2 {
			t.Log("join success")
			joined = true
			break
		}
		time.Sleep(time.Second)
	}
	if !joined {
		t.Error("join failed")
		return
	}

	// rebalance
	leader.replicaManager.masterHeartbeats[leader.SelfID()] = time.Now().Add(-time.Hour)
	leader.doFailoverCheck()
	time.Sleep(2 * time.Second)
	for i := 0; i < 1000; i++ {
		success := false
		leader.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
			ms := fsm.MasterSlaves[follower.SelfID()]	
			if ms != nil && len(ms.Slaves) > 0 {
				success = true
			}
		})
		if success {
			t.Log("rebalance success")
			break
		} else {
			time.Sleep(time.Second)
		}
	}
}
