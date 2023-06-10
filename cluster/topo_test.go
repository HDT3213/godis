package cluster

import (
	"github.com/hdt3213/godis/config"
	database2 "github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/lib/idgenerator"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"path"
	"strconv"
	"testing"
	"time"
)

func makeTestRaft(addresses []string, timeoutFlags []bool, persistFilenames []string) ([]*Cluster, error) {
	nodes := make([]*Cluster, len(addresses))
	factory := &testClientFactory{
		nodes:        nodes,
		timeoutFlags: timeoutFlags,
	}
	for i, addr := range addresses {
		addr := addr
		nodes[i] = &Cluster{
			self:          addr,
			addr:          addr,
			db:            database2.NewStandaloneServer(),
			transactions:  dict.MakeSimple(),
			idGenerator:   idgenerator.MakeGenerator(config.Properties.Self),
			clientFactory: factory,
			slots:         make(map[uint32]*hostSlot),
		}
		topologyPersistFile := persistFilenames[i]
		nodes[i].topology = newRaft(nodes[i], topologyPersistFile)
	}

	err := nodes[0].startAsSeed(addresses[0])
	if err != nil {
		return nil, err
	}
	err = nodes[1].Join(addresses[0])
	if err != nil {
		return nil, err
	}
	err = nodes[2].Join(addresses[0])
	if err != nil {
		return nil, err
	}
	return nodes, nil
}

func TestRaftStart(t *testing.T) {
	addresses := []string{"127.0.0.1:6399", "127.0.0.1:7379", "127.0.0.1:7369"}
	timeoutFlags := []bool{false, false, false}
	persistFilenames := []string{"", "", ""}
	nodes, err := makeTestRaft(addresses, timeoutFlags, persistFilenames)
	if err != nil {
		t.Error(err)
		return
	}
	if nodes[0].asRaft().state != leader {
		t.Error("expect leader")
		return
	}
	if nodes[1].asRaft().state != follower {
		t.Error("expect follower")
		return
	}
	if nodes[2].asRaft().state != follower {
		t.Error("expect follower")
		return
	}
	size := 100
	conn := connection.NewFakeConn()
	for i := 0; i < size; i++ {
		str := strconv.Itoa(i)
		result := nodes[0].Exec(conn, utils.ToCmdLine("SET", str, str))
		asserts.AssertNotError(t, result)
	}
	for i := 0; i < size; i++ {
		str := strconv.Itoa(i)
		result := nodes[0].Exec(conn, utils.ToCmdLine("Get", str))
		asserts.AssertBulkReply(t, result, str)
	}
	for _, node := range nodes {
		_ = node.asRaft().Close()
	}
}

func TestRaftElection(t *testing.T) {
	addresses := []string{"127.0.0.1:6399", "127.0.0.1:7379", "127.0.0.1:7369"}
	timeoutFlags := []bool{false, false, false}
	persistFilenames := []string{"", "", ""}
	nodes, err := makeTestRaft(addresses, timeoutFlags, persistFilenames)
	if err != nil {
		t.Error(err)
		return
	}
	nodes[0].asRaft().Close()
	time.Sleep(3 * electionTimeoutMaxMs * time.Millisecond) // wait for leader timeout
	//<-make(chan struct{}) // wait for leader timeout
	for i := 0; i < 10; i++ {
		leaderCount := 0
		for _, node := range nodes {
			if node.asRaft().closed {
				continue
			}
			switch node.asRaft().state {
			case leader:
				leaderCount++
			}
		}
		if leaderCount == 1 {
			break
		} else if leaderCount > 1 {
			t.Errorf("get %d leaders, split brain", leaderCount)
			break
		}
		time.Sleep(time.Second)
	}
}

func TestRaftPersist(t *testing.T) {
	addresses := []string{"127.0.0.1:6399", "127.0.0.1:7379", "127.0.0.1:7369"}
	timeoutFlags := []bool{false, false, false}
	persistFilenames := []string{
		path.Join(config.Properties.Dir, "test6399.conf"),
		path.Join(config.Properties.Dir, "test7379.conf"),
		path.Join(config.Properties.Dir, "test7369.conf"),
	}
	nodes, err := makeTestRaft(addresses, timeoutFlags, persistFilenames)
	if err != nil {
		t.Error(err)
		return
	}
	node1 := nodes[0].asRaft()
	err = node1.persist()
	if err != nil {
		t.Error(err)
		return
	}
	for _, node := range nodes {
		_ = node.asRaft().Close()
	}

	err = node1.LoadConfigFile()
	if err != nil {
		t.Error(err)
		return
	}
}
