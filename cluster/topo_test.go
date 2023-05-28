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

func TestRaftStart(t *testing.T) {
	addresses := []string{"127.0.0.1:6399", "127.0.0.1:7379", "127.0.0.1:7369"}
	timeoutFlags := []bool{false, false, false}
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
		topologyPersistFile := path.Join(config.Properties.Dir, "test"+addr+".conf")
		nodes[i].topology = newRaft(nodes[i], topologyPersistFile)
	}

	err := nodes[0].startAsSeed(addresses[0])
	if err != nil {
		t.Error(err)
		return
	}
	err = nodes[1].Join(addresses[0])
	if err != nil {
		t.Error(err)
		return
	}
	err = nodes[2].Join(addresses[0])
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
	time.Sleep(3 * time.Second)
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
}
