package database

import (
	"bytes"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/client"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"sync/atomic"
	"testing"
	"time"
)

func TestReplication(t *testing.T) {
	mdb := &MultiDB{}
	mdb.dbSet = make([]*atomic.Value, 16)
	for i := range mdb.dbSet {
		singleDB := makeDB()
		singleDB.index = i
		holder := &atomic.Value{}
		holder.Store(singleDB)
		mdb.dbSet[i] = holder
	}
	mdb.replication = initReplStatus()
	masterCli, err := client.MakeClient("127.0.0.1:6379")
	if err != nil {
		t.Error(err)
	}
	masterCli.Start()

	// sync with master
	ret := masterCli.Send(utils.ToCmdLine("set", "1", "1"))
	asserts.AssertStatusReply(t, ret, "OK")
	conn := &connection.FakeConn{}
	ret = mdb.Exec(conn, utils.ToCmdLine("SLAVEOF", "127.0.0.1", "6379"))
	asserts.AssertStatusReply(t, ret, "OK")
	success := false
	for i := 0; i < 30; i++ {
		// wait for sync
		time.Sleep(time.Second)
		ret = mdb.Exec(conn, utils.ToCmdLine("GET", "1"))
		bulkRet, ok := ret.(*protocol.BulkReply)
		if ok {
			if bytes.Equal(bulkRet.Arg, []byte("1")) {
				success = true
				break
			}
		}
	}
	if !success {
		t.Error("sync failed")
		return
	}

	// receive aof
	ret = masterCli.Send(utils.ToCmdLine("set", "1", "2"))
	asserts.AssertStatusReply(t, ret, "OK")
	success = false
	for i := 0; i < 10; i++ {
		// wait for sync
		time.Sleep(time.Second)
		ret = mdb.Exec(conn, utils.ToCmdLine("GET", "1"))
		bulkRet, ok := ret.(*protocol.BulkReply)
		if ok {
			if bytes.Equal(bulkRet.Arg, []byte("2")) {
				success = true
				break
			}
		}
	}
	if !success {
		t.Error("sync failed")
		return
	}
	err = mdb.replication.sendAck2Master()
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(3 * time.Second)

	// test reconnect
	config.Properties.ReplTimeout = 1
	_ = mdb.replication.masterConn.Close()
	mdb.replication.lastRecvTime = time.Now().Add(-time.Hour) // mock timeout
	mdb.slaveCron()
	time.Sleep(3 * time.Second)
	ret = masterCli.Send(utils.ToCmdLine("set", "1", "3"))
	asserts.AssertStatusReply(t, ret, "OK")
	success = false
	for i := 0; i < 10; i++ {
		// wait for sync
		time.Sleep(time.Second)
		ret = mdb.Exec(conn, utils.ToCmdLine("GET", "1"))
		bulkRet, ok := ret.(*protocol.BulkReply)
		if ok {
			if bytes.Equal(bulkRet.Arg, []byte("3")) {
				success = true
				break
			}
		}
	}
	if !success {
		t.Error("sync failed")
		return
	}

	// test slave of no one
	ret = mdb.Exec(conn, utils.ToCmdLine("SLAVEOF", "NO", "ONE"))
	asserts.AssertStatusReply(t, ret, "OK")
	ret = masterCli.Send(utils.ToCmdLine("set", "1", "4"))
	asserts.AssertStatusReply(t, ret, "OK")
	ret = mdb.Exec(conn, utils.ToCmdLine("GET", "1"))
	asserts.AssertBulkReply(t, ret, "3")
	ret = mdb.Exec(conn, utils.ToCmdLine("SLAVEOF", "127.0.0.1", "6379"))
	asserts.AssertStatusReply(t, ret, "OK")
	success = false
	for i := 0; i < 30; i++ {
		// wait for sync
		time.Sleep(time.Second)
		ret = mdb.Exec(conn, utils.ToCmdLine("GET", "1"))
		bulkRet, ok := ret.(*protocol.BulkReply)
		if ok {
			if bytes.Equal(bulkRet.Arg, []byte("4")) {
				success = true
				break
			}
		}
	}
	if !success {
		t.Error("sync failed")
		return
	}

	err = mdb.replication.close()
	if err != nil {
		t.Error("cannot close")
	}
}
