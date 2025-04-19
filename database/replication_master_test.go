package database

import (
	"bytes"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hdt3213/godis/aof"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	rdb "github.com/hdt3213/rdb/parser"
)

func mockServer() *Server {
	server := &Server{}
	server.dbSet = make([]*atomic.Value, 16)
	for i := range server.dbSet {
		singleDB := makeDB()
		singleDB.index = i
		holder := &atomic.Value{}
		holder.Store(singleDB)
		server.dbSet[i] = holder
	}
	server.slaveStatus = initReplSlaveStatus()
	server.initMasterStatus()
	return server
}

func TestReplicationMasterSide(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "godis")
	if err != nil {
		t.Error(err)
		return
	}
	aofFilename := path.Join(tmpDir, "a.aof")
	defer func() {
		_ = os.Remove(aofFilename)
	}()
	config.Properties = &config.ServerProperties{
		Databases:      16,
		AppendOnly:     true,
		AppendFilename: aofFilename,
	}
	master := mockServer()
	aofHandler, err := NewPersister(master, config.Properties.AppendFilename, true, config.Properties.AppendFsync)
	if err != nil {
		panic(err)
	}
	master.bindPersister(aofHandler)
	slave := mockServer()
	replConn := connection.NewFakeConn()

	// set data to master
	masterConn := connection.NewFakeConn()
	resp := master.Exec(masterConn, utils.ToCmdLine("SET", "a", "a"))
	asserts.AssertNotError(t, resp)
	time.Sleep(time.Millisecond * 100) // wait write aof

	// full re-sync
	master.Exec(replConn, utils.ToCmdLine("psync", "?", "-1"))
	masterChan := parser.ParseStream(replConn)
	psyncPayload := <-masterChan
	if psyncPayload.Err != nil {
		t.Errorf("master bad protocol: %v", psyncPayload.Err)
		return
	}
	psyncHeader, ok := psyncPayload.Data.(*protocol.StatusReply)
	if !ok {
		t.Error("psync header is not a status reply")
		return
	}
	headers := strings.Split(psyncHeader.Status, " ")
	if len(headers) != 3 {
		t.Errorf("illegal psync header: %s", psyncHeader.Status)
		return
	}

	replId := headers[1]
	replOffset, err := strconv.ParseInt(headers[2], 10, 64)
	if err != nil {
		t.Errorf("illegal offset: %s", headers[2])
		return
	}
	t.Logf("repl id: %s, offset: %d", replId, replOffset)

	rdbPayload := <-masterChan
	if rdbPayload.Err != nil {
		t.Error("read response failed: " + rdbPayload.Err.Error())
		return
	}
	rdbReply, ok := rdbPayload.Data.(*protocol.BulkReply)
	if !ok {
		t.Error("illegal payload header: " + string(rdbPayload.Data.ToBytes()))
		return
	}

	rdbDec := rdb.NewDecoder(bytes.NewReader(rdbReply.Arg))
	err = slave.LoadRDB(rdbDec)
	if err != nil {
		t.Error("import rdb failed: " + err.Error())
		return
	}

	// get a
	slaveConn := connection.NewFakeConn()
	resp = slave.Exec(slaveConn, utils.ToCmdLine("get", "a"))
	asserts.AssertBulkReply(t, resp, "a")

	/*----  test broadcast aof  ----*/
	masterConn = connection.NewFakeConn()
	resp = master.Exec(masterConn, utils.ToCmdLine("SET", "b", "b"))
	time.Sleep(time.Millisecond * 100) // wait write aof
	asserts.AssertNotError(t, resp)
	master.masterCron()
	for {
		payload := <-masterChan
		if payload.Err != nil {
			t.Error(payload.Err)
			return
		}
		cmdLine, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			t.Error("unexpected payload: " + string(payload.Data.ToBytes()))
			return
		}
		slave.Exec(replConn, cmdLine.Args)
		n := len(cmdLine.ToBytes())
		slave.slaveStatus.replOffset += int64(n)
		if string(cmdLine.Args[0]) != "ping" {
			break
		}
	}

	resp = slave.Exec(slaveConn, utils.ToCmdLine("get", "b"))
	asserts.AssertBulkReply(t, resp, "b")

	/*----  test partial reconnect  ----*/
	_ = replConn.Close() // mock disconnect

	replConn = connection.NewFakeConn()

	master.Exec(replConn, utils.ToCmdLine("psync", replId,
		strconv.FormatInt(slave.slaveStatus.replOffset, 10)))
	masterChan = parser.ParseStream(replConn)
	psyncPayload = <-masterChan
	if psyncPayload.Err != nil {
		t.Errorf("master bad protocol: %v", psyncPayload.Err)
		return
	}
	psyncHeader, ok = psyncPayload.Data.(*protocol.StatusReply)
	if !ok {
		t.Error("psync header is not a status reply")
		return
	}
	headers = strings.Split(psyncHeader.Status, " ")
	if len(headers) != 2 {
		t.Errorf("illegal psync header: %s", psyncHeader.Status)
		return
	}
	if headers[0] != "CONTINUE" {
		t.Errorf("expect CONTINUE actual %s", headers[0])
		return
	}
	replId = headers[1]
	t.Logf("partial resync repl id: %s, offset: %d", replId, slave.slaveStatus.replOffset)

	resp = master.Exec(masterConn, utils.ToCmdLine("SET", "c", "c"))
	time.Sleep(time.Millisecond * 100) // wait write aof
	asserts.AssertNotError(t, resp)
	master.masterCron()
	for {
		payload := <-masterChan
		if payload.Err != nil {
			t.Error(payload.Err)
			return
		}
		cmdLine, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			t.Error("unexpected payload: " + string(payload.Data.ToBytes()))
			return
		}
		slave.Exec(replConn, cmdLine.Args)
		if string(cmdLine.Args[0]) != "ping" {
			break
		}
	}

	resp = slave.Exec(slaveConn, utils.ToCmdLine("get", "c"))
	asserts.AssertBulkReply(t, resp, "c")
}

func TestReplicationMasterRewriteRDB(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "godis")
	if err != nil {
		t.Error(err)
		return
	}
	aofFilename := path.Join(tmpDir, "a.aof")
	defer func() {
		_ = os.Remove(aofFilename)
	}()
	config.Properties = &config.ServerProperties{
		Databases:      16,
		AppendOnly:     true,
		AppendFilename: aofFilename,
		AppendFsync:    aof.FsyncAlways,
	}
	master := mockServer()
	aofHandler, err := NewPersister(master, config.Properties.AppendFilename, true, config.Properties.AppendFsync)
	if err != nil {
		panic(err)
	}
	master.bindPersister(aofHandler)

	masterConn := connection.NewFakeConn()
	resp := master.Exec(masterConn, utils.ToCmdLine("SET", "a", "a"))
	asserts.AssertNotError(t, resp)
	resp = master.Exec(masterConn, utils.ToCmdLine("SET", "b", "b"))
	asserts.AssertNotError(t, resp)
	time.Sleep(time.Millisecond * 100) // wait write aof

	err = master.rewriteRDB()
	if err != nil {
		t.Error(err)
		return
	}
	resp = master.Exec(masterConn, utils.ToCmdLine("SET", "c", "c"))
	asserts.AssertNotError(t, resp)
	time.Sleep(time.Millisecond * 100) // wait write aof

	// set slave
	slave := mockServer()
	replConn := connection.NewFakeConn()
	master.Exec(replConn, utils.ToCmdLine("psync", "?", "-1"))
	masterChan := parser.ParseStream(replConn)
	psyncPayload := <-masterChan
	if psyncPayload.Err != nil {
		t.Errorf("master bad protocol: %v", psyncPayload.Err)
		return
	}
	psyncHeader, ok := psyncPayload.Data.(*protocol.StatusReply)
	if !ok {
		t.Error("psync header is not a status reply")
		return
	}
	headers := strings.Split(psyncHeader.Status, " ")
	if len(headers) != 3 {
		t.Errorf("illegal psync header: %s", psyncHeader.Status)
		return
	}

	replId := headers[1]
	replOffset, err := strconv.ParseInt(headers[2], 10, 64)
	if err != nil {
		t.Errorf("illegal offset: %s", headers[2])
		return
	}
	t.Logf("repl id: %s, offset: %d", replId, replOffset)

	rdbPayload := <-masterChan
	if rdbPayload.Err != nil {
		t.Error("read response failed: " + rdbPayload.Err.Error())
		return
	}
	rdbReply, ok := rdbPayload.Data.(*protocol.BulkReply)
	if !ok {
		t.Error("illegal payload header: " + string(rdbPayload.Data.ToBytes()))
		return
	}

	rdbDec := rdb.NewDecoder(bytes.NewReader(rdbReply.Arg))
	err = slave.LoadRDB(rdbDec)
	if err != nil {
		t.Error("import rdb failed: " + err.Error())
		return
	}

	slaveConn := connection.NewFakeConn()
	resp = slave.Exec(slaveConn, utils.ToCmdLine("get", "a"))
	asserts.AssertBulkReply(t, resp, "a")
	resp = slave.Exec(slaveConn, utils.ToCmdLine("get", "b"))
	asserts.AssertBulkReply(t, resp, "b")

	master.masterCron()
	for {
		payload := <-masterChan
		if payload.Err != nil {
			t.Error(payload.Err)
			return
		}
		cmdLine, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			t.Error("unexpected payload: " + string(payload.Data.ToBytes()))
			return
		}
		slave.Exec(replConn, cmdLine.Args)
		n := len(cmdLine.ToBytes())
		slave.slaveStatus.replOffset += int64(n)
		if string(cmdLine.Args[0]) != "ping" {
			break
		}
	}
	resp = slave.Exec(slaveConn, utils.ToCmdLine("get", "c"))
	asserts.AssertBulkReply(t, resp, "c")
}
