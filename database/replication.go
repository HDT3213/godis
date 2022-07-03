package database

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
	rdb "github.com/hdt3213/rdb/parser"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	masterRole = iota
	slaveRole
)

type replicationStatus struct {
	mutex    sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	modCount int32 // execSlaveOf() or reconnect will cause modCount++

	masterHost string
	masterPort int

	masterConn   net.Conn
	masterChan   <-chan *parser.Payload
	replId       string
	replOffset   int64
	lastRecvTime time.Time
	running      sync.WaitGroup
}

func initReplStatus() *replicationStatus {
	repl := &replicationStatus{}
	// start cron
	return repl
}

func (mdb *MultiDB) startReplCron() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Error("panic", err)
			}
		}()
		ticker := time.Tick(time.Second)
		for range ticker {
			mdb.slaveCron()
		}
	}()
}

func (mdb *MultiDB) execSlaveOf(c redis.Connection, args [][]byte) redis.Reply {
	if strings.ToLower(string(args[0])) == "no" &&
		strings.ToLower(string(args[1])) == "one" {
		mdb.slaveOfNone()
		return protocol.MakeOkReply()
	}
	host := string(args[0])
	port, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	mdb.replication.mutex.Lock()
	atomic.StoreInt32(&mdb.role, slaveRole)
	mdb.replication.masterHost = host
	mdb.replication.masterPort = port
	// use buffered channel in case receiver goroutine exited before controller send stop signal
	atomic.AddInt32(&mdb.replication.modCount, 1)
	mdb.replication.mutex.Unlock()
	go mdb.syncWithMaster()
	return protocol.MakeOkReply()
}

func (mdb *MultiDB) slaveOfNone() {
	mdb.replication.mutex.Lock()
	defer mdb.replication.mutex.Unlock()
	mdb.replication.masterHost = ""
	mdb.replication.masterPort = 0
	mdb.replication.replId = ""
	mdb.replication.replOffset = -1
	mdb.replication.stopSlaveWithMutex()
}

// stopSlaveWithMutex stops in-progress connectWithMaster/fullSync/receiveAOF
// invoker should have replication mutex
func (repl *replicationStatus) stopSlaveWithMutex() {
	// update modCount to stop connectWithMaster and fullSync
	atomic.AddInt32(&repl.modCount, 1)
	// send cancel to receiveAOF
	if repl.cancel != nil {
		repl.cancel()
		repl.running.Wait()
	}
	repl.ctx = context.Background()
	repl.cancel = nil
	if repl.masterConn != nil {
		_ = repl.masterConn.Close() // parser.ParseStream will close masterChan
	}
	repl.masterConn = nil
	repl.masterChan = nil
}

func (repl *replicationStatus) close() error {
	repl.mutex.Lock()
	defer repl.mutex.Unlock()
	repl.stopSlaveWithMutex()
	return nil
}

func (mdb *MultiDB) syncWithMaster() {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()
	mdb.replication.mutex.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	mdb.replication.ctx = ctx
	mdb.replication.cancel = cancel
	mdb.replication.mutex.Unlock()
	err := mdb.connectWithMaster()
	if err != nil {
		// full sync failed, abort
		return
	}
	err = mdb.doPsync()
	if err != nil {
		// full sync failed, abort
		return
	}
	err = mdb.receiveAOF()
	if err != nil {
		// full sync failed, abort
		return
	}
}

func (mdb *MultiDB) connectWithMaster() error {
	modCount := atomic.LoadInt32(&mdb.replication.modCount)
	addr := mdb.replication.masterHost + ":" + strconv.Itoa(mdb.replication.masterPort)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		mdb.slaveOfNone() // abort
		return errors.New("connect master failed " + err.Error())
	}
	masterChan := parser.ParseStream(conn)

	// ping
	pingCmdLine := utils.ToCmdLine("ping")
	pingReq := protocol.MakeMultiBulkReply(pingCmdLine)
	_, err = conn.Write(pingReq.ToBytes())
	if err != nil {
		return errors.New("send failed " + err.Error())
	}
	pingResp := <-masterChan
	if pingResp.Err != nil {
		return errors.New("read response failed: " + pingResp.Err.Error())
	}
	switch reply := pingResp.Data.(type) {
	case *protocol.StandardErrReply:
		if !strings.HasPrefix(reply.Error(), "NOAUTH") &&
			!strings.HasPrefix(reply.Error(), "NOPERM") &&
			!strings.HasPrefix(reply.Error(), "ERR operation not permitted") {
			logger.Error("Error reply to PING from master: " + string(reply.ToBytes()))
			mdb.slaveOfNone() // abort
			return nil
		}
	}

	// just to reduce duplication of code
	sendCmdToMaster := func(conn net.Conn, cmdLine CmdLine, masterChan <-chan *parser.Payload) error {
		req := protocol.MakeMultiBulkReply(cmdLine)
		_, err := conn.Write(req.ToBytes())
		if err != nil {
			mdb.slaveOfNone() // abort
			return errors.New("send failed " + err.Error())
		}
		resp := <-masterChan
		if resp.Err != nil {
			mdb.slaveOfNone() // abort
			return errors.New("read response failed: " + resp.Err.Error())
		}
		if !protocol.IsOKReply(resp.Data) {
			mdb.slaveOfNone() // abort
			return errors.New("unexpected auth response: " + string(resp.Data.ToBytes()))
		}
		return nil
	}

	// auth
	if config.Properties.MasterAuth != "" {
		authCmdLine := utils.ToCmdLine("auth", config.Properties.MasterAuth)
		err = sendCmdToMaster(conn, authCmdLine, masterChan)
		if err != nil {
			return err
		}
	}

	// announce port
	var port int
	if config.Properties.SlaveAnnouncePort != 0 {
		port = config.Properties.SlaveAnnouncePort
	} else {
		port = config.Properties.Port
	}
	portCmdLine := utils.ToCmdLine("REPLCONF", "listening-port", strconv.Itoa(port))
	err = sendCmdToMaster(conn, portCmdLine, masterChan)
	if err != nil {
		return err
	}

	// announce ip
	if config.Properties.SlaveAnnounceIP != "" {
		ipCmdLine := utils.ToCmdLine("REPLCONF", "ip-address", config.Properties.SlaveAnnounceIP)
		err = sendCmdToMaster(conn, ipCmdLine, masterChan)
		if err != nil {
			return err
		}
	}

	// announce capacity
	capaCmdLine := utils.ToCmdLine("REPLCONF", "capa", "psync2")
	err = sendCmdToMaster(conn, capaCmdLine, masterChan)
	if err != nil {
		return err
	}

	// update connection
	mdb.replication.mutex.Lock()
	if mdb.replication.modCount != modCount {
		// replication conf changed during connecting and waiting mutex
		return nil
	}
	mdb.replication.masterConn = conn
	mdb.replication.masterChan = masterChan
	mdb.replication.mutex.Unlock()
	return nil
}

// doPsync send psync to master and sync repl-id then load rdb
func (mdb *MultiDB) doPsync() error {
	modCount := atomic.LoadInt32(&mdb.replication.modCount)
	psyncCmdLine := utils.ToCmdLine("psync", "?", "-1")
	psyncReq := protocol.MakeMultiBulkReply(psyncCmdLine)
	_, err := mdb.replication.masterConn.Write(psyncReq.ToBytes())
	if err != nil {
		return errors.New("send failed " + err.Error())
	}
	ch := mdb.replication.masterChan
	psyncPayload1 := <-ch
	if psyncPayload1.Err != nil {
		return errors.New("read response failed: " + psyncPayload1.Err.Error())
	}
	psyncHeader, ok := psyncPayload1.Data.(*protocol.StatusReply)
	if !ok {
		return errors.New("illegal payload header: " + string(psyncPayload1.Data.ToBytes()))
	}
	headers := strings.Split(psyncHeader.Status, " ")
	if len(headers) != 3 {
		return errors.New("illegal payload header: " + psyncHeader.Status)
	}
	logger.Info("receive psync header from master")
	psyncPayload2 := <-ch
	if psyncPayload2.Err != nil {
		return errors.New("read response failed: " + psyncPayload2.Err.Error())
	}
	psyncData, ok := psyncPayload2.Data.(*protocol.BulkReply)
	if !ok {
		return errors.New("illegal payload header: " + string(psyncPayload2.Data.ToBytes()))
	}
	logger.Info(fmt.Sprintf("receive %d bytes of rdb from master", len(psyncData.Arg)))
	rdbDec := rdb.NewDecoder(bytes.NewReader(psyncData.Arg))
	rdbHolder := MakeBasicMultiDB()
	err = dumpRDB(rdbDec, rdbHolder)
	if err != nil {
		return errors.New("dump rdb failed: " + err.Error())
	}

	mdb.replication.mutex.Lock()
	defer mdb.replication.mutex.Unlock()
	if mdb.replication.modCount != modCount {
		// replication conf changed during connecting and waiting mutex
		return nil
	}
	mdb.replication.replId = headers[1]
	mdb.replication.replOffset, err = strconv.ParseInt(headers[2], 10, 64)
	if err != nil {
		return errors.New("get illegal repl offset: " + headers[2])
	}
	logger.Info("full resync from master: " + mdb.replication.replId)
	logger.Info("current offset:", mdb.replication.replOffset)
	for i, newDB := range rdbHolder.dbSet {
		oldDB := mdb.selectDB(i)
		oldDB.Load(newDB)
	}
	// there is no CRLF between RDB and following AOF, reset stream to avoid parser error
	mdb.replication.masterChan = parser.ParseStream(mdb.replication.masterConn)
	// fixme: update aof file
	return nil
}

func (mdb *MultiDB) receiveAOF() error {
	conn := connection.NewConn(mdb.replication.masterConn)
	conn.SetRole(connection.ReplicationRecvCli)
	mdb.replication.mutex.Lock()
	modCount := mdb.replication.modCount
	done := mdb.replication.ctx.Done()
	mdb.replication.mutex.Unlock()
	if done == nil {
		// stopSlaveWithMutex() is called during waiting mutex
		return nil
	}
	mdb.replication.running.Add(1)
	defer mdb.replication.running.Done()
	for {
		select {
		case payload, open := <-mdb.replication.masterChan:
			if !open {
				return errors.New("master channel unexpected close")
			}
			if payload.Err != nil {
				return payload.Err
			}
			cmdLine, ok := payload.Data.(*protocol.MultiBulkReply)
			if !ok {
				return errors.New("unexpected payload: " + string(payload.Data.ToBytes()))
			}
			mdb.replication.mutex.Lock()
			if mdb.replication.modCount != modCount {
				// replication conf changed during connecting and waiting mutex
				return nil
			}
			mdb.Exec(conn, cmdLine.Args)
			// todo: directly get size from socket
			n := len(cmdLine.ToBytes())
			mdb.replication.replOffset += int64(n)
			logger.Info(fmt.Sprintf("receive %d bytes from master, current offset %d",
				n, mdb.replication.replOffset))
			mdb.replication.mutex.Unlock()
		case <-done:
			return nil
		}
	}
}

func (mdb *MultiDB) slaveCron() {
	repl := mdb.replication
	if repl.masterConn == nil {
		return
	}
	replTimeout := 60 * time.Second
	if config.Properties.ReplTimeout != 0 {
		replTimeout = time.Duration(config.Properties.ReplTimeout) * time.Second
	}
	minLastRecvTime := time.Now().Add(-replTimeout)
	if repl.lastRecvTime.Before(minLastRecvTime) {
		// reconnect with master
		err := mdb.reconnectWithMaster()
		if err != nil {
			logger.Error("send failed " + err.Error())
		}
		return
	}
	// send ack to master
	err := repl.sendAck2Master()
	if err != nil {
		logger.Error("send failed " + err.Error())
	}
}

// Send a REPLCONF ACK command to the master to inform it about the current processed offset
func (repl *replicationStatus) sendAck2Master() error {
	psyncCmdLine := utils.ToCmdLine("REPLCONF", "ACK",
		strconv.FormatInt(repl.replOffset, 10))
	psyncReq := protocol.MakeMultiBulkReply(psyncCmdLine)
	_, err := repl.masterConn.Write(psyncReq.ToBytes())
	//logger.Info("send ack to master")
	return err
}

func (mdb *MultiDB) reconnectWithMaster() error {
	logger.Info("reconnecting with master")
	mdb.replication.mutex.Lock()
	defer mdb.replication.mutex.Unlock()
	mdb.replication.stopSlaveWithMutex()
	go mdb.syncWithMaster()
	return nil
}
