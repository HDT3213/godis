package database

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/atomic"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

const (
	slaveStateHandShake = uint8(iota)
	slaveStateWaitSaveEnd
	slaveStateSendingRDB
	slaveStateOnline
)

const (
	bgSaveIdle = uint8(iota)
	bgSaveRunning
	bgSaveFinish
)

const (
	slaveCapacityNone = 0
	slaveCapacityEOF  = 1 << iota
	slaveCapacityPsync2
)

// slaveClient stores slave status in the view of master
type slaveClient struct {
	conn         redis.Connection
	state        uint8
	offset       int64
	lastAckTime  time.Time
	announceIp   string
	announcePort int
	capacity     uint8
}

// aofListener is currently only responsible for updating the backlog
type replBacklog struct {
	buf           []byte
	beginOffset   int64
	currentOffset int64
}

func (backlog *replBacklog) appendBytes(bin []byte) {
	backlog.buf = append(backlog.buf, bin...)
	backlog.currentOffset += int64(len(bin))
}

func (backlog *replBacklog) getSnapshot() ([]byte, int64) {
	return backlog.buf[:], backlog.currentOffset
}

func (backlog *replBacklog) getSnapshotAfter(beginOffset int64) ([]byte, int64) {
	beg := beginOffset - backlog.beginOffset
	return backlog.buf[beg:], backlog.currentOffset
}

func (backlog *replBacklog) isValidOffset(offset int64) bool {
	return offset >= backlog.beginOffset && offset < backlog.currentOffset
}

type masterStatus struct {
	mu           sync.RWMutex
	replId       string
	backlog      *replBacklog
	slaveMap     map[redis.Connection]*slaveClient
	waitSlaves   map[*slaveClient]struct{}
	onlineSlaves map[*slaveClient]struct{}
	bgSaveState  uint8
	rdbFilename  string
	aofListener  *replAofListener
	rewriting    atomic.Boolean
}

// bgSaveForReplication does bg-save and send rdb to waiting slaves
func (server *Server) bgSaveForReplication() {
	go func() {
		defer func() {
			if e := recover(); e != nil {
				logger.Errorf("panic: %v", e)
			}
		}()
		if err := server.saveForReplication(); err != nil {
			logger.Errorf("save for replication error: %v", err)
		}
	}()

}

// saveForReplication does bg-save and send rdb to waiting slaves
func (server *Server) saveForReplication() error {
	rdbFile, err := ioutil.TempFile("", "*.rdb")
	if err != nil {
		return fmt.Errorf("create temp rdb failed: %v", err)
	}
	rdbFilename := rdbFile.Name()
	server.masterStatus.mu.Lock()
	server.masterStatus.bgSaveState = bgSaveRunning
	server.masterStatus.rdbFilename = rdbFilename // todo: can reuse config.Properties.RDBFilename?
	aofListener := &replAofListener{
		mdb:     server,
		backlog: server.masterStatus.backlog,
	}
	server.masterStatus.aofListener = aofListener
	server.masterStatus.mu.Unlock()

	err = server.persister.GenerateRDBForReplication(rdbFilename, aofListener, nil)
	if err != nil {
		return err
	}
	aofListener.readyToSend = true

	// change bgSaveState and get waitSlaves for sending
	waitSlaves := make(map[*slaveClient]struct{})
	server.masterStatus.mu.Lock()
	server.masterStatus.bgSaveState = bgSaveFinish
	for slave := range server.masterStatus.waitSlaves {
		waitSlaves[slave] = struct{}{}
	}
	server.masterStatus.waitSlaves = nil
	server.masterStatus.mu.Unlock()

	// send rdb to waiting slaves
	for slave := range waitSlaves {
		err = server.masterFullReSyncWithSlave(slave)
		if err != nil {
			server.removeSlave(slave)
			logger.Errorf("masterFullReSyncWithSlave error: %v", err)
			continue
		}
	}
	return nil
}

func (server *Server) rewriteRDB() error {
	rdbFile, err := ioutil.TempFile("", "*.rdb")
	if err != nil {
		return fmt.Errorf("create temp rdb failed: %v", err)
	}
	rdbFilename := rdbFile.Name()
	newBacklog := &replBacklog{}
	aofListener := &replAofListener{
		backlog: newBacklog,
		mdb:     server,
	}
	hook := func() {
		// pausing aof first, then lock masterStatus.
		// use the same order as replAofListener to avoid dead lock
		server.masterStatus.mu.Lock()
		defer server.masterStatus.mu.Unlock()
		newBacklog.beginOffset = server.masterStatus.backlog.currentOffset
	}
	err = server.persister.GenerateRDBForReplication(rdbFilename, aofListener, hook)
	if err != nil { // wait rdb result
		return err
	}
	server.masterStatus.mu.Lock()
	server.masterStatus.rdbFilename = rdbFilename
	server.masterStatus.backlog = newBacklog
	server.persister.RemoveListener(server.masterStatus.aofListener)
	server.masterStatus.aofListener = aofListener
	server.masterStatus.mu.Unlock()
	// It is ok to know that new backlog is ready later, so we change readyToSend without sync
	// But setting readyToSend=true must after new backlog is really ready (that means master.mu.Unlock)
	aofListener.readyToSend = true
	return nil
}

// masterFullReSyncWithSlave send replication header, rdb file and all backlogs to slave
func (server *Server) masterFullReSyncWithSlave(slave *slaveClient) error {
	// write replication header
	header := "+FULLRESYNC " + server.masterStatus.replId + " " +
		strconv.FormatInt(server.masterStatus.backlog.beginOffset, 10) + protocol.CRLF
	_, err := slave.conn.Write([]byte(header))
	if err != nil {
		return fmt.Errorf("write replication header to slave failed: %v", err)
	}
	// send rdb
	rdbFile, err := os.Open(server.masterStatus.rdbFilename)
	if err != nil {
		return fmt.Errorf("open rdb file %s for replication error: %v", server.masterStatus.rdbFilename, err)
	}
	slave.state = slaveStateSendingRDB
	rdbInfo, _ := os.Stat(server.masterStatus.rdbFilename)
	rdbSize := rdbInfo.Size()
	rdbHeader := "$" + strconv.FormatInt(rdbSize, 10) + protocol.CRLF
	_, err = slave.conn.Write([]byte(rdbHeader))
	if err != nil {
		return fmt.Errorf("write rdb header to slave failed: %v", err)
	}
	_, err = io.Copy(slave.conn, rdbFile)
	if err != nil {
		return fmt.Errorf("write rdb file to slave failed: %v", err)
	}

	// send backlog
	server.masterStatus.mu.RLock()
	backlog, currentOffset := server.masterStatus.backlog.getSnapshot()
	server.masterStatus.mu.RUnlock()
	_, err = slave.conn.Write(backlog)
	if err != nil {
		return fmt.Errorf("full resync write backlog to slave failed: %v", err)
	}

	// set slave as online
	server.setSlaveOnline(slave, currentOffset)
	return nil
}

var cannotPartialSync = errors.New("cannot do partial sync")

func (server *Server) masterTryPartialSyncWithSlave(slave *slaveClient, replId string, slaveOffset int64) error {
	server.masterStatus.mu.RLock()
	if replId != server.masterStatus.replId {
		server.masterStatus.mu.RUnlock()
		return cannotPartialSync
	}
	if !server.masterStatus.backlog.isValidOffset(slaveOffset) {
		server.masterStatus.mu.RUnlock()
		return cannotPartialSync
	}
	backlog, currentOffset := server.masterStatus.backlog.getSnapshotAfter(slaveOffset)
	server.masterStatus.mu.RUnlock()

	// send replication header
	header := "+CONTINUE " + server.masterStatus.replId + protocol.CRLF
	_, err := slave.conn.Write([]byte(header))
	if err != nil {
		return fmt.Errorf("write replication header to slave failed: %v", err)
	}
	// send backlog
	_, err = slave.conn.Write(backlog)
	if err != nil {
		return fmt.Errorf("partial resync write backlog to slave failed: %v", err)
	}

	// set slave online
	server.setSlaveOnline(slave, currentOffset)
	return nil
}

// masterSendUpdatesToSlave only sends data to online slaves after bgSave is finished
// if bgSave is running, updates will be sent after the saving finished
func (server *Server) masterSendUpdatesToSlave() error {
	onlineSlaves := make(map[*slaveClient]struct{})
	server.masterStatus.mu.RLock()
	beginOffset := server.masterStatus.backlog.beginOffset
	backlog, currentOffset := server.masterStatus.backlog.getSnapshot()
	for slave := range server.masterStatus.onlineSlaves {
		onlineSlaves[slave] = struct{}{}
	}
	server.masterStatus.mu.RUnlock()
	for slave := range onlineSlaves {
		slaveBeginOffset := slave.offset - beginOffset
		_, err := slave.conn.Write(backlog[slaveBeginOffset:])
		if err != nil {
			logger.Errorf("send updates backlog to slave failed: %v", err)
			server.removeSlave(slave)
			continue
		}
		slave.offset = currentOffset
	}
	return nil
}

func (server *Server) execPSync(c redis.Connection, args [][]byte) redis.Reply {
	replId := string(args[0])
	replOffset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	server.masterStatus.mu.Lock()
	defer server.masterStatus.mu.Unlock()
	slave := server.masterStatus.slaveMap[c]
	if slave == nil {
		slave = &slaveClient{
			conn: c,
		}
		c.SetSlave()
		server.masterStatus.slaveMap[c] = slave
	}
	if server.masterStatus.bgSaveState == bgSaveIdle {
		slave.state = slaveStateWaitSaveEnd
		server.masterStatus.waitSlaves[slave] = struct{}{}
		server.bgSaveForReplication()
	} else if server.masterStatus.bgSaveState == bgSaveRunning {
		slave.state = slaveStateWaitSaveEnd
		server.masterStatus.waitSlaves[slave] = struct{}{}
	} else if server.masterStatus.bgSaveState == bgSaveFinish {
		go func() {
			defer func() {
				if e := recover(); e != nil {
					logger.Errorf("panic: %v", e)
				}
			}()
			err := server.masterTryPartialSyncWithSlave(slave, replId, replOffset)
			if err == nil {
				return
			}
			if err != cannotPartialSync {
				server.removeSlave(slave)
				logger.Errorf("masterTryPartialSyncWithSlave error: %v", err)
				return
			}
			// assert err == cannotPartialSync
			if err := server.masterFullReSyncWithSlave(slave); err != nil {
				server.removeSlave(slave)
				logger.Errorf("masterFullReSyncWithSlave error: %v", err)
				return
			}
		}()
	}
	return &protocol.NoReply{}
}

func (server *Server) execReplConf(c redis.Connection, args [][]byte) redis.Reply {
	if len(args)%2 != 0 {
		return protocol.MakeSyntaxErrReply()
	}
	server.masterStatus.mu.RLock()
	slave := server.masterStatus.slaveMap[c]
	server.masterStatus.mu.RUnlock()
	for i := 0; i < len(args); i += 2 {
		key := strings.ToLower(string(args[i]))
		value := string(args[i+1])
		switch key {
		case "ack":
			offset, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			slave.offset = offset
			slave.lastAckTime = time.Now()
			return &protocol.NoReply{}
		}
	}
	return protocol.MakeOkReply()
}

func (server *Server) removeSlave(slave *slaveClient) {
	server.masterStatus.mu.Lock()
	defer server.masterStatus.mu.Unlock()
	_ = slave.conn.Close()
	delete(server.masterStatus.slaveMap, slave.conn)
	delete(server.masterStatus.waitSlaves, slave)
	delete(server.masterStatus.onlineSlaves, slave)
	logger.Info("disconnect with slave " + slave.conn.Name())
}

func (server *Server) setSlaveOnline(slave *slaveClient, currentOffset int64) {
	server.masterStatus.mu.Lock()
	defer server.masterStatus.mu.Unlock()
	slave.state = slaveStateOnline
	slave.offset = currentOffset
	server.masterStatus.onlineSlaves[slave] = struct{}{}
}

var pingBytes = protocol.MakeMultiBulkReply(utils.ToCmdLine("ping")).ToBytes()

const maxBacklogSize = 10 * 1024 * 1024 // 10MB

func (server *Server) masterCron() {
	server.masterStatus.mu.Lock()
	if len(server.masterStatus.slaveMap) == 0 { // no slaves, do nothing
		server.masterStatus.mu.Unlock()
		return
	}
	if server.masterStatus.bgSaveState == bgSaveFinish {
		server.masterStatus.backlog.appendBytes(pingBytes)
	}
	backlogSize := len(server.masterStatus.backlog.buf)
	server.masterStatus.mu.Unlock()
	if err := server.masterSendUpdatesToSlave(); err != nil {
		logger.Errorf("masterSendUpdatesToSlave error: %v", err)
	}
	if backlogSize > maxBacklogSize && !server.masterStatus.rewriting.Get() {
		go func() {
			server.masterStatus.rewriting.Set(true)
			defer server.masterStatus.rewriting.Set(false)
			if err := server.rewriteRDB(); err != nil {
				server.masterStatus.rewriting.Set(false)
				logger.Errorf("rewrite error: %v", err)
			}
		}()
	}
}

// replAofListener is an implementation for aof.Listener
type replAofListener struct {
	mdb         *Server
	backlog     *replBacklog // may NOT be mdb.masterStatus.backlog
	readyToSend bool
}

func (listener *replAofListener) Callback(cmdLines []CmdLine) {
	listener.mdb.masterStatus.mu.Lock()
	for _, cmdLine := range cmdLines {
		reply := protocol.MakeMultiBulkReply(cmdLine)
		listener.backlog.appendBytes(reply.ToBytes())
	}
	listener.mdb.masterStatus.mu.Unlock()
	// listener could receive updates generated during rdb saving in progress
	// Do not send updates to slave before rdb saving is finished
	if listener.readyToSend {
		if err := listener.mdb.masterSendUpdatesToSlave(); err != nil {
			logger.Errorf("masterSendUpdatesToSlave after receive aof error: %v", err)
		}
	}
}

func (server *Server) initMasterStatus() {
	server.masterStatus = &masterStatus{
		mu:           sync.RWMutex{},
		replId:       utils.RandHexString(40),
		backlog:      &replBacklog{},
		slaveMap:     make(map[redis.Connection]*slaveClient),
		waitSlaves:   make(map[*slaveClient]struct{}),
		onlineSlaves: make(map[*slaveClient]struct{}),
		bgSaveState:  bgSaveIdle,
		rdbFilename:  "",
	}
}

func (server *Server) stopMaster() {
	server.masterStatus.mu.Lock()
	defer server.masterStatus.mu.Unlock()

	// disconnect with slave
	for _, slave := range server.masterStatus.slaveMap {
		_ = slave.conn.Close()
		delete(server.masterStatus.slaveMap, slave.conn)
		delete(server.masterStatus.waitSlaves, slave)
		delete(server.masterStatus.onlineSlaves, slave)
	}

	// clean master status
	if server.persister != nil {
		server.persister.RemoveListener(server.masterStatus.aofListener)
	}
	_ = os.Remove(server.masterStatus.rdbFilename)
	server.masterStatus.rdbFilename = ""
	server.masterStatus.replId = ""
	server.masterStatus.backlog = &replBacklog{}
	server.masterStatus.slaveMap = make(map[redis.Connection]*slaveClient)
	server.masterStatus.waitSlaves = make(map[*slaveClient]struct{})
	server.masterStatus.onlineSlaves = make(map[*slaveClient]struct{})
	server.masterStatus.bgSaveState = bgSaveIdle
}
