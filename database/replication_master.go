package database

import (
	"errors"
	"fmt"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/atomic"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
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
	salveCapacityNone = 0
	salveCapacityEOF  = 1 << iota
	salveCapacityPsync2
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

// aofListener 只负责更新 backlog
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

func (mdb *MultiDB) bgSaveForReplication() {
	go func() {
		defer func() {
			if e := recover(); e != nil {
				logger.Errorf("panic: %v", e)
			}
		}()
		if err := mdb.saveForReplication(); err != nil {
			logger.Errorf("save for replication error: %v", err)
		}
	}()

}

// saveForReplication does bg-save and send rdb to waiting slaves
func (mdb *MultiDB) saveForReplication() error {
	rdbFile, err := ioutil.TempFile("", "*.rdb")
	if err != nil {
		return fmt.Errorf("create temp rdb failed: %v", err)
	}
	rdbFilename := rdbFile.Name()
	mdb.masterStatus.mu.Lock()
	mdb.masterStatus.bgSaveState = bgSaveRunning
	mdb.masterStatus.rdbFilename = rdbFilename // todo: can reuse config.Properties.RDBFilename?
	aofListener := &replAofListener{
		mdb:     mdb,
		backlog: mdb.masterStatus.backlog,
	}
	mdb.masterStatus.aofListener = aofListener
	mdb.masterStatus.mu.Unlock()

	err = mdb.aofHandler.Rewrite2RDBForReplication(rdbFilename, aofListener, nil)
	if err != nil {
		return err
	}
	aofListener.readyToSend = true

	// change bgSaveState and get waitSlaves for sending
	waitSlaves := make(map[*slaveClient]struct{})
	mdb.masterStatus.mu.Lock()
	mdb.masterStatus.bgSaveState = bgSaveFinish
	for slave := range mdb.masterStatus.waitSlaves {
		waitSlaves[slave] = struct{}{}
	}
	mdb.masterStatus.waitSlaves = nil
	mdb.masterStatus.mu.Unlock()

	for slave := range waitSlaves {
		err = mdb.masterFullReSyncWithSlave(slave)
		if err != nil {
			mdb.removeSlave(slave)
			logger.Errorf("masterFullReSyncWithSlave error: %v", err)
			continue
		}
	}
	return nil
}

func (mdb *MultiDB) rewriteRDB() error {
	rdbFile, err := ioutil.TempFile("", "*.rdb")
	if err != nil {
		return fmt.Errorf("create temp rdb failed: %v", err)
	}
	rdbFilename := rdbFile.Name()
	newBacklog := &replBacklog{}
	aofListener := &replAofListener{
		backlog: newBacklog,
		mdb:     mdb,
	}
	hook := func() {
		// pausing aof first, then lock masterStatus.
		// use the same order as replAofListener to avoid dead lock
		mdb.masterStatus.mu.Lock()
		defer mdb.masterStatus.mu.Unlock()
		newBacklog.beginOffset = mdb.masterStatus.backlog.currentOffset
	}
	err = mdb.aofHandler.Rewrite2RDBForReplication(rdbFilename, aofListener, hook)
	if err != nil { // wait rdb result
		return err
	}
	mdb.masterStatus.mu.Lock()
	mdb.masterStatus.rdbFilename = rdbFilename
	mdb.masterStatus.backlog = newBacklog
	mdb.masterStatus.mu.Unlock()
	// It is ok to know that new backlog is ready later, so we change readyToSend without sync
	// But setting readyToSend=true must after new backlog is really ready (that means master.mu.Unlock)
	aofListener.readyToSend = true
	return nil
}

// masterFullReSyncWithSlave send replication header, rdb file and all backlogs to slave
func (mdb *MultiDB) masterFullReSyncWithSlave(slave *slaveClient) error {
	// write replication header
	header := "+FULLRESYNC " + mdb.masterStatus.replId + " " +
		strconv.FormatInt(mdb.masterStatus.backlog.beginOffset, 10) + protocol.CRLF
	_, err := slave.conn.Write([]byte(header))
	if err != nil {
		return fmt.Errorf("write replication header to slave failed: %v", err)
	}
	// send rdb
	rdbFile, err := os.Open(mdb.masterStatus.rdbFilename)
	if err != nil {
		return fmt.Errorf("open rdb file %s for replication error: %v", mdb.masterStatus.rdbFilename, err)
	}
	slave.state = slaveStateSendingRDB
	rdbInfo, _ := os.Stat(mdb.masterStatus.rdbFilename)
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
	mdb.masterStatus.mu.RLock()
	backlog, currentOffset := mdb.masterStatus.backlog.getSnapshot()
	mdb.masterStatus.mu.RUnlock()
	_, err = slave.conn.Write(backlog)
	if err != nil {
		return fmt.Errorf("full resync write backlog to slave failed: %v", err)
	}

	// set slave as online
	mdb.setSlaveOnline(slave, currentOffset)
	return nil
}

var cannotPartialSync = errors.New("cannot do partial sync")

func (mdb *MultiDB) masterTryPartialSyncWithSlave(slave *slaveClient, replId string, slaveOffset int64) error {
	mdb.masterStatus.mu.RLock()
	if replId != mdb.masterStatus.replId {
		mdb.masterStatus.mu.RUnlock()
		return cannotPartialSync
	}
	if !mdb.masterStatus.backlog.isValidOffset(slaveOffset) {
		mdb.masterStatus.mu.RUnlock()
		return cannotPartialSync
	}
	backlog, currentOffset := mdb.masterStatus.backlog.getSnapshotAfter(slaveOffset)
	mdb.masterStatus.mu.RUnlock()

	// send replication header
	header := "+CONTINUE " + mdb.masterStatus.replId + protocol.CRLF
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
	mdb.setSlaveOnline(slave, currentOffset)
	return nil
}

// masterSendUpdatesToSlave only sends data to online slaves after bgSave is finished
// if bgSave is running, updates will be sent after the saving finished
func (mdb *MultiDB) masterSendUpdatesToSlave() error {
	onlineSlaves := make(map[*slaveClient]struct{})
	mdb.masterStatus.mu.RLock()
	beginOffset := mdb.masterStatus.backlog.beginOffset
	backlog, currentOffset := mdb.masterStatus.backlog.getSnapshot()
	for slave := range mdb.masterStatus.onlineSlaves {
		onlineSlaves[slave] = struct{}{}
	}
	mdb.masterStatus.mu.RUnlock()
	for slave := range onlineSlaves {
		slaveBeginOffset := slave.offset - beginOffset
		_, err := slave.conn.Write(backlog[slaveBeginOffset:])
		if err != nil {
			logger.Errorf("send updates backlog to slave failed: %v", err)
			mdb.removeSlave(slave)
			continue
		}
		slave.offset = currentOffset
	}
	return nil
}

func (mdb *MultiDB) execPSync(c redis.Connection, args [][]byte) redis.Reply {
	replId := string(args[0])
	replOffset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	mdb.masterStatus.mu.Lock()
	defer mdb.masterStatus.mu.Unlock()
	slave := mdb.masterStatus.slaveMap[c]
	if slave == nil {
		slave = &slaveClient{
			conn: c,
		}
		c.SetSlave()
		mdb.masterStatus.slaveMap[c] = slave
	}
	if mdb.masterStatus.bgSaveState == bgSaveIdle {
		slave.state = slaveStateWaitSaveEnd
		mdb.masterStatus.waitSlaves[slave] = struct{}{}
		mdb.bgSaveForReplication()
	} else if mdb.masterStatus.bgSaveState == bgSaveRunning {
		slave.state = slaveStateWaitSaveEnd
		mdb.masterStatus.waitSlaves[slave] = struct{}{}
	} else if mdb.masterStatus.bgSaveState == bgSaveFinish {
		go func() {
			defer func() {
				if e := recover(); e != nil {
					logger.Errorf("panic: %v", e)
				}
			}()
			err := mdb.masterTryPartialSyncWithSlave(slave, replId, replOffset)
			if err == nil {
				return
			}
			if err != nil && err != cannotPartialSync {
				mdb.removeSlave(slave)
				logger.Errorf("masterTryPartialSyncWithSlave error: %v", err)
				return
			}
			// assert err == cannotPartialSync
			if err := mdb.masterFullReSyncWithSlave(slave); err != nil {
				mdb.removeSlave(slave)
				logger.Errorf("masterFullReSyncWithSlave error: %v", err)
				return
			}
		}()
	}
	return &protocol.NoReply{}
}

func (mdb *MultiDB) execReplConf(c redis.Connection, args [][]byte) redis.Reply {
	if len(args)%2 != 0 {
		return protocol.MakeSyntaxErrReply()
	}
	mdb.masterStatus.mu.RLock()
	slave := mdb.masterStatus.slaveMap[c]
	mdb.masterStatus.mu.RUnlock()
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

func (mdb *MultiDB) removeSlave(slave *slaveClient) {
	mdb.masterStatus.mu.Lock()
	defer mdb.masterStatus.mu.Unlock()
	_ = slave.conn.Close()
	delete(mdb.masterStatus.slaveMap, slave.conn)
	delete(mdb.masterStatus.waitSlaves, slave)
	delete(mdb.masterStatus.onlineSlaves, slave)
}

func (mdb *MultiDB) setSlaveOnline(slave *slaveClient, currentOffset int64) {
	mdb.masterStatus.mu.Lock()
	defer mdb.masterStatus.mu.Unlock()
	slave.state = slaveStateOnline
	slave.offset = currentOffset
	mdb.masterStatus.onlineSlaves[slave] = struct{}{}
}

var pingBytes = protocol.MakeMultiBulkReply(utils.ToCmdLine("ping")).ToBytes()

const maxBacklogSize = 10 * 1024 * 1024 // 10MB

func (mdb *MultiDB) masterCron() {
	if mdb.role != masterRole {
		return
	}
	mdb.masterStatus.mu.Lock()
	if mdb.masterStatus.bgSaveState == bgSaveFinish {
		mdb.masterStatus.backlog.appendBytes(pingBytes)
	}
	backlogSize := len(mdb.masterStatus.backlog.buf)
	mdb.masterStatus.mu.Unlock()
	if err := mdb.masterSendUpdatesToSlave(); err != nil {
		logger.Errorf("masterSendUpdatesToSlave error: %v", err)
	}
	if backlogSize > maxBacklogSize && !mdb.masterStatus.rewriting.Get() {
		go func() {
			mdb.masterStatus.rewriting.Set(true)
			defer mdb.masterStatus.rewriting.Set(false)
			if err := mdb.rewriteRDB(); err != nil {
				mdb.masterStatus.rewriting.Set(false)
				logger.Errorf("rewrite error: %v", err)
			}
		}()
	}
}

type replAofListener struct {
	mdb         *MultiDB
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

func (mdb *MultiDB) startAsMaster() {
	mdb.masterStatus = &masterStatus{
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
