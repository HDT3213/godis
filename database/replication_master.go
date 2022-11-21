package database

import (
	"context"
	"errors"
	"fmt"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
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

type masterStatus struct {
	ctx           context.Context
	mu            sync.RWMutex
	replId        string
	backlog       []byte // backlog can be appended or replaced as a whole, cannot be modified(insert/set/delete)
	beginOffset   int64
	currentOffset int64
	slaveMap      map[redis.Connection]*slaveClient
	waitSlaves    map[*slaveClient]struct{}
	onlineSlaves  map[*slaveClient]struct{}
	bgSaveState   uint8
	rdbFilename   string
}

func (master *masterStatus) appendBacklog(bin []byte) {
	master.backlog = append(master.backlog, bin...)
	master.currentOffset += int64(len(bin))
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
	mdb.masterStatus.mu.Unlock()

	aofListener := make(chan CmdLine, 1024) // give channel enough capacity to store all updates during rewrite to db
	err = mdb.aofHandler.Rewrite2RDB(rdbFilename, aofListener)
	if err != nil {
		return err
	}
	go func() {
		mdb.masterListenAof(aofListener)
	}()

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

// masterFullReSyncWithSlave send replication header, rdb file and all backlogs to slave
func (mdb *MultiDB) masterFullReSyncWithSlave(slave *slaveClient) error {
	// write replication header
	header := "+FULLRESYNC " + mdb.masterStatus.replId + " " +
		strconv.FormatInt(mdb.masterStatus.beginOffset, 10) + protocol.CRLF
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
	currentOffset := mdb.masterStatus.currentOffset
	backlog := mdb.masterStatus.backlog[:currentOffset-mdb.masterStatus.beginOffset]
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
	if slaveOffset < mdb.masterStatus.beginOffset || slaveOffset > mdb.masterStatus.currentOffset {
		mdb.masterStatus.mu.RUnlock()
		return cannotPartialSync
	}
	currentOffset := mdb.masterStatus.currentOffset
	backlog := mdb.masterStatus.backlog[slaveOffset-mdb.masterStatus.beginOffset : currentOffset-mdb.masterStatus.beginOffset]
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

func (mdb *MultiDB) masterSendUpdatesToSlave() error {
	onlineSlaves := make(map[*slaveClient]struct{})
	mdb.masterStatus.mu.RLock()
	currentOffset := mdb.masterStatus.currentOffset
	beginOffset := mdb.masterStatus.beginOffset
	backlog := mdb.masterStatus.backlog[:currentOffset-beginOffset]
	for slave := range mdb.masterStatus.onlineSlaves {
		onlineSlaves[slave] = struct{}{}
	}
	mdb.masterStatus.mu.RUnlock()
	for slave := range onlineSlaves {
		slaveBeginOffset := slave.offset - beginOffset
		_, err := slave.conn.Write(backlog[slaveBeginOffset:])
		if err != nil {
			logger.Errorf("send updates write backlog to slave failed: %v", err)
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

func (mdb *MultiDB) masterCron() {
	if mdb.role != masterRole {
		return
	}
	mdb.masterStatus.mu.Lock()
	if mdb.masterStatus.bgSaveState == bgSaveFinish {
		mdb.masterStatus.appendBacklog(pingBytes)
	}
	mdb.masterStatus.mu.Unlock()
	if err := mdb.masterSendUpdatesToSlave(); err != nil {
		logger.Errorf("masterSendUpdatesToSlave error: %v", err)
	}
}

func (mdb *MultiDB) masterListenAof(listener chan CmdLine) {
	for {
		select {
		case cmdLine := <-listener:
			mdb.masterStatus.mu.Lock()
			reply := protocol.MakeMultiBulkReply(cmdLine)
			mdb.masterStatus.appendBacklog(reply.ToBytes())
			mdb.masterStatus.mu.Unlock()
			if err := mdb.masterSendUpdatesToSlave(); err != nil {
				logger.Errorf("masterSendUpdatesToSlave after receive aof error: %v", err)
			}
			// if bgSave is running, updates will be sent after the save finished
		case <-mdb.masterStatus.ctx.Done():
			break
		}
	}
}

func (mdb *MultiDB) startAsMaster() {
	mdb.masterStatus = &masterStatus{
		ctx:           context.Background(),
		mu:            sync.RWMutex{},
		replId:        utils.RandHexString(40),
		backlog:       nil,
		beginOffset:   0,
		currentOffset: 0,
		slaveMap:      make(map[redis.Connection]*slaveClient),
		waitSlaves:    make(map[*slaveClient]struct{}),
		onlineSlaves:  make(map[*slaveClient]struct{}),
		bgSaveState:   bgSaveIdle,
		rdbFilename:   "",
	}
}
