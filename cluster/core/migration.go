package core

import (
	"fmt"
	"strconv"
	"time"

	"github.com/hdt3213/godis/aof"
	"github.com/hdt3213/godis/cluster/raft"
	"github.com/hdt3213/godis/datastruct/set"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
)

const exportCommand = "cluster.migration.export"
const migrationDoneCommand = "cluster.migration.done"
const startMigrationCommand = "cluster.migration.start"

func init() {
	RegisterCmd(exportCommand, execExport)
	RegisterCmd(migrationDoneCommand, execFinishExport)
	RegisterCmd(startMigrationCommand, execStartMigration)
}

func (sm *slotStatus) startExporting() protocol.ErrorReply {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.state != slotStateHosting {
		return protocol.MakeErrReply("slot host is not hosting")
	}
	sm.state = slotStateExporting
	sm.dirtyKeys = set.Make()
	sm.exportSnapshot = sm.keys.ShallowCopy()
	return nil
}

func (sm *slotStatus) finishExportingWithinLock() {
	sm.state = slotStateHosting
	sm.dirtyKeys = nil
	sm.exportSnapshot = nil
}

func (cluster *Cluster) injectInsertCallback() {
	cb := func(dbIndex int, key string, entity *database.DataEntity) {
		slotIndex := GetSlot(key)
		slotManager := cluster.slotsManager.getSlot(slotIndex)
		slotManager.mu.Lock()
		defer slotManager.mu.Unlock()
		slotManager.keys.Add(key)
		if slotManager.state == slotStateExporting {
			slotManager.dirtyKeys.Add(key)
		}
	}
	cluster.db.SetKeyInsertedCallback(cb)
}

func (cluster *Cluster) injectDeleteCallback() {
	cb := func(dbIndex int, key string, entity *database.DataEntity) {
		slotIndex := GetSlot(key)
		slotManager := cluster.slotsManager.getSlot(slotIndex)
		slotManager.mu.Lock()
		defer slotManager.mu.Unlock()
		slotManager.keys.Remove(key)
		if slotManager.state == slotStateExporting {
			// 可能数据迁移后再进行了一次 delete, 所以这也是一个 dirty key
			slotManager.dirtyKeys.Add(key)
		}
	}
	cluster.db.SetKeyDeletedCallback(cb)
}

func (cluster *Cluster) dumpDataThroughConnection(c redis.Connection, keyset *set.Set) {
	keyset.ForEach(func(key string) bool {
		entity, ok := cluster.db.GetEntity(0, key)
		if ok {
			ret := aof.EntityToCmd(key, entity)
			// todo: handle error and close connection
			_, _ = c.Write(ret.ToBytes())
			expire := cluster.db.GetExpiration(0, key)
			if expire != nil {
				ret = aof.MakeExpireCmd(key, *expire)
				_, _ = c.Write(ret.ToBytes())
			}
		}
		return true
	})
}

// execExport dump slots data to caller
// command line: cluster.export taskId
func execExport(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeArgNumErrReply(exportCommand)
	}

	var task *raft.MigratingTask
	taskId := string(cmdLine[1])
	for i := 0; i < 50; i++ {
		task = cluster.raftNode.FSM.GetMigratingTask(taskId)
		if task == nil {
			time.Sleep(time.Millisecond * 100)
		}
	}
	if task == nil {
		return protocol.MakeErrReply("ERR get migrating task timeout")
	}
	cluster.slotsManager.mu.Lock()
	if cluster.slotsManager.importingTask != nil {
		cluster.slotsManager.mu.Unlock()
		return protocol.MakeErrReply("ERR another migrating task in progress")
	}
	cluster.slotsManager.importingTask = task
	cluster.slotsManager.mu.Unlock()

	for _, slotId := range task.Slots {
		slotManager := cluster.slotsManager.getSlot(slotId)
		errReply := slotManager.startExporting()
		if errReply != nil {
			return errReply
		}
		cluster.dumpDataThroughConnection(c, slotManager.exportSnapshot)
		logger.Info("finish dump slot ", slotId)
		// send a ok reply to tell requesting node dump finished
	}
	c.Write(protocol.MakeOkReply().ToBytes())
	return &protocol.NoReply{}
}

// execFinishExport
// command line: migrationDoneCommand taskId
// firstly dump dirty data from connection `c`, then returns an OK response
func execFinishExport(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeArgNumErrReply(exportCommand)
	}
	// get MigratingTask from raft
	var task *raft.MigratingTask
	taskId := string(cmdLine[1])
	logger.Info("finishing migration task: " + taskId)
	for i := 0; i < 50; i++ {
		task = cluster.raftNode.FSM.GetMigratingTask(taskId)
		if task == nil {
			time.Sleep(time.Millisecond * 100)
		}
	}
	if task == nil {
		return protocol.MakeErrReply("ERR get migrating task timeout")
	}
	logger.Infof("finishing migration task %s, got task info", taskId)

	// transport dirty keys within lock, lock will be released while migration done
	var lockedSlots []uint32
	defer func() {
		for i := len(lockedSlots) - 1; i >= 0; i-- {
			slotId := lockedSlots[i]
			slotManager := cluster.slotsManager.getSlot(slotId)
			slotManager.mu.Unlock()
		}
	}()
	for _, slotId := range task.Slots {
		slotManager := cluster.slotsManager.getSlot(slotId)
		slotManager.mu.Lock()
		lockedSlots = append(lockedSlots, slotId)
		cluster.dumpDataThroughConnection(c, slotManager.dirtyKeys)
		slotManager.finishExportingWithinLock()
	}
	logger.Infof("finishing migration task %s, dirty keys transported", taskId)

	// propose migrate finish
	leaderConn, err := cluster.BorrowLeaderClient()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	defer cluster.connections.ReturnPeerClient(leaderConn)
	reply := leaderConn.Send(utils.ToCmdLine(migrationChangeRouteCommand, taskId))
	switch reply := reply.(type) {
	case *protocol.StatusReply, *protocol.OkReply:
		return protocol.MakeOkReply()
	case *protocol.StandardErrReply:
		logger.Infof("migration done command failed: %v", reply.Error())
	default:
		logger.Infof("finish migration request unknown response %s", string(reply.ToBytes()))
	}
	logger.Infof("finishing migration task %s, route changed", taskId)

	c.Write(protocol.MakeOkReply().ToBytes())
	return &protocol.NoReply{}
}

// execStartMigration receives startMigrationCommand from leader and start migration job at background
// command line: startMigrationCommand taskId srcNode slotId1 [slotId2]...
func execStartMigration(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 4 {
		return protocol.MakeArgNumErrReply(startMigrationCommand)
	}
	taskId := string(cmdLine[1])
	srcNode := string(cmdLine[2])
	var slotIds []uint32
	for _, slotIdStr := range cmdLine[3:] {
		slotId, err := strconv.Atoi(string(slotIdStr))
		if err != nil {
			return protocol.MakeErrReply("illegal slot id: " + string(slotIdStr))
		}
		slotIds = append(slotIds, uint32(slotId))
	}
	task := &raft.MigratingTask{
		ID:         taskId,
		SrcNode:    srcNode,
		TargetNode: cluster.SelfID(),
		Slots:      slotIds,
	}
	cluster.slotsManager.mu.Lock()
	cluster.slotsManager.importingTask = task
	cluster.slotsManager.mu.Unlock()
	logger.Infof("received importing task %s,  %d slots to import", task.ID, len(task.Slots))
	go func() {
		defer func() {
			if e := recover(); e != nil {
				logger.Errorf("panic: %v", e)
			}
		}()
		cluster.doImports(task)
	}()
	return protocol.MakeOkReply()
}

func (cluster *Cluster) doImports(task *raft.MigratingTask) error {
	/// STEP1: export
	cmdLine := utils.ToCmdLine(exportCommand, task.ID)
	stream, err := cluster.connections.NewStream(task.SrcNode, cmdLine)
	if err != nil {
		return err
	}
	defer stream.Close()

	fakeConn := connection.NewFakeConn()

	// todo: import 状态的 slots 只接受 srcNode 的写入
recvLoop:
	for proto := range stream.Stream() {
		if proto.Err != nil {
			return fmt.Errorf("export error: %v", err)
		}
		switch reply := proto.Data.(type) {
		case *protocol.MultiBulkReply:
			_ = cluster.db.Exec(fakeConn, reply.Args)
		case *protocol.StatusReply, *protocol.OkReply:
			if protocol.IsOKReply(reply) {
				logger.Info("importing task received OK reply, phase 1 done")
				break recvLoop
			} else {
				msg := fmt.Sprintf("migrate error: %s", string(reply.ToBytes()))
				logger.Errorf(msg)
				return protocol.MakeErrReply(msg)
			}
		case protocol.ErrorReply:
			// todo: return slot to former host node
			msg := fmt.Sprintf("migrate error: %s", reply.Error())
			logger.Errorf(msg)
			return protocol.MakeErrReply(msg)
		}
	}

	///STEP3: 通知 srcNode 进入结束流程
	stream2, err := cluster.connections.NewStream(task.SrcNode, utils.ToCmdLine(migrationDoneCommand, task.ID))
	if err != nil {
		return err
	}
	defer stream2.Close()
	// receive dirty datas
recvLoop2:
	for proto := range stream2.Stream() {
		if proto.Err != nil {
			return fmt.Errorf("export error: %v", err)
		}
		switch reply := proto.Data.(type) {
		case *protocol.MultiBulkReply:
			_ = cluster.db.Exec(fakeConn, reply.Args)
		case *protocol.StatusReply, *protocol.OkReply:
			if protocol.IsOKReply(reply) {
				logger.Info("importing task received OK reply, phase 2 done")
				break recvLoop2
			} else {
				msg := fmt.Sprintf("migrate error: %s", string(reply.ToBytes()))
				logger.Errorf(msg)
				return protocol.MakeErrReply(msg)
			}
		case protocol.ErrorReply:
			// todo: return slot to former host node
			msg := fmt.Sprintf("migrate error: %s", reply.Error())
			logger.Errorf(msg)
			return protocol.MakeErrReply(msg)
		}
	}

	return nil
}
