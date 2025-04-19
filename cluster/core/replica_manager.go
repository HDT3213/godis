package core

import (
	"net"
	"sync"
	"time"

	"github.com/hdt3213/godis/cluster/raft"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
)

const heartbeatCommand = "cluster.heartbeat"

func init() {
	RegisterCmd(heartbeatCommand, execHeartbeat)
}

const (
	statusNormal  = iota
	statusFailing // failover in progress
)

type replicaManager struct {
	mu               sync.RWMutex
	masterHeartbeats map[string]time.Time // id -> lastHeartbeatTime
}

func newReplicaManager() *replicaManager {
	return &replicaManager{
		masterHeartbeats: make(map[string]time.Time),
	}
}

// execHeartbeat receives heartbeat from follower as raft leader
// cmdLine: cluster.heartbeat nodeId
func execHeartbeat(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeArgNumErrReply(heartbeatCommand)
	}
	id := string(cmdLine[1])
	cluster.replicaManager.mu.Lock()
	cluster.replicaManager.masterHeartbeats[id] = time.Now()
	cluster.replicaManager.mu.Unlock()

	return protocol.MakeOkReply()
}

func (cluster *Cluster) sendHearbeat() {
	leaderConn, err := cluster.BorrowLeaderClient()
	if err != nil {
		logger.Error(err)
	}
	defer cluster.connections.ReturnPeerClient(leaderConn)
	reply := leaderConn.Send(utils.ToCmdLine(heartbeatCommand, cluster.SelfID()))
	if err := protocol.Try2ErrorReply(reply); err != nil {
		logger.Error(err)
	}
}

const followerTimeout = 10 * time.Second

func (cluster *Cluster) doFailoverCheck() {
	// find timeout masters
	var timeoutMasters []*raft.MasterSlave
	ddl := time.Now().Add(-followerTimeout)
	cluster.replicaManager.mu.RLock()
	for masterId, lastTime := range cluster.replicaManager.masterHeartbeats {
		if lastTime.Second() == 0 {
			// do not set new joined node as timeout
			cluster.replicaManager.masterHeartbeats[masterId] = time.Now()
		}
		if lastTime.Before(ddl) {
			slaves := cluster.raftNode.GetSlaves(masterId)
			if slaves != nil && len(slaves.Slaves) > 0 {
				timeoutMasters = append(timeoutMasters, slaves)
			}

		}
	}
	cluster.replicaManager.mu.RUnlock()

	// trigger failover
	for _, failed := range timeoutMasters {
		cluster.triggerFailover(failed)
	}
}

func (cluster *Cluster) triggerFailover(failed *raft.MasterSlave) error {
	newMaster := failed.Slaves[0]
	id := utils.RandString(20)
	// propose change
	_, err := cluster.raftNode.Propose(&raft.LogEntry{
		Event: raft.EventStartFailover,
		FailoverTask: &raft.FailoverTask{
			ID:          id,
			OldMasterId: failed.MasterId,
			NewMasterId: newMaster,
		},
	})
	if err != nil {
		return err
	}
	logger.Infof("proposed start failover id=%s, oldMaster=%s, newMaster=%s", id, failed.MasterId, newMaster)
	// send slave of to new master
	conn, err := cluster.connections.BorrowPeerClient(newMaster)
	if err != nil {
		return err
	}
	defer cluster.connections.ReturnPeerClient(conn)

	reply := conn.Send(utils.ToCmdLine("slaveof", "no", "one"))
	if err := protocol.Try2ErrorReply(reply); err != nil {
		return err
	}

	// new master is ready to receive commands, change route
	_, err = cluster.raftNode.Propose(&raft.LogEntry{
		Event: raft.EventFinishFailover,
		FailoverTask: &raft.FailoverTask{
			ID:          id,
			OldMasterId: failed.MasterId,
			NewMasterId: newMaster,
		},
	})
	if err != nil {
		return err
	}
	logger.Infof("proposed finish failover id=%s, oldMaster=%s, newMaster=%s", id, failed.MasterId, newMaster)
	// other slaves will listen to raft to change master
	return nil
}

func (cluster *Cluster) registerOnFailover() {
	cluster.raftNode.SetOnFailover(func(newMaster string) {
		if newMaster != "" && newMaster != cluster.SelfID() {
			// old master failed and other node become the new master
			// this node may be the old master
			ip, port, err := net.SplitHostPort(newMaster)
			if err != nil {
				logger.Errorf("illegal new master: %s", newMaster)
				return
			}
			c := connection.NewFakeConn()
			ret := cluster.db.Exec(c, utils.ToCmdLine("slaveof", ip, port))
			if err := protocol.Try2ErrorReply(ret); err != nil {
				logger.Errorf("slave of failed: %v", err)
				return
			}
		}
	})
}
