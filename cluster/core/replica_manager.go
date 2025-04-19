package core

import (
	"sync"
	"time"

	"github.com/hdt3213/godis/cluster/raft"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

const heartbeatCommand = "cluster.heartbeat"

func init() {
	RegisterCmd(heartbeatCommand, execHeartbeat)
}

const (
	statusNormal = iota
	statusFailing // failover in progress
)

type replicaManager struct {
	mu               sync.RWMutex
	masterHeartbeats map[string]time.Time    // id -> lastHeartbeatTime
}

func newReplicaManager() *replicaManager {
	return &replicaManager{
		masterHeartbeats: make(map[string]time.Time),
	}
}

// execHeartbeat receives heartbeat from follower
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

func (cluster *Cluster) heartbeatJob() {
	// todo: maybe a close chan is needed
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			if cluster.raftNode.State() == raft.Leader {
				cluster.doFailoverCheck()
			} else {
				cluster.sendHearbeat()
			}
		case <-cluster.closeChan:
			ticker.Stop()
			return
		}
	}
}

func (cluster *Cluster) sendHearbeat() {
	leaderConn, err := cluster.BorrowLeaderClient()
	if err != nil {
		logger.Error(err)
	}
	defer cluster.connections.ReturnPeerClient(leaderConn)
	reply := leaderConn.Send(utils.ToCmdLine(heartbeatCommand))
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
		if lastTime.Before(ddl) {
			slaves := cluster.raftNode.GetSlaves(masterId)
			if slaves != nil && len(slaves.Slaves) > 0 {
				timeoutMasters = append(timeoutMasters, slaves)
			}

		}
	}
	cluster.replicaManager.mu.RUnlock()

	// trigger failover
	
}
