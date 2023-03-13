package cluster

import (
	"fmt"
	"github.com/hdt3213/godis/aof"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/redis/protocol"
	"strconv"
	"strings"
)

func init() {
	registerCmd("gcluster", execGCluster)
}

func execGCluster(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("gcluster")
	}
	subCmd := strings.ToLower(string(args[1]))
	switch subCmd {
	case "set-slot":
		// Command line: gcluster set-slot <slotID> <targetNodeID>
		// Other node request current node to migrate a slot to it.
		// Current node will set the slot as migrating state.
		// After this function return, all requests of target slot will be routed to target node
		return execGClusterSetSlot(cluster, c, args[2:])
	case "migrate":
		// Command line: gcluster migrate <slotId>
		// Current node will  dump the given slot to the node sending this request
		// The given slot must in migrating state
		return execGClusterMigrate(cluster, c, args[2:])
	case "migrate-done":
		// command line: gcluster migrate-done <slotId>
		// The new node hosting given slot tells current node that migration has finished, remains data can be deleted
		return execGClusterMigrateDone(cluster, c, args[2:])
	}
	return protocol.MakeErrReply(" ERR unknown gcluster sub command '" + subCmd + "'")
}

// execGClusterSetSlot set a current node hosted slot as migrating
// args is [slotID, newNodeId]
func execGClusterSetSlot(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeArgNumErrReply("gcluster")
	}
	slotId0, err := strconv.Atoi(string(args[0]))
	if err != nil || slotId0 >= slotCount {
		return protocol.MakeErrReply("ERR value is not a valid slot id")
	}
	slotId := uint32(slotId0)
	targetNodeID := string(args[1])
	_, ok := cluster.topology.GetTopology()[targetNodeID]
	if !ok {
		return protocol.MakeErrReply("ERR node not found")
	}
	cluster.setSlotMovingOut(slotId, targetNodeID)
	err = cluster.topology.SetSlot(slotId, targetNodeID)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	logger.Info(fmt.Sprintf("set slot %d to node %s", slotId, targetNodeID))
	return protocol.MakeOkReply()
}

// execGClusterMigrate Command line: gcluster migrate slotId
// Current node will  dump data in the given slot to the node sending this request
// The given slot must in migrating state
func execGClusterMigrate(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	slotId0, err := strconv.Atoi(string(args[0]))
	if err != nil || slotId0 >= slotCount {
		return protocol.MakeErrReply("ERR value is not a valid slot id")
	}
	slotId := uint32(slotId0)
	slot := cluster.getHostSlot(slotId)
	if slot == nil || slot.state != slotStateMovingOut {
		return protocol.MakeErrReply("ERR only dump migrating slot")
	}
	// migrating slot is immutable
	logger.Info("start dump slot", slotId)
	slot.keys.ForEach(func(key string) bool {
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
	logger.Info("finish dump slot ", slotId)
	// send a ok reply to tell requesting node dump finished
	return protocol.MakeOkReply()
}

// execGClusterMigrateDone command line: gcluster migrate-done <slotId>
func execGClusterMigrateDone(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	slotId0, err := strconv.Atoi(string(args[0]))
	if err != nil || slotId0 >= slotCount {
		return protocol.MakeErrReply("ERR value is not a valid slot id")
	}
	slotId := uint32(slotId0)
	slot := cluster.getHostSlot(slotId)
	if slot == nil || slot.state != slotStateMovingOut {
		return protocol.MakeErrReply("ERR slot is not moving out")
	}
	cluster.cleanDroppedSlot(slotId)
	cluster.slotMu.Lock()
	delete(cluster.slots, slotId)
	cluster.slotMu.Unlock()
	return protocol.MakeOkReply()
}
