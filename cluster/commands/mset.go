package commands

import (
	"github.com/hdt3213/godis/cluster/core"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

func init() {
	core.RegisterCmd("mset_", execMSetInLocal)
	core.RegisterCmd("mset", execMSet)

}

type CmdLine = [][]byte

// node -> keys on the node
type RouteMap map[string][]string

func getRouteMap(cluster *core.Cluster, keys []string) RouteMap {
	m := make(RouteMap)
	for _, key := range keys {
		slot := cluster.GetSlot(key)
		node := cluster.PickNode(slot)
		m[node] = append(m[node], key)
	}
	return m
}

// execMSetInLocal executes msets in local node
func execMSetInLocal(cluster *core.Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeArgNumErrReply("mset")
	}
	cmdLine[0] = []byte("mset")
	return cluster.LocalExec(c, cmdLine)
}

func execMSet(cluster *core.Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 3 || len(cmdLine)%2 != 1 {
		return protocol.MakeArgNumErrReply("mset")
	}
	var keys []string
	for i := 1; i < len(cmdLine); i += 2 {
		keys = append(keys, string(cmdLine[i]))
	}
	routeMap := getRouteMap(cluster, keys)
	if len(routeMap) == 1 {
		// only one node, do it fast
		for node := range routeMap {
			cmdLine[0] = []byte("mset_")
			return cluster.Relay(node, c, cmdLine)
		}
	}
	return execMSetSlow(cluster, c, cmdLine, routeMap)
}

func requestRollback(cluster *core.Cluster, c redis.Connection, txId string, routeMap RouteMap) {
	rollbackCmd := utils.ToCmdLine("rollback", txId)
	for node := range routeMap {
		cluster.Relay(node, c, rollbackCmd)
	}
}

// execMSetSlow execute mset through tcc
func execMSetSlow(cluster *core.Cluster, c redis.Connection, cmdLine CmdLine, routeMap RouteMap) redis.Reply {
	txId := utils.RandString(6)

	keyValues := make(map[string][]byte)
	for i := 1; i < len(cmdLine); i += 2 {
		key := string(cmdLine[i])
		value := cmdLine[i+1]
		keyValues[key] = value
	}

	// make prepare requests
	nodePrepareCmdMap := make(map[string]CmdLine)
	for node, keys := range routeMap {
		prepareCmd := utils.ToCmdLine("prepare", txId, "mset")
		for _, key := range keys {
			value := keyValues[key]
			prepareCmd = append(prepareCmd, []byte(key), value)
		}
		nodePrepareCmdMap[node] = prepareCmd
	}

	// send prepare request
	for node, prepareCmd := range nodePrepareCmdMap {
		reply := cluster.Relay(node, c, prepareCmd)
		if protocol.IsErrorReply(reply) {
			requestRollback(cluster, c, txId, routeMap)
			return protocol.MakeErrReply("prepare failed")
		}
	}

	// send commit request
	commiteCmd := utils.ToCmdLine("commit", txId)
	for node := range nodePrepareCmdMap {
		reply := cluster.Relay(node, c, commiteCmd)
		if protocol.IsErrorReply(reply) {
			requestRollback(cluster, c, txId, routeMap)
			return protocol.MakeErrReply("commit failed")
		}
	}

	return protocol.MakeOkReply()
}
