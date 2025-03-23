package commands

import (
	"github.com/hdt3213/godis/cluster/core"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

func init() {
	core.RegisterCmd("del_", execDelInLocal)
	core.RegisterCmd("del", execDel)
}

func execDelInLocal(cluster *core.Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 2 {
		return protocol.MakeArgNumErrReply("del")
	}
	cmdLine[0] = []byte("del")
	return cluster.LocalExec(c, cmdLine)
}

func execDel(cluster *core.Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 2 {
		return protocol.MakeArgNumErrReply("del")
	}
	var keys []string
	keyValues := make(map[string][]byte)
	for i := 1; i < len(cmdLine); i++ {
		key := string(cmdLine[i])
		keys = append(keys, key)
	}
	routeMap := getRouteMap(cluster, keys)
	if len(routeMap) == 1 {
		// only one node, do it fast
		for node := range routeMap {
			cmdLine[0] = []byte("del_")
			return cluster.Relay(node, c, cmdLine)
		}
	}

	// tcc
	cmdLineMap := make(map[string]CmdLine)
	for node, keys := range routeMap {
		nodeCmdLine := utils.ToCmdLine("del")
		for _, key := range keys {
			val := keyValues[key]
			nodeCmdLine = append(nodeCmdLine, []byte(key), val)
		}
		cmdLineMap[node] = nodeCmdLine
	}
	tx := &TccTx{
		rawCmdLine: cmdLine,
		routeMap:   routeMap,
		cmdLines:   cmdLineMap,
	}
	results, err := doTcc(cluster, c, tx)
	if err != nil {
		return err
	}
	var count int64 = 0
	for _, res := range results {
		res2, ok := res.(*protocol.IntReply)
		if ok {
			count += res2.Code
		}
	}
	return protocol.MakeIntReply(count)
}
