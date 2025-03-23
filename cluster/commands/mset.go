package commands

import (
	"strings"

	"github.com/hdt3213/godis/cluster/core"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

func init() {
	core.RegisterCmd("mset_", execMSetInLocal)
	core.RegisterCmd("mset", execMSet)
	core.RegisterCmd("mget_", execMGetInLocal)
	core.RegisterCmd("mget", execMGet)
	core.RegisterCmd("msetnx_", execMSetNxInLocal)
	core.RegisterCmd("msetnx", execMSet)
}

// execMSetInLocal executes msets in local node
func execMSetInLocal(cluster *core.Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeArgNumErrReply("mset")
	}
	cmdLine[0] = []byte("mset")
	return cluster.LocalExec(c, cmdLine)
}

// execMSetInLocal executes msets in local node
func execMGetInLocal(cluster *core.Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 2 {
		return protocol.MakeArgNumErrReply("mget")
	}
	cmdLine[0] = []byte("mget")
	return cluster.LocalExec(c, cmdLine)
}

// execMSetInLocal executes msets in local node
func execMSetNxInLocal(cluster *core.Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeArgNumErrReply("msetnx")
	}
	cmdLine[0] = []byte("msetnx")
	return cluster.LocalExec(c, cmdLine)
}

func execMSet(cluster *core.Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 3 || len(cmdLine)%2 != 1 {
		return protocol.MakeArgNumErrReply("mset")
	}
	var keys []string
	keyValues := make(map[string][]byte)
	for i := 1; i < len(cmdLine); i += 2 {
		key := string(cmdLine[i])
		value := cmdLine[i+1]
		keyValues[key] = value
		keys = append(keys, key)
	}
	routeMap := getRouteMap(cluster, keys)
	if len(routeMap) == 1 {
		// only one node, do it fast
		for node := range routeMap {
			cmdLine[0] = []byte("mset_")
			return cluster.Relay(node, c, cmdLine)
		}
	}

	// tcc
	cmdLineMap := make(map[string]CmdLine)
	for node, keys := range routeMap {
		nodeCmdLine := utils.ToCmdLine("mset")
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
	_, err := doTcc(cluster, c, tx)
	if err != nil {
		return err
	}
	return protocol.MakeOkReply()
}

func execMGet(cluster *core.Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 2 {
		return protocol.MakeArgNumErrReply("mget")
	}
	keys := make([]string, 0, len(cmdLine)-1)
	for i := 1; i < len(cmdLine); i++ {
		keys = append(keys, string(cmdLine[i]))
	}
	routeMap := getRouteMap(cluster, keys)
	if len(routeMap) == 1 {
		// only one node, do it fast
		for node := range routeMap {
			cmdLine[0] = []byte("mget_")
			return cluster.Relay(node, c, cmdLine)
		}
	}

	// tcc
	cmdLineMap := make(map[string]CmdLine)
	for node, keys := range routeMap {
		cmdLineMap[node] = utils.ToCmdLine2("mget", keys...)
	}
	tx := &TccTx{
		rawCmdLine: cmdLine,
		routeMap:   routeMap,
		cmdLines:   cmdLineMap,
	}
	nodeResults, err := doTcc(cluster, c, tx)
	if err != nil {
		return err
	}
	keyValues := make(map[string][]byte, len(cmdLine)-1)
	for node, ret := range nodeResults {
		nodeCmdLine := cmdLineMap[node]
		result := ret.(*protocol.MultiBulkReply)
		if len(result.Args) != len(nodeCmdLine) - 1 {
			return protocol.MakeErrReply("wrong response from node " + node)
		}
		for i := 1; i < len(nodeCmdLine); i++ {
			key := string(nodeCmdLine[i])
			value := result.Args[i-1]
			keyValues[key] = value
		}
	}
	result := make([][]byte, 0, len(cmdLine)-1)
	for i := 1; i < len(cmdLine); i++ {
		value := keyValues[string(cmdLine[i])]
		result = append(result, value)
	}
	return protocol.MakeMultiBulkReply(result)
}

const someKeysExistsErr = "Some Keys Exists"

func init() {
	core.RegisterPreCheck("msetnx", msetNxPrecheck)
}

func msetNxPrecheck(cluster *core.Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	var keys []string
	for i := 1; i < len(cmdLine); i+=2 {
		keys = append(keys, string(cmdLine[i]))
	}
	exists := cluster.LocalExists(keys)
	if len(exists) > 0 {
		return protocol.MakeErrReply(someKeysExistsErr)
	}
	return protocol.MakeOkReply()
}

func execMSetNx(cluster *core.Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 3 || len(cmdLine)%2 != 1 {
		return protocol.MakeArgNumErrReply("mset")
	}
	var keys []string
	keyValues := make(map[string][]byte)
	for i := 1; i < len(cmdLine); i += 2 {
		key := string(cmdLine[i])
		value := cmdLine[i+1]
		keyValues[key] = value
		keys = append(keys, key)
	}
	routeMap := getRouteMap(cluster, keys)
	if len(routeMap) == 1 {
		// only one node, do it fast
		for node := range routeMap {
			cmdLine[0] = []byte("msetnx_")
			return cluster.Relay(node, c, cmdLine)
		}
	}

	// tcc
	cmdLineMap := make(map[string]CmdLine)
	for node, keys := range routeMap {
		nodeCmdLine := utils.ToCmdLine("msetnx")
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
	_, err := doTcc(cluster, c, tx)
	if err != nil {
		if strings.Contains(err.Error(), someKeysExistsErr) {
			return protocol.MakeIntReply(0)
		}
		return err
	}
	return protocol.MakeIntReply(1)
}