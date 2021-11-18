package cluster

import (
	"github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/reply"
	"strconv"
)

const relayMulti = "_multi"
const innerWatch = "_watch"

var relayMultiBytes = []byte(relayMulti)

// cmdLine == []string{"exec"}
func execMulti(cluster *Cluster, conn redis.Connection, cmdLine CmdLine) redis.Reply {
	if !conn.InMultiState() {
		return reply.MakeErrReply("ERR EXEC without MULTI")
	}
	defer conn.SetMultiState(false)
	cmdLines := conn.GetQueuedCmdLine()

	// analysis related keys
	keys := make([]string, 0) // may contains duplicate
	for _, cl := range cmdLines {
		wKeys, rKeys := database.GetRelatedKeys(cl)
		keys = append(keys, wKeys...)
		keys = append(keys, rKeys...)
	}
	watching := conn.GetWatching()
	watchingKeys := make([]string, 0, len(watching))
	for key := range watching {
		watchingKeys = append(watchingKeys, key)
	}
	keys = append(keys, watchingKeys...)
	if len(keys) == 0 {
		// empty transaction or only `PING`s
		return cluster.db.ExecMulti(conn, watching, cmdLines)
	}
	groupMap := cluster.groupBy(keys)
	if len(groupMap) > 1 {
		return reply.MakeErrReply("ERR MULTI commands transaction must within one slot in cluster mode")
	}
	var peer string
	// assert len(groupMap) == 1
	for p := range groupMap {
		peer = p
	}

	// out parser not support reply.MultiRawReply, so we have to encode it
	if peer == cluster.self {
		return cluster.db.ExecMulti(conn, watching, cmdLines)
	}
	return execMultiOnOtherNode(cluster, conn, peer, watching, cmdLines)
}

func execMultiOnOtherNode(cluster *Cluster, conn redis.Connection, peer string, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	defer func() {
		conn.ClearQueuedCmds()
		conn.SetMultiState(false)
	}()
	relayCmdLine := [][]byte{ // relay it to executing node
		relayMultiBytes,
	}
	// watching commands
	var watchingCmdLine = utils.ToCmdLine(innerWatch)
	for key, ver := range watching {
		verStr := strconv.FormatUint(uint64(ver), 10)
		watchingCmdLine = append(watchingCmdLine, []byte(key), []byte(verStr))
	}
	relayCmdLine = append(relayCmdLine, encodeCmdLine([]CmdLine{watchingCmdLine})...)
	relayCmdLine = append(relayCmdLine, encodeCmdLine(cmdLines)...)
	var rawRelayResult redis.Reply
	if peer == cluster.self {
		// this branch just for testing
		rawRelayResult = execRelayedMulti(cluster, conn, relayCmdLine)
	} else {
		rawRelayResult = cluster.relay(peer, conn, relayCmdLine)
	}
	if reply.IsErrorReply(rawRelayResult) {
		return rawRelayResult
	}
	_, ok := rawRelayResult.(*reply.EmptyMultiBulkReply)
	if ok {
		return rawRelayResult
	}
	relayResult, ok := rawRelayResult.(*reply.MultiBulkReply)
	if !ok {
		return reply.MakeErrReply("execute failed")
	}
	rep, err := parseEncodedMultiRawReply(relayResult.Args)
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	return rep
}

// execRelayedMulti execute relayed multi commands transaction
// cmdLine format: _multi watch-cmdLine base64ed-cmdLine
// result format: base64ed-reply list
func execRelayedMulti(cluster *Cluster, conn redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 2 {
		return reply.MakeArgNumErrReply("_exec")
	}
	decoded, err := parseEncodedMultiRawReply(cmdLine[1:])
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	var txCmdLines []CmdLine
	for _, rep := range decoded.Replies {
		mbr, ok := rep.(*reply.MultiBulkReply)
		if !ok {
			return reply.MakeErrReply("exec failed")
		}
		txCmdLines = append(txCmdLines, mbr.Args)
	}
	watching := make(map[string]uint32)
	watchCmdLine := txCmdLines[0] // format: _watch key1 ver1 key2 ver2...
	for i := 2; i < len(watchCmdLine); i += 2 {
		key := string(watchCmdLine[i-1])
		verStr := string(watchCmdLine[i])
		ver, err := strconv.ParseUint(verStr, 10, 64)
		if err != nil {
			return reply.MakeErrReply("watching command line failed")
		}
		watching[key] = uint32(ver)
	}
	rawResult := cluster.db.ExecMulti(conn, watching, txCmdLines[1:])
	_, ok := rawResult.(*reply.EmptyMultiBulkReply)
	if ok {
		return rawResult
	}
	resultMBR, ok := rawResult.(*reply.MultiRawReply)
	if !ok {
		return reply.MakeErrReply("exec failed")
	}
	return encodeMultiRawReply(resultMBR)
}

func execWatch(cluster *Cluster, conn redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeArgNumErrReply("watch")
	}
	args = args[1:]
	watching := conn.GetWatching()
	for _, bkey := range args {
		key := string(bkey)
		peer := cluster.peerPicker.PickNode(key)
		result := cluster.relay(peer, conn, utils.ToCmdLine("GetVer", key))
		if reply.IsErrorReply(result) {
			return result
		}
		intResult, ok := result.(*reply.IntReply)
		if !ok {
			return reply.MakeErrReply("get version failed")
		}
		watching[key] = uint32(intResult.Code)
	}
	return reply.MakeOkReply()
}
