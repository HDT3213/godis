package cluster

import (
	"github.com/hdt3213/godis"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/reply"
)

const relayMulti = "_multi"

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
		wKeys, rKeys := cluster.db.GetRelatedKeys(cl)
		keys = append(keys, wKeys...)
		keys = append(keys, rKeys...)
	}
	if len(keys) == 0 {
		// empty transaction or only `PING`s
		return godis.ExecMulti(cluster.db, cmdLines)
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
		return godis.ExecMulti(cluster.db, cmdLines)
	}
	return execMultiOnOtherNode(cluster, conn, peer, cmdLines)
}

func execMultiOnOtherNode(cluster *Cluster, conn redis.Connection, peer string, cmdLines []CmdLine) redis.Reply {
	defer func() {
		conn.ClearQueuedCmds()
		conn.SetMultiState(false)
	}()
	relayCmdLine := [][]byte{ // relay it to executing node
		relayMultiBytes,
	}
	relayCmdLine = append(relayCmdLine, encodeCmdLine(cmdLines)...)
	rawRelayResult := cluster.relay(peer, conn, relayCmdLine)
	if reply.IsErrorReply(rawRelayResult) {
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
// cmdLine format: _multi base64ed-cmdLine
// result format: base64ed-reply list
func execRelayedMulti(cluster *Cluster, conn redis.Connection, cmdLine CmdLine) redis.Reply {
	decoded, err := parseEncodedMultiRawReply(cmdLine[1:])
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	var cmdLines []CmdLine
	for _, rep := range decoded.Replies {
		mbr, ok := rep.(*reply.MultiBulkReply)
		if !ok {
			return reply.MakeErrReply("exec failed")
		}
		cmdLines = append(cmdLines, mbr.Args)
	}
	rawResult := godis.ExecMulti(cluster.db, cmdLines)
	resultMBR, ok := rawResult.(*reply.MultiRawReply)
	if !ok {
		return reply.MakeErrReply("exec failed")
	}
	return encodeMultiRawReply(resultMBR)
}
