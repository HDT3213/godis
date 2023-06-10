package cluster

import (
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
	"strconv"
	"strings"
)

const copyToAnotherDBErr = "ERR Copying to another database is not allowed in cluster mode"
const noReplace = "NoReplace"
const useReplace = "UseReplace"

// Copy copies the value stored at the source key to the destination key.
// the origin and the destination must within the same node.
func Copy(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'copy' command")
	}
	srcKey := string(args[1])
	destKey := string(args[2])
	srcNode := cluster.pickNodeAddrByKey(srcKey)
	destNode := cluster.pickNodeAddrByKey(destKey)
	replaceFlag := noReplace
	if len(args) > 3 {
		for i := 3; i < len(args); i++ {
			arg := strings.ToLower(string(args[i]))
			if arg == "db" {
				return protocol.MakeErrReply(copyToAnotherDBErr)
			} else if arg == "replace" {
				replaceFlag = useReplace
			} else {
				return protocol.MakeSyntaxErrReply()
			}
		}
	}

	if srcNode == destNode {
		args[0] = []byte("Copy_") // Copy_ will go directly to cluster.DB avoiding infinite recursion
		return cluster.relay(srcNode, c, args)
	}
	groupMap := map[string][]string{
		srcNode:  {srcKey},
		destNode: {destKey},
	}

	txID := cluster.idGenerator.NextID()
	txIDStr := strconv.FormatInt(txID, 10)
	// prepare Copy from
	srcPrepareResp := cluster.relay(srcNode, c, makeArgs("Prepare", txIDStr, "CopyFrom", srcKey))
	if protocol.IsErrorReply(srcPrepareResp) {
		// rollback src node
		requestRollback(cluster, c, txID, map[string][]string{srcNode: {srcKey}})
		return srcPrepareResp
	}
	srcPrepareMBR, ok := srcPrepareResp.(*protocol.MultiBulkReply)
	if !ok || len(srcPrepareMBR.Args) < 2 {
		requestRollback(cluster, c, txID, map[string][]string{srcNode: {srcKey}})
		return protocol.MakeErrReply("ERR invalid prepare response")
	}
	// prepare Copy to
	destPrepareResp := cluster.relay(destNode, c, utils.ToCmdLine3("Prepare", []byte(txIDStr),
		[]byte("CopyTo"), []byte(destKey), srcPrepareMBR.Args[0], srcPrepareMBR.Args[1], []byte(replaceFlag)))
	if destErr, ok := destPrepareResp.(protocol.ErrorReply); ok {
		// rollback src node
		requestRollback(cluster, c, txID, groupMap)
		if destErr.Error() == keyExistsErr {
			return protocol.MakeIntReply(0)
		}
		return destPrepareResp
	}
	if _, errReply := requestCommit(cluster, c, txID, groupMap); errReply != nil {
		requestRollback(cluster, c, txID, groupMap)
		return errReply
	}
	return protocol.MakeIntReply(1)
}

// prepareCopyFrom is prepare-function for CopyFrom, see prepareFuncMap
func prepareCopyFrom(cluster *Cluster, conn redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeArgNumErrReply("CopyFrom")
	}
	key := string(cmdLine[1])
	existResp := cluster.db.ExecWithLock(conn, utils.ToCmdLine("Exists", key))
	if protocol.IsErrorReply(existResp) {
		return existResp
	}
	existIntResp := existResp.(*protocol.IntReply)
	if existIntResp.Code == 0 {
		return protocol.MakeErrReply("ERR no such key")
	}
	return cluster.db.ExecWithLock(conn, utils.ToCmdLine2("DumpKey", key))
}

func prepareCopyTo(cluster *Cluster, conn redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 5 {
		return protocol.MakeArgNumErrReply("CopyTo")
	}
	key := string(cmdLine[1])
	replaceFlag := string(cmdLine[4])
	existResp := cluster.db.ExecWithLock(conn, utils.ToCmdLine("Exists", key))
	if protocol.IsErrorReply(existResp) {
		return existResp
	}
	existIntResp := existResp.(*protocol.IntReply)
	if existIntResp.Code == 1 {
		if replaceFlag == noReplace {
			return protocol.MakeErrReply(keyExistsErr)
		}
	}
	return protocol.MakeOkReply()
}

func init() {
	registerPrepareFunc("CopyFrom", prepareCopyFrom)
	registerPrepareFunc("CopyTo", prepareCopyTo)
}
