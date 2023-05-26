package cluster

import (
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
	"strconv"
)

// Del atomically removes given writeKeys from cluster, writeKeys can be distributed on any node
// if the given writeKeys are distributed on different node, Del will use try-commit-catch to remove them
func Del(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'del' command")
	}
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = string(args[i])
	}
	groupMap := cluster.groupBy(keys)
	if len(groupMap) == 1 && allowFastTransaction { // do fast
		for peer, group := range groupMap { // only one peerKeys
			return cluster.relay(peer, c, makeArgs("Del_", group...))
		}
	}
	// prepare
	var errReply redis.Reply
	txID := cluster.idGenerator.NextID()
	txIDStr := strconv.FormatInt(txID, 10)
	rollback := false
	for peer, peerKeys := range groupMap {
		peerArgs := []string{txIDStr, "DEL"}
		peerArgs = append(peerArgs, peerKeys...)
		var resp redis.Reply
		resp = cluster.relay(peer, c, makeArgs("Prepare", peerArgs...))
		if protocol.IsErrorReply(resp) {
			errReply = resp
			rollback = true
			break
		}
	}
	var respList []redis.Reply
	if rollback {
		// rollback
		requestRollback(cluster, c, txID, groupMap)
	} else {
		// commit
		respList, errReply = requestCommit(cluster, c, txID, groupMap)
		if errReply != nil {
			rollback = true
		}
	}
	if !rollback {
		var deleted int64 = 0
		for _, resp := range respList {
			intResp := resp.(*protocol.IntReply)
			deleted += intResp.Code
		}
		return protocol.MakeIntReply(int64(deleted))
	}
	return errReply
}
