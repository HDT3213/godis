package cluster

import (
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/reply"
	"strconv"
)

// Del atomically removes given keys from cluster, keys can be distributed on any node
// if the given keys are distributed on different node, Del will use try-commit-catch to remove them
func Del(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'del' command")
	}
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = string(args[i])
	}
	groupMap := cluster.groupBy(keys)
	if len(groupMap) == 1 && allowFastTransaction { // do fast
		for peer, group := range groupMap { // only one group
			return cluster.relay(peer, c, makeArgs("DEL", group...))
		}
	}
	// prepare
	var errReply redis.Reply
	txID := cluster.idGenerator.NextID()
	txIDStr := strconv.FormatInt(txID, 10)
	rollback := false
	for peer, group := range groupMap {
		args := []string{txIDStr}
		args = append(args, group...)
		var resp redis.Reply
		if peer == cluster.self {
			resp = prepareDel(cluster, c, makeArgs("PrepareDel", args...))
		} else {
			resp = cluster.relay(peer, c, makeArgs("PrepareDel", args...))
		}
		if reply.IsErrorReply(resp) {
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
			intResp := resp.(*reply.IntReply)
			deleted += intResp.Code
		}
		return reply.MakeIntReply(int64(deleted))
	}
	return errReply
}

// args: PrepareDel id keys...
func prepareDel(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'preparedel' command")
	}
	txID := string(args[1])
	keys := make([]string, 0, len(args)-2)
	for i := 2; i < len(args); i++ {
		arg := args[i]
		keys = append(keys, string(arg))
	}
	txArgs := makeArgs("DEL", keys...) // actual args for cluster.db
	tx := NewTransaction(cluster, c, txID, txArgs, keys)
	cluster.transactions.Put(txID, tx)
	err := tx.prepare()
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	return &reply.OkReply{}
}

// invoker should provide lock
func commitDel(cluster *Cluster, c redis.Connection, tx *Transaction) redis.Reply {
	keys := make([]string, len(tx.args))
	for i, v := range tx.args {
		keys[i] = string(v)
	}
	keys = keys[1:]

	deleted := cluster.db.Removes(keys...)
	if deleted > 0 {
		cluster.db.AddAof(reply.MakeMultiBulkReply(tx.args))
	}
	return reply.MakeIntReply(int64(deleted))
}
