package cluster

import (
	"github.com/hdt3213/godis/src/interface/redis"
	"github.com/hdt3213/godis/src/redis/reply"
	"strconv"
)

func Del(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'del' command")
	}
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = string(args[i])
	}
	groupMap := cluster.groupBy(keys)
	if len(groupMap) == 1 { // do fast
		for peer, group := range groupMap { // only one group
			return cluster.Relay(peer, c, makeArgs("DEL", group...))
		}
	}
	// prepare
	var errReply redis.Reply
	txId := cluster.idGenerator.NextId()
	txIdStr := strconv.FormatInt(txId, 10)
	rollback := false
	for peer, group := range groupMap {
		args := []string{txIdStr}
		args = append(args, group...)
		var resp redis.Reply
		if peer == cluster.self {
			resp = PrepareDel(cluster, c, makeArgs("PrepareDel", args...))
		} else {
			resp = cluster.Relay(peer, c, makeArgs("PrepareDel", args...))
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
		RequestRollback(cluster, c, txId, groupMap)
	} else {
		// commit
		respList, errReply = RequestCommit(cluster, c, txId, groupMap)
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
func PrepareDel(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'preparedel' command")
	}
	txId := string(args[1])
	keys := make([]string, 0, len(args)-2)
	for i := 2; i < len(args); i++ {
		arg := args[i]
		keys = append(keys, string(arg))
	}
	txArgs := makeArgs("DEL", keys...) // actual args for cluster.db
	tx := NewTransaction(cluster, c, txId, txArgs, keys)
	cluster.transactions.Put(txId, tx)
	err := tx.prepare()
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	return &reply.OkReply{}
}

// invoker should provide lock
func CommitDel(cluster *Cluster, c redis.Connection, tx *Transaction) redis.Reply {
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
