package cluster

import (
	"fmt"
	"github.com/hdt3213/godis"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/reply"
	"strconv"
)

func MGet(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'mget' command")
	}
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = string(args[i])
	}

	resultMap := make(map[string][]byte)
	groupMap := cluster.groupBy(keys)
	for peer, group := range groupMap {
		resp := cluster.Relay(peer, c, makeArgs("MGET", group...))
		if reply.IsErrorReply(resp) {
			errReply := resp.(reply.ErrorReply)
			return reply.MakeErrReply(fmt.Sprintf("ERR during get %s occurs: %v", group[0], errReply.Error()))
		}
		arrReply, _ := resp.(*reply.MultiBulkReply)
		for i, v := range arrReply.Args {
			key := group[i]
			resultMap[key] = v
		}
	}
	result := make([][]byte, len(keys))
	for i, k := range keys {
		result[i] = resultMap[k]
	}
	return reply.MakeMultiBulkReply(result)
}

// args: PrepareMSet id keys...
func PrepareMSet(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'preparemset' command")
	}
	txId := string(args[1])
	size := (len(args) - 2) / 2
	keys := make([]string, size)
	for i := 0; i < size; i++ {
		keys[i] = string(args[2*i+2])
	}

	txArgs := [][]byte{
		[]byte("MSet"),
	} // actual args for cluster.db
	txArgs = append(txArgs, args[2:]...)
	tx := NewTransaction(cluster, c, txId, txArgs, keys)
	cluster.transactions.Put(txId, tx)
	err := tx.prepare()
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	return &reply.OkReply{}
}

// invoker should provide lock
func CommitMSet(cluster *Cluster, c redis.Connection, tx *Transaction) redis.Reply {
	size := len(tx.args) / 2
	keys := make([]string, size)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		keys[i] = string(tx.args[2*i+1])
		values[i] = tx.args[2*i+2]
	}
	for i, key := range keys {
		value := values[i]
		cluster.db.PutEntity(key, &godis.DataEntity{Data: value})
	}
	cluster.db.AddAof(reply.MakeMultiBulkReply(tx.args))
	return &reply.OkReply{}
}

func MSet(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	argCount := len(args) - 1
	if argCount%2 != 0 || argCount < 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'mset' command")
	}

	size := argCount / 2
	keys := make([]string, size)
	valueMap := make(map[string]string)
	for i := 0; i < size; i++ {
		keys[i] = string(args[2*i+1])
		valueMap[keys[i]] = string(args[2*i+2])
	}

	groupMap := cluster.groupBy(keys)
	if len(groupMap) == 1 && allowFastTransaction { // do fast
		for peer := range groupMap {
			return cluster.Relay(peer, c, args)
		}
	}

	//prepare
	var errReply redis.Reply
	txId := cluster.idGenerator.NextId()
	txIdStr := strconv.FormatInt(txId, 10)
	rollback := false
	for peer, group := range groupMap {
		peerArgs := []string{txIdStr}
		for _, k := range group {
			peerArgs = append(peerArgs, k, valueMap[k])
		}
		var resp redis.Reply
		if peer == cluster.self {
			resp = PrepareMSet(cluster, c, makeArgs("PrepareMSet", peerArgs...))
		} else {
			resp = cluster.Relay(peer, c, makeArgs("PrepareMSet", peerArgs...))
		}
		if reply.IsErrorReply(resp) {
			errReply = resp
			rollback = true
			break
		}
	}
	if rollback {
		// rollback
		RequestRollback(cluster, c, txId, groupMap)
	} else {
		_, errReply = RequestCommit(cluster, c, txId, groupMap)
		rollback = errReply != nil
	}
	if !rollback {
		return &reply.OkReply{}
	}
	return errReply

}

func MSetNX(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	argCount := len(args) - 1
	if argCount%2 != 0 || argCount < 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'mset' command")
	}
	var peer string
	size := argCount / 2
	for i := 0; i < size; i++ {
		key := string(args[2*i])
		currentPeer := cluster.peerPicker.PickNode(key)
		if peer == "" {
			peer = currentPeer
		} else {
			if peer != currentPeer {
				return reply.MakeErrReply("ERR msetnx must within one slot in cluster mode")
			}
		}
	}
	return cluster.Relay(peer, c, args)
}
