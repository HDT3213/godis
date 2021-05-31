package cluster

import (
	"fmt"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/reply"
	"strconv"
)

// MGet atomically get multi key-value from cluster, writeKeys can be distributed on any node
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
		resp := cluster.relay(peer, c, makeArgs("MGET", group...))
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

// MSet atomically sets multi key-value in cluster, writeKeys can be distributed on any node
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
			return cluster.relay(peer, c, args)
		}
	}

	//prepare
	var errReply redis.Reply
	txID := cluster.idGenerator.NextID()
	txIDStr := strconv.FormatInt(txID, 10)
	rollback := false
	for peer, group := range groupMap {
		peerArgs := []string{txIDStr, "MSET"}
		for _, k := range group {
			peerArgs = append(peerArgs, k, valueMap[k])
		}
		var resp redis.Reply
		if peer == cluster.self {
			resp = execPrepare(cluster, c, makeArgs("Prepare", peerArgs...))
		} else {
			resp = cluster.relay(peer, c, makeArgs("Prepare", peerArgs...))
		}
		if reply.IsErrorReply(resp) {
			errReply = resp
			rollback = true
			break
		}
	}
	if rollback {
		// rollback
		requestRollback(cluster, c, txID, groupMap)
	} else {
		_, errReply = requestCommit(cluster, c, txID, groupMap)
		rollback = errReply != nil
	}
	if !rollback {
		return &reply.OkReply{}
	}
	return errReply

}

// MSetNX sets multi key-value in database, only if none of the given writeKeys exist and all given writeKeys are on the same node
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
	return cluster.relay(peer, c, args)
}
