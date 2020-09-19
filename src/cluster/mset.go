package cluster

import (
    "fmt"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
    "strings"
)

func MGet(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'del' command")
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

func MSet(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
    argCount := len(args) - 1
    if argCount%2 != 0 || argCount < 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'mset' command")
    }

    size := argCount / 2
    keys := make([]string, size)
    valueMap := make(map[string]string)
    for i := 0; i < size; i++ {
        keys[i] = string(args[2*i])
        valueMap[keys[i]] = string(args[2*i+1])
    }

    failedKeys := make([]string, 0)
    groupMap := cluster.groupBy(keys)
    for peer, groupKeys := range groupMap {
        peerArgs := make([][]byte, 2*len(groupKeys)+1)
        peerArgs[0] = []byte("MSET")
        for i, k := range groupKeys {
            peerArgs[2*i+1] = []byte(k)
            value := valueMap[k]
            peerArgs[2*i+2] = []byte(value)
        }
        resp := cluster.Relay(peer, c, peerArgs)
        if reply.IsErrorReply(resp) {
            failedKeys = append(failedKeys, groupKeys...)
        }
    }
    if len(failedKeys) > 0 {
        return reply.MakeErrReply("ERR part failure: " + strings.Join(failedKeys, ","))
    }
    return &reply.OkReply{}

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
        currentPeer := cluster.peerPicker.Get(key)
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
