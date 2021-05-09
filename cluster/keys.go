package cluster

import (
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/reply"
)

// FlushDB removes all data in current database
func FlushDB(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	replies := cluster.broadcast(c, args)
	var errReply reply.ErrorReply
	for _, v := range replies {
		if reply.IsErrorReply(v) {
			errReply = v.(reply.ErrorReply)
			break
		}
	}
	if errReply == nil {
		return &reply.OkReply{}
	}
	return reply.MakeErrReply("error occurs: " + errReply.Error())
}

// FlushAll removes all data in cluster
func FlushAll(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	return FlushDB(cluster, c, args)
}
