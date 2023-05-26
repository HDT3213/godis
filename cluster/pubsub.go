package cluster

import (
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/redis/protocol"
)

const (
	relayPublish = "publish_"
)

// Publish broadcasts msg to all peers in cluster when receive publish command from client
func Publish(cluster *Cluster, c redis.Connection, cmdLine [][]byte) redis.Reply {
	var count int64 = 0
	results := cluster.broadcast(c, modifyCmd(cmdLine, relayPublish))
	for _, val := range results {
		if errReply, ok := val.(protocol.ErrorReply); ok {
			logger.Error("publish occurs error: " + errReply.Error())
		} else if intReply, ok := val.(*protocol.IntReply); ok {
			count += intReply.Code
		}
	}
	return protocol.MakeIntReply(count)
}

// Subscribe puts the given connection into the given channel
func Subscribe(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	return cluster.db.Exec(c, args) // let local db.hub handle subscribe
}

// UnSubscribe removes the given connection from the given channel
func UnSubscribe(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	return cluster.db.Exec(c, args) // let local db.hub handle subscribe
}
