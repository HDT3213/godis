package cluster

import (
	"github.com/HDT3213/godis/src/interface/redis"
	"github.com/HDT3213/godis/src/lib/logger"
	"github.com/HDT3213/godis/src/redis/reply"
)

const (
	relayPublish = "_publish"
	publish      = "publish"
)

var (
	publishRelayCmd = []byte(relayPublish)
	publishCmd      = []byte(publish)
)

// broadcast msg to all peers in cluster when receive publish command from client
func Publish(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	var count int64 = 0
	for _, peer := range cluster.peers {
		var re redis.Reply
		if peer == cluster.self {
			args0 := make([][]byte, len(args))
			copy(args0, args)
			args0[0] = publishCmd
			re = cluster.db.Exec(c, args0) // let local db.hub handle publish
		} else {
			args[0] = publishRelayCmd
			re = cluster.Relay(peer, c, args)
		}
		if errReply, ok := re.(reply.ErrorReply); ok {
			logger.Error("publish occurs error: " + errReply.Error())
		} else if intReply, ok := re.(*reply.IntReply); ok {
			count += intReply.Code
		}
	}
	return reply.MakeIntReply(count)
}

// receive publish command from peer, just publish to local subscribing clients, do not relay to peers
func OnRelayedPublish(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	args[0] = publishCmd
	return cluster.db.Exec(c, args) // let local db.hub handle publish
}

func Subscribe(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	return cluster.db.Exec(c, args) // let local db.hub handle subscribe
}

func UnSubscribe(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	return cluster.db.Exec(c, args) // let local db.hub handle subscribe
}
