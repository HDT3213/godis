package cluster

import (
	"errors"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/client"
	"github.com/hdt3213/godis/redis/protocol"
	"strconv"
)

func (cluster *Cluster) getPeerClient(peer string) (*client.Client, error) {
	pool, ok := cluster.nodeConnections[peer]
	if !ok {
		return nil, errors.New("connection pool not found")
	}
	raw, err := pool.Get()
	if err != nil {
		return nil, err
	}
	conn, ok := raw.(*client.Client)
	if !ok {
		return nil, errors.New("connection pool make wrong type")
	}
	return conn, nil
}

func (cluster *Cluster) returnPeerClient(peer string, peerClient *client.Client) error {
	pool, ok := cluster.nodeConnections[peer]
	if !ok {
		return errors.New("connection pool not found")
	}
	pool.Put(peerClient)
	return nil
}

var defaultRelayImpl = func(cluster *Cluster, node string, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if node == cluster.self {
		// to self db
		return cluster.db.Exec(c, cmdLine)
	}
	peerClient, err := cluster.getPeerClient(node)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.returnPeerClient(node, peerClient)
	}()
	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(c.GetDBIndex())))
	return peerClient.Send(cmdLine)
}

// relay function relays command to peer
// select db by c.GetDBIndex()
// cannot call Prepare, Commit, execRollback of self node
func (cluster *Cluster) relay(peer string, c redis.Connection, args [][]byte) redis.Reply {
	// use a variable to allow injecting stub for testing
	return cluster.relayImpl(cluster, peer, c, args)
}

// broadcast function broadcasts command to all node in cluster
func (cluster *Cluster) broadcast(c redis.Connection, args [][]byte) map[string]redis.Reply {
	result := make(map[string]redis.Reply)
	for _, node := range cluster.nodes {
		reply := cluster.relay(node, c, args)
		result[node] = reply
	}
	return result
}
