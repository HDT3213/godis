package cluster

import (
	"errors"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/pool"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/client"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
)

var connectionPoolConfig = pool.Config{
	MaxIdle:   1,
	MaxActive: 16,
}

func (cluster *Cluster) getPeerClient(peer string) (*client.Client, error) {
	var connectionPool *pool.Pool
	raw, ok := cluster.nodeConnections.Get(peer)
	if !ok {
		factory := func() (interface{}, error) {
			c, err := client.MakeClient(peer)
			if err != nil {
				return nil, err
			}
			c.Start()
			// all peers of cluster should use the same password
			if config.Properties.RequirePass != "" {
				c.Send(utils.ToCmdLine("AUTH", config.Properties.RequirePass))
			}
			return c, nil
		}
		finalizer := func(x interface{}) {
			cli, ok := x.(client.Client)
			if !ok {
				return
			}
			cli.Close()
		}
		connectionPool = pool.New(factory, finalizer, connectionPoolConfig)
		cluster.nodeConnections.Put(peer, connectionPool)
	} else {
		connectionPool = raw.(*pool.Pool)
	}
	raw, err := connectionPool.Get()
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
	raw, ok := cluster.nodeConnections.Get(peer)
	if !ok {
		return errors.New("connection pool not found")
	}
	raw.(*pool.Pool).Put(peerClient)
	return nil
}

var defaultRelayImpl = func(cluster *Cluster, node string, c redis.Connection, cmdLine CmdLine) redis.Reply {
	peerClient, err := cluster.getPeerClient(node)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.returnPeerClient(node, peerClient)
	}()
	return peerClient.Send(cmdLine)
}

// relay function relays command to peer or calls cluster.db.Exec
// If the invoker is a commander handler of cluster,
// 	calling cluster.db.Exec instead of Cluster.Exec could avoid infinite recursion. such as cluster.Del
// But it can not execute command implemented in `cluster` package, such as Transaction.prepare
func (cluster *Cluster) relay(peer string, c redis.Connection, args [][]byte) redis.Reply {
	// use a variable to allow injecting stub for testing, see defaultRelayImpl
	if peer == cluster.self {
		// to self db
		return cluster.db.Exec(c, args)
	}
	return cluster.relayImpl(cluster, peer, c, args)
}

// relay2 function relays command to peer or calls cluster.Exec
// If relay2 invoked by a commander handler of cluster may cause infinite recursion
// For example. if cluster.Del calls relay2("DEL") the actual stack is: cluster.Exec -> cluster.Del -> relay2 -> cluster.Exec
// But it can not execute command implemented in `cluster` package, such as Transaction.prepare
func (cluster *Cluster) relay2(peer string, c redis.Connection, args [][]byte) redis.Reply {
	// use a variable to allow injecting stub for testing, see defaultRelayImpl
	if peer == cluster.self {
		// to self db
		return cluster.Exec(c, args)
	}
	return cluster.relayImpl(cluster, peer, c, args)
}

// relayByKey function relays command to peer
// use routeKey to determine peer node
func (cluster *Cluster) relayByKey(routeKey string, c redis.Connection, args [][]byte) redis.Reply {
	slotId := getSlot(routeKey)
	peer := cluster.topology.PickNode(slotId)
	return cluster.relay(peer.Addr, c, args)
}

// broadcast function broadcasts command to all node in cluster
func (cluster *Cluster) broadcast(c redis.Connection, args [][]byte) map[string]redis.Reply {
	result := make(map[string]redis.Reply)
	for _, node := range cluster.topology.GetTopology() {
		reply := cluster.relay(node.Addr, c, args)
		result[node.Addr] = reply
	}
	return result
}

// ensureKey will migrate key to current node if the key is in a slot migrating to current node
// invoker should provide with locks of key
func (cluster *Cluster) ensureKey(key string) protocol.ErrorReply {
	slotId := getSlot(key)
	cluster.slotMu.RLock()
	slot := cluster.slots[slotId]
	cluster.slotMu.RUnlock()
	if slot == nil {
		return nil
	}
	if slot.state != slotStateImporting || slot.importedKeys.Has(key) {
		return nil
	}
	resp := cluster.relay(slot.oldNodeID, connection.NewFakeConn(), utils.ToCmdLine("DumpKey", key))
	if protocol.IsErrorReply(resp) {
		return resp.(protocol.ErrorReply)
	}
	dumpResp := resp.(*protocol.MultiBulkReply)
	if len(dumpResp.Args) != 2 {
		return protocol.MakeErrReply("illegal dump key response")
	}
	// reuse copy to command ^_^
	resp = cluster.db.Exec(connection.NewFakeConn(), [][]byte{
		[]byte("CopyTo"), []byte(key), dumpResp.Args[0], dumpResp.Args[1],
	})
	if protocol.IsErrorReply(resp) {
		return resp.(protocol.ErrorReply)
	}
	slot.importedKeys.Add(key)
	return nil
}
