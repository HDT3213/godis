package cluster

import (
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
)

// relay function relays command to peer or calls cluster.Exec
func (cluster *Cluster) relay(peerId string, c redis.Connection, cmdLine [][]byte) redis.Reply {
	// use a variable to allow injecting stub for testing, see defaultRelayImpl
	if peerId == cluster.self {
		// to self db
		return cluster.Exec(c, cmdLine)
	}
	// peerId is peer.Addr
	cli, err := cluster.clientFactory.GetPeerClient(peerId)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.clientFactory.ReturnPeerClient(peerId, cli)
	}()
	return cli.Send(cmdLine)
}

// relayByKey function relays command to peer
// use routeKey to determine peer node
func (cluster *Cluster) relayByKey(routeKey string, c redis.Connection, args [][]byte) redis.Reply {
	slotId := getSlot(routeKey)
	peer := cluster.pickNode(slotId)
	return cluster.relay(peer.ID, c, args)
}

// broadcast function broadcasts command to all node in cluster
func (cluster *Cluster) broadcast(c redis.Connection, args [][]byte) map[string]redis.Reply {
	result := make(map[string]redis.Reply)
	for _, node := range cluster.topology.GetNodes() {
		reply := cluster.relay(node.ID, c, args)
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
	resp := cluster.relay(slot.oldNodeID, connection.NewFakeConn(), utils.ToCmdLine("DumpKey_", key))
	if protocol.IsErrorReply(resp) {
		return resp.(protocol.ErrorReply)
	}
	if protocol.IsEmptyMultiBulkReply(resp) {
		slot.importedKeys.Add(key)
		return nil
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

func (cluster *Cluster) ensureKeyWithoutLock(key string) protocol.ErrorReply {
	cluster.db.RWLocks(0, []string{key}, nil)
	defer cluster.db.RWUnLocks(0, []string{key}, nil)
	return cluster.ensureKey(key)
}
