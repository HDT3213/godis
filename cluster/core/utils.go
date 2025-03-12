package core

import (
	"hash/crc32"
	"strings"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
)

const SlotCount int = 1024

const getCommittedIndexCommand = "raft.committedindex"

func init() {
	RegisterCmd(getCommittedIndexCommand, execRaftCommittedIndex)
}

// relay function relays command to peer or calls cluster.Exec
func (cluster *Cluster) Relay(peerId string, c redis.Connection, cmdLine [][]byte) redis.Reply {
	// use a variable to allow injecting stub for testing, see defaultRelayImpl
	if peerId == cluster.SelfID() {
		// to self db
		return cluster.Exec(c, cmdLine)
	}
	// peerId is peer.Addr
	cli, err := cluster.connections.BorrowPeerClient(peerId)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.connections.ReturnPeerClient(cli)
	}()
	return cli.Send(cmdLine)
}

// LocalExec executes command at local node
func (cluster *Cluster) LocalExec(c redis.Connection, cmdLine [][]byte) redis.Reply {
	return cluster.db.Exec(c, cmdLine)
}

// GetPartitionKey extract hashtag
func GetPartitionKey(key string) string {
	beg := strings.Index(key, "{")
	if beg == -1 {
		return key
	}
	end := strings.Index(key, "}")
	if end == -1 || end == beg+1 {
		return key
	}
	return key[beg+1 : end]
}

func defaultGetSlotImpl(cluster *Cluster, key string) uint32 {
	partitionKey := GetPartitionKey(key)
	return crc32.ChecksumIEEE([]byte(partitionKey)) % uint32(SlotCount)
}

func (cluster *Cluster) GetSlot(key string) uint32 {
	return cluster.getSlotImpl(key)
}

func defaultPickNodeImpl(cluster *Cluster, slotID uint32) string {
	return cluster.raftNode.FSM.PickNode(slotID)
}

// pickNode returns the node id hosting the given slot.
// If the slot is migrating, return the node which is exporting the slot
func (cluster *Cluster) PickNode(slotID uint32) string {
	return cluster.pickNodeImpl(slotID)
}

// format: raft.committedindex
func execRaftCommittedIndex(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	index, err := cluster.raftNode.CommittedIndex()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeIntReply(int64(index))
}
