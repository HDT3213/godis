package cluster

// raft event handlers

import (
	"errors"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
)

const (
	eventNewNode = iota + 1
	eventSetSlot
)

// invoker should provide with raft.mu lock
func (raft *Raft) applyLogEntries(entries []*logEntry) {
	for _, entry := range entries {
		switch entry.Event {
		case eventNewNode:
			node := &Node{
				ID:   entry.NodeID,
				Addr: entry.Addr,
			}
			raft.nodes[node.ID] = node
			if raft.state == leader {
				raft.nodeIndexMap[entry.NodeID] = &nodeStatus{
					receivedIndex: entry.Index, // the new node should not receive its own join event
				}
			}
		case eventSetSlot:
			for _, slotID := range entry.SlotIDs {
				slot := raft.slots[slotID]
				oldNode := raft.nodes[slot.NodeID]
				// remove from old oldNode
				for i, s := range oldNode.Slots {
					if s.ID == slot.ID {
						copy(oldNode.Slots[i:], oldNode.Slots[i+1:])
						oldNode.Slots = oldNode.Slots[:len(oldNode.Slots)-1]
						break
					}
				}
				newNodeID := entry.NodeID
				slot.NodeID = newNodeID
				// fixme: 多个节点同时加入后 re balance 时 newNode 可能为 nil
				newNode := raft.nodes[slot.NodeID]
				newNode.Slots = append(newNode.Slots, slot)
			}
		}
	}
	if err := raft.persist(); err != nil {
		logger.Errorf("persist raft error: %v", err)
	}

}

// NewNode creates a new Node when a node request self node for joining cluster
func (raft *Raft) NewNode(addr string) (*Node, error) {
	if _, ok := raft.nodes[addr]; ok {
		return nil, errors.New("node existed")
	}
	node := &Node{
		ID:   addr,
		Addr: addr,
	}
	raft.nodes[node.ID] = node
	proposal := &logEntry{
		Event:  eventNewNode,
		NodeID: node.ID,
		Addr:   node.Addr,
	}
	conn := connection.NewFakeConn()
	resp := raft.cluster.relay(raft.leaderId, conn,
		utils.ToCmdLine("raft", "propose", string(proposal.marshal())))
	if err, ok := resp.(protocol.ErrorReply); ok {
		return nil, err
	}
	return node, nil
}

// SetSlot propose
func (raft *Raft) SetSlot(slotIDs []uint32, newNodeID string) protocol.ErrorReply {
	proposal := &logEntry{
		Event:   eventSetSlot,
		NodeID:  newNodeID,
		SlotIDs: slotIDs,
	}
	conn := connection.NewFakeConn()
	resp := raft.cluster.relay(raft.leaderId, conn,
		utils.ToCmdLine("raft", "propose", string(proposal.marshal())))
	if err, ok := resp.(protocol.ErrorReply); ok {
		return err
	}
	return nil
}

// execRaftJoin handles requests from a new node to join raft group, current node should be leader
// command line: raft join addr
func execRaftJoin(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeArgNumErrReply("raft join")
	}
	raft := cluster.asRaft()
	if raft.state != leader {
		leaderNode := raft.nodes[raft.leaderId]
		return protocol.MakeErrReply("NOT LEADER " + leaderNode.ID + " " + leaderNode.Addr)
	}
	addr := string(args[0])
	nodeID := addr

	raft.mu.RLock()
	_, exist := raft.nodes[addr]
	raft.mu.RUnlock()
	// if node has joint cluster but terminated before persisting cluster config,
	// it may try to join at next start.
	// In this case, we only have to send a snapshot for it
	if !exist {
		proposal := &logEntry{
			Event:  eventNewNode,
			NodeID: nodeID,
			Addr:   addr,
		}
		if err := raft.propose(proposal); err != nil {
			return err
		}
	}
	raft.mu.RLock()
	snapshot := raft.makeSnapshotForFollower(nodeID)
	raft.mu.RUnlock()
	return protocol.MakeMultiBulkReply(snapshot)
}
