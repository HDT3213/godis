package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
	"strconv"
	"sync"
)

const (
	eventNewNode = iota + 1
	eventSetSlot
)

type logEntry struct {
	Term  int
	Index int
	Event int
	wg    *sync.WaitGroup
	// payload
	SlotID uint32
	NodeID string
	Addr   string
}

func (e *logEntry) marshal() []byte {
	bin, _ := json.Marshal(e)
	return bin
}

func (e *logEntry) unmarshal(bin []byte) error {
	err := json.Unmarshal(bin, e)
	if err != nil {
		return fmt.Errorf("illegal message: %v", err)
	}
	return nil
}

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
		case eventSetSlot:
			slot := raft.slots[int(entry.SlotID)]
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
			newNode := raft.nodes[slot.NodeID]
			newNode.Slots = append(newNode.Slots, slot)
		}
	}
}

// execRaftPropose handles requests from other nodes (follower or learner) to propose a change
// command line: raft propose <logEntry>
func execRaftPropose(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if cluster.topology.state != leader {
		leaderNode := cluster.topology.nodes[cluster.topology.leaderId]
		return protocol.MakeErrReply("NOT LEADER " + leaderNode.ID + " " + leaderNode.Addr)
	}
	if len(args) != 1 {
		return protocol.MakeArgNumErrReply("raft propose")
	}

	e := &logEntry{}
	err := e.unmarshal(args[0])
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	if errReply := cluster.topology.propose(e); errReply != nil {
		return errReply
	}
	return protocol.MakeOkReply()
}

func (raft *Raft) propose(e *logEntry) protocol.ErrorReply {
	switch e.Event {
	case eventNewNode:
		raft.mu.Lock()
		_, ok := raft.nodes[e.Addr]
		raft.mu.Unlock()
		if ok {
			return protocol.MakeErrReply("node exists")
		}
	}
	cluster := raft.cluster
	wg := wgPool.Get().(*sync.WaitGroup)
	defer wgPool.Put(wg)
	e.wg = wg
	cluster.topology.mu.Lock()
	cluster.topology.proposalIndex++
	e.Term = cluster.topology.term
	e.Index = cluster.topology.proposalIndex
	cluster.topology.log = append(cluster.topology.log, e)
	cluster.topology.mu.Unlock()
	e.wg.Add(1)
	e.wg.Wait() // wait for the raft group to reach a consensus
	return nil
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

// setLocalSlotMigrating set a hosting slot as migrating state.
// only two related nodes should invoke this function
func (raft *Raft) setLocalSlotMigrating(slotID uint32, newNodeID string) {
	slot := raft.slots[int(slotID)]
	slot.Flags |= slotFlagMigrating
	slot.OldNodeID = slot.NodeID
	slot.NodeID = newNodeID
	// raft.nodes.Slots is the same object with raft.slots, no need to update
}

// SetSlot propose
func (raft *Raft) SetSlot(slotID uint32, newNodeID string) error {
	proposal := &logEntry{
		Event:  eventSetSlot,
		NodeID: newNodeID,
		SlotID: slotID,
	}
	conn := connection.NewFakeConn()
	resp := raft.cluster.relay(raft.leaderId, conn,
		utils.ToCmdLine("raft", "propose", string(proposal.marshal())))
	if err, ok := resp.(protocol.ErrorReply); ok {
		return err
	}
	return nil
}

// execRaftJoin handles requests from a new node to join raft group as leader
// command line: raft join addr
func execRaftJoin(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeArgNumErrReply("raft join")
	}
	if cluster.topology.state != leader {
		leaderNode := cluster.topology.nodes[cluster.topology.leaderId]
		return protocol.MakeErrReply("NOT LEADER " + leaderNode.ID + " " + leaderNode.Addr)
	}
	addr := string(args[0])
	nodeID := addr
	proposal := &logEntry{
		Event:  eventNewNode,
		NodeID: nodeID,
		Addr:   addr,
	}
	if err := cluster.topology.propose(proposal); err != nil {
		return err
	}
	cluster.topology.mu.Lock()
	defer cluster.topology.mu.Unlock()
	if cluster.topology.nodeIndexMap[nodeID] == nil {
		cluster.topology.nodeIndexMap[nodeID] = &nodeIndex{}
	}
	cluster.topology.nodeIndexMap[nodeID].recvedIndex = cluster.topology.commitIndex
	topology := marshalTopology(cluster.topology.nodes)
	resp := make([][]byte, 0, len(topology)+1)
	resp = append(resp,
		[]byte(nodeID),
		[]byte(cluster.topology.leaderId),
		[]byte(strconv.Itoa(cluster.topology.term)),
		[]byte(strconv.Itoa(cluster.topology.commitIndex)),
	)
	resp = append(resp, topology...)
	return protocol.MakeMultiBulkReply(resp)
}
