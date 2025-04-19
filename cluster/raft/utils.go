package raft

import (
	"fmt"
	"strconv"

	"github.com/hashicorp/raft"
)

func (node *Node) Self() string {
	return node.Cfg.ID()
}

func (node *Node) State() raft.RaftState {
	return node.inner.State()
}

func (node *Node) CommittedIndex() (uint64, error) {
	stats := node.inner.Stats()
	committedIndex0 := stats["commit_index"]
	return strconv.ParseUint(committedIndex0, 10, 64)
}

func (node *Node) GetLeaderRedisAddress() string {
	// redis advertise address used as leader id
	_, id := node.inner.LeaderWithID()
	return string(id)
}

func (node *Node) GetNodes() ([]raft.Server, error) {
	configFuture := node.inner.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return nil, fmt.Errorf("failed to get raft configuration: %v", err)
	}
	return configFuture.Configuration().Servers, nil
}

func (node *Node) GetSlaves(id string) *MasterSlave {
	node.FSM.mu.RLock()
	defer node.FSM.mu.RUnlock()
	return node.FSM.MasterSlaves[id]
}
