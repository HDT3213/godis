package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type Node struct {
	Cfg           *RaftConfig
	inner         *raft.Raft
	FSM           *FSM
	logStore      raft.LogStore
	stableStore   raft.StableStore
	snapshotStore raft.SnapshotStore
	transport     raft.Transport
}

type RaftConfig struct {
	RedisAdvertiseAddr string // it also be used as node id,
	RaftListenAddr     string
	RaftAdvertiseAddr  string
	Dir                string
}

func (cfg *RaftConfig) ID() string {
	return cfg.RedisAdvertiseAddr
}

var Leader = raft.Leader
var Follower = raft.Follower
var Candidate = raft.Candidate

func StartNode(cfg *RaftConfig) (*Node, error) {
	if cfg.RaftAdvertiseAddr == "" {
		cfg.RaftAdvertiseAddr = cfg.RaftListenAddr
	}
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(cfg.ID())
	if config.LocalID == "" {
		// cfg.ID() actually is  cfg.RedisAdvertiseAddr
		return nil, errors.New("redis address is required")
	}
	leaderNotifyCh := make(chan bool, 10)
	config.NotifyCh = leaderNotifyCh

	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAdvertiseAddr)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(cfg.RaftListenAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}
	// todo: mkdir if possible
	snapshotStore, err := raft.NewFileSnapshotStore(cfg.Dir, 2, os.Stderr)
	if err != nil {
		return nil, err
	}

	boltDB, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(cfg.Dir, "raft.db"),
	})
	if err != nil {
		return nil, err
	}

	storage := &FSM{
		Node2Slot:  make(map[string][]uint32),
		Slot2Node:  make(map[uint32]string),
		Migratings: make(map[string]*MigratingTask),
	}

	logStore := boltDB
	stableStore := boltDB
	inner, err := raft.NewRaft(config, storage, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	return &Node{
		Cfg:           cfg,
		inner:         inner,
		FSM:           storage,
		logStore:      logStore,
		stableStore:   stableStore,
		snapshotStore: snapshotStore,
		transport:     transport,
	}, nil
}

func (node *Node) HasExistingState() (bool, error) {
	return raft.HasExistingState(node.logStore, node.stableStore, node.snapshotStore)
}

// BootstrapCluster creates a raft cluster, and returns after self makes leader
func (node *Node) BootstrapCluster(slotCount int) error {
	future := node.inner.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(node.Cfg.ID()),
				Address: node.transport.LocalAddr(),
			},
		},
	})
	err := future.Error()
	if err != nil {
		return fmt.Errorf("BootstrapCluster failed: %v", err)
	}
	// wait self leader
	for {
		if node.State() == raft.Leader {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	// init fsm
	_, err = node.Propose(&LogEntry{Event: EventSeedStart, InitTask: &InitTask{
		Leader:    node.Cfg.ID(),
		SlotCount: slotCount,
	}})
	return err
}

func (node *Node) Shutdown() error {
	future := node.inner.Shutdown()
	return future.Error()
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

// HandleJoin handles join request, node must be leader
func (node *Node) HandleJoin(redisAddr, raftAddr string) error {
	configFuture := node.inner.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to get raft configuration: %v", err)
	}
	id := raft.ServerID(redisAddr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == id {
			return errors.New("already in cluster")
		}
	}
	future := node.inner.AddVoter(id, raft.ServerAddress(raftAddr), 0, 0)
	return future.Error()
}

func (node *Node) Propose(event *LogEntry) (uint64, error) {
	bin, err := json.Marshal(event)
	if err != nil {
		return 0, fmt.Errorf("marshal event failed: %v", err)
	}
	future := node.inner.Apply(bin, 0)
	err = future.Error()
	if err != nil {
		return 0, fmt.Errorf("raft propose failed: %v", err)
	}
	return future.Index(), nil
}
