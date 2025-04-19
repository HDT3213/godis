package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
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
	watcher       watcher
}

type watcher struct {
	watch         func(*FSM)
	currentMaster string
	onFailover    func(newMaster string)
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
		Node2Slot:    make(map[string][]uint32),
		Slot2Node:    make(map[uint32]string),
		Migratings:   make(map[string]*MigratingTask),
		MasterSlaves: make(map[string]*MasterSlave),
		SlaveMasters: make(map[string]string),
		Failovers:    make(map[string]*FailoverTask),
	}

	logStore := boltDB
	stableStore := boltDB
	inner, err := raft.NewRaft(config, storage, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}
	node := &Node{
		Cfg:           cfg,
		inner:         inner,
		FSM:           storage,
		logStore:      logStore,
		stableStore:   stableStore,
		snapshotStore: snapshotStore,
		transport:     transport,
	}
	node.setupWatch()
	return node, nil
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

func (node *Node) Close() error {
	future := node.inner.Shutdown()
	return fmt.Errorf("raft shutdown %v", future.Error())
}

// AddToRaft handles join request, node must be leader
func (node *Node) AddToRaft(redisAddr, raftAddr string) error {
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

func (node *Node) HandleEvict(redisAddr string) error {
	configFuture := node.inner.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to get raft configuration: %v", err)
	}
	id := raft.ServerID(redisAddr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == id {
			err := node.inner.RemoveServer(srv.ID, 0, 0).Error()
			if err != nil {
				return err
			}
		}
	}
	return nil
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

func (node *Node) setupWatch() {
	node.watcher.watch = func(f *FSM) {
		newMaster := f.SlaveMasters[node.Self()]
		if newMaster != node.watcher.currentMaster {
			node.watcher.currentMaster = newMaster
			if node.watcher.onFailover != nil {
				node.watcher.onFailover(newMaster)
			}
		}
	}
}

// SetOnFailover sets onFailover callback
// After a failover, onFailover will receive the new master
func (node *Node) SetOnFailover(fn func(newMaster string)) {
	node.watcher.currentMaster = node.FSM.getMaster(node.Self())
	node.watcher.onFailover = fn
}
