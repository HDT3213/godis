package core

import (
	"sync"

	"github.com/hdt3213/godis/cluster/raft"
	dbimpl "github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/datastruct/set"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
	rdbcore "github.com/hdt3213/rdb/core"
)

type Cluster struct {
	raftNode    *raft.Node
	db          database.DBEngine
	connections ConnectionFactory
	config      *Config

	slotsManager    *slotsManager
	rebalanceManger *rebalanceManager
	transactions    *TransactionManager
	replicaManager  *replicaManager

	closeChan chan struct{}

	// allow inject route implementation
	getSlotImpl  func(key string) uint32
	pickNodeImpl func(slotID uint32) string
	id_          string // for tests only
}

type Config struct {
	raft.RaftConfig
	StartAsSeed    bool
	JoinAddress    string
	Master         string
	connectionStub ConnectionFactory // for test
	noCron         bool // for test
}

func (c *Cluster) SelfID() string {
	if c.raftNode == nil {
		return c.id_
	}
	return c.raftNode.Cfg.ID()
}

// slotsManager 负责管理当前 node 上的 slot
type slotsManager struct {
	mu            *sync.RWMutex
	slots         map[uint32]*slotStatus // 记录当前node上的 slot
	importingTask *raft.MigratingTask
}

const (
	slotStateHosting = iota
	slotStateImporting
	slotStateExporting
)

type slotStatus struct {
	mu    *sync.RWMutex
	state int
	keys  *set.Set // 记录当前 slot 上的 key

	exportSnapshot *set.Set // 开始传输时拷贝 slot 中的 key, 避免并发并发
	dirtyKeys      *set.Set // 传输开始后被修改的key, 在传输结束阶段需要重传一遍
}

func newSlotsManager() *slotsManager {
	return &slotsManager{
		mu:    &sync.RWMutex{},
		slots: map[uint32]*slotStatus{},
	}
}

func (ssm *slotsManager) getSlot(index uint32) *slotStatus {
	ssm.mu.RLock()
	slot := ssm.slots[index]
	ssm.mu.RUnlock()
	if slot != nil {
		return slot
	}
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	// check-lock-check
	slot = ssm.slots[index]
	if slot != nil {
		return slot
	}
	slot = &slotStatus{
		state: slotStateHosting,
		keys:  set.Make(),
		mu:    &sync.RWMutex{},
	}
	ssm.slots[index] = slot
	return slot
}

func NewCluster(cfg *Config) (*Cluster, error) {
	var connections ConnectionFactory
	if cfg.connectionStub != nil {
		connections = cfg.connectionStub
	} else {
		connections = newDefaultClientFactory()
	}
	db := dbimpl.NewStandaloneServer()
	raftNode, err := raft.StartNode(&cfg.RaftConfig)
	if err != nil {
		return nil, err
	}
	hasState, err := raftNode.HasExistingState()
	if err != nil {
		return nil, err
	}
	if !hasState {
		if cfg.StartAsSeed {
			err = raftNode.BootstrapCluster(SlotCount)
			if err != nil {
				return nil, err
			}
		} else {
			// join cluster
			conn, err := connections.BorrowPeerClient(cfg.JoinAddress)
			if err != nil {
				return nil, err
			}
			joinCmdLine := utils.ToCmdLine(joinClusterCommand, cfg.RedisAdvertiseAddr, cfg.RaftAdvertiseAddr)
			if cfg.Master != "" {
				joinCmdLine = append(joinCmdLine, []byte(cfg.Master))
			}
			result := conn.Send(joinCmdLine)
			if err := protocol.Try2ErrorReply(result); err != nil {
				return nil, err
			}
		}
	}
	cluster := &Cluster{
		raftNode:        raftNode,
		db:              db,
		connections:     connections,
		config:          cfg,
		rebalanceManger: newRebalanceManager(),
		slotsManager:    newSlotsManager(),
		transactions:    newTransactionManager(),
		replicaManager:  newReplicaManager(),
		closeChan:       make(chan struct{}),
	}
	cluster.pickNodeImpl = func(slotID uint32) string {
		return defaultPickNodeImpl(cluster, slotID)
	}
	cluster.getSlotImpl = func(key string) uint32 {
		return defaultGetSlotImpl(cluster, key)
	}
	cluster.injectInsertCallback()
	cluster.injectDeleteCallback()
	cluster.registerOnFailover()
	go cluster.clusterCron()
	return cluster, nil
}

// AfterClientClose does some clean after client close connection
func (cluster *Cluster) AfterClientClose(c redis.Connection) {

}

func (cluster *Cluster) Close() {
	close(cluster.closeChan)
	cluster.db.Close()
	err := cluster.raftNode.Close()
	if err != nil {
		panic(err)
	}
}

// LoadRDB real implementation of loading rdb file
func (cluster *Cluster) LoadRDB(dec *rdbcore.Decoder) error {
	return cluster.db.LoadRDB(dec)
}
