package core

import (

	"github.com/hdt3213/godis/cluster/raft"
	dbimpl "github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
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

	// setup
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
			defer connections.ReturnPeerClient(conn)
			joinCmdLine := utils.ToCmdLine(joinClusterCommand, cfg.RedisAdvertiseAddr, cfg.RaftAdvertiseAddr)
			if cfg.Master != "" {
				joinCmdLine = append(joinCmdLine, []byte(cfg.Master))
			}
			logger.Infof("send join cluster request to %s", cfg.JoinAddress)
			result := conn.Send(joinCmdLine)
			if err := protocol.Try2ErrorReply(result); err != nil {
				return nil, err
			}
		}
	} else {
		masterAddr := cluster.raftNode.FSM.GetMaster(cluster.SelfID())
		if masterAddr != "" {
			err := cluster.SlaveOf(masterAddr)
			if err != nil {
				panic(err)
			}
		}
	}
	
	go cluster.clusterCron()
	return cluster, nil
}

// AfterClientClose does some clean after client close connection
func (cluster *Cluster) AfterClientClose(c redis.Connection) {
	cluster.db.AfterClientClose(c)
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
