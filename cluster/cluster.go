// Package cluster provides a server side cluster which is transparent to client. You can connect to any node in the cluster to access all data in the cluster
package cluster

import (
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/hdt3213/rdb/core"

	"github.com/hdt3213/godis/config"
	database2 "github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/datastruct/set"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/idgenerator"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
	"os"
	"path"
	"sync"
)

// Cluster represents a node of godis cluster
// it holds part of data and coordinates other nodes to finish transactions
type Cluster struct {
	self          string
	addr          string
	db            database.DBEngine
	transactions  *dict.SimpleDict // id -> Transaction
	transactionMu sync.RWMutex
	topology      topology
	slotMu        sync.RWMutex
	slots         map[uint32]*hostSlot
	idGenerator   *idgenerator.IDGenerator

	clientFactory clientFactory
}

type peerClient interface {
	Send(args [][]byte) redis.Reply
}

type peerStream interface {
	Stream() <-chan *parser.Payload
	Close() error
}

type clientFactory interface {
	GetPeerClient(peerAddr string) (peerClient, error)
	ReturnPeerClient(peerAddr string, peerClient peerClient) error
	NewStream(peerAddr string, cmdLine CmdLine) (peerStream, error)
	Close() error
}

const (
	slotStateHost = iota
	slotStateImporting
	slotStateMovingOut
)

// hostSlot stores status of host which hosted by current node
type hostSlot struct {
	state uint32
	mu    sync.RWMutex
	// OldNodeID is the node which is moving out this slot
	// only valid during slot is importing
	oldNodeID string
	// OldNodeID is the node which is importing this slot
	// only valid during slot is moving out
	newNodeID string

	/* importedKeys stores imported keys during migrating progress
	 * While this slot is migrating, if importedKeys does not have the given key, then current node will import key before execute commands
	 *
	 * In a migrating slot, the slot on the old node is immutable, we only delete a key in the new node.
	 * Therefore, we must distinguish between non-migrated key and deleted key.
	 * Even if a key has been deleted, it still exists in importedKeys, so we can distinguish between non-migrated and deleted.
	 */
	importedKeys *set.Set
	// keys stores all keys in this slot
	// Cluster.makeInsertCallback and Cluster.makeDeleteCallback will keep keys up to time
	keys *set.Set
}

// if only one node involved in a transaction, just execute the command don't apply tcc procedure
var allowFastTransaction = true

// MakeCluster creates and starts a node of cluster
func MakeCluster() *Cluster {
	cluster := &Cluster{
		self:          config.Properties.Self,
		addr:          config.Properties.AnnounceAddress(),
		db:            database2.NewStandaloneServer(),
		transactions:  dict.MakeSimple(),
		idGenerator:   idgenerator.MakeGenerator(config.Properties.Self),
		clientFactory: newDefaultClientFactory(),
	}
	topologyPersistFile := path.Join(config.Properties.Dir, config.Properties.ClusterConfigFile)
	cluster.topology = newRaft(cluster, topologyPersistFile)
	cluster.db.SetKeyInsertedCallback(cluster.makeInsertCallback())
	cluster.db.SetKeyDeletedCallback(cluster.makeDeleteCallback())
	cluster.slots = make(map[uint32]*hostSlot)
	var err error
	if topologyPersistFile != "" && fileExists(topologyPersistFile) {
		err = cluster.LoadConfig()
	} else if config.Properties.ClusterAsSeed {
		err = cluster.startAsSeed(config.Properties.AnnounceAddress())
	} else {
		err = cluster.Join(config.Properties.ClusterSeed)
	}
	if err != nil {
		panic(err)
	}
	return cluster
}

// CmdFunc represents the handler of a redis command
type CmdFunc func(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply

// Close stops current node of cluster
func (cluster *Cluster) Close() {
	_ = cluster.topology.Close()
	cluster.db.Close()
	cluster.clientFactory.Close()
}

func isAuthenticated(c redis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}

// Exec executes command on cluster
func (cluster *Cluster) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "info" {
		if ser, ok := cluster.db.(*database2.Server); ok {
			return database2.Info(ser, cmdLine[1:])
		}
	}
	if cmdName == "auth" {
		return database2.Auth(c, cmdLine[1:])
	}
	if !isAuthenticated(c) {
		return protocol.MakeErrReply("NOAUTH Authentication required")
	}

	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return database2.StartMulti(c)
	} else if cmdName == "discard" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return database2.DiscardMulti(c)
	} else if cmdName == "exec" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return execMulti(cluster, c, nil)
	} else if cmdName == "select" {
		return protocol.MakeErrReply("select not supported in cluster")
	}
	if c != nil && c.InMultiState() {
		return database2.EnqueueCmd(c, cmdLine)
	}
	cmdFunc, ok := router[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}
	result = cmdFunc(cluster, c, cmdLine)
	return
}

// AfterClientClose does some clean after client close connection
func (cluster *Cluster) AfterClientClose(c redis.Connection) {
	cluster.db.AfterClientClose(c)
}

func (cluster *Cluster) LoadRDB(dec *core.Decoder) error {
	return cluster.db.LoadRDB(dec)
}

func (cluster *Cluster) makeInsertCallback() database.KeyEventCallback {
	return func(dbIndex int, key string, entity *database.DataEntity) {
		slotId := getSlot(key)
		cluster.slotMu.RLock()
		slot, ok := cluster.slots[slotId]
		cluster.slotMu.RUnlock()
		// As long as the command is executed, we should update slot.keys regardless of slot.state
		if ok {
			slot.mu.Lock()
			defer slot.mu.Unlock()
			slot.keys.Add(key)
		}
	}
}

func (cluster *Cluster) makeDeleteCallback() database.KeyEventCallback {
	return func(dbIndex int, key string, entity *database.DataEntity) {
		slotId := getSlot(key)
		cluster.slotMu.RLock()
		slot, ok := cluster.slots[slotId]
		cluster.slotMu.RUnlock()
		// As long as the command is executed, we should update slot.keys regardless of slot.state
		if ok {
			slot.mu.Lock()
			defer slot.mu.Unlock()
			slot.keys.Remove(key)
		}
	}
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}
