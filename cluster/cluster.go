// Package cluster provides a server side cluster which is transparent to client. You can connect to any node in the cluster to access all data in the cluster
package cluster

import (
	"context"
	"fmt"
	"github.com/hdt3213/godis"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/consistenthash"
	"github.com/hdt3213/godis/lib/idgenerator"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/redis/reply"
	"github.com/jolestar/go-commons-pool/v2"
	"runtime/debug"
	"strings"
)

// Cluster represents a node of godis cluster
// it holds part of data and coordinates other nodes to finish transactions
type Cluster struct {
	self string

	nodes          []string
	peerPicker     *consistenthash.Map
	peerConnection map[string]*pool.ObjectPool

	db           *godis.DB
	transactions *dict.SimpleDict // id -> Transaction

	idGenerator *idgenerator.IDGenerator
}

const (
	replicas = 4
	lockSize = 64
)

// if only one node involved in a transaction, just execute the command don't apply tcc procedure
var allowFastTransaction = true

// MakeCluster creates and starts a node of cluster
func MakeCluster() *Cluster {
	cluster := &Cluster{
		self: config.Properties.Self,

		db:             godis.MakeDB(),
		transactions:   dict.MakeSimple(),
		peerPicker:     consistenthash.New(replicas, nil),
		peerConnection: make(map[string]*pool.ObjectPool),

		idGenerator: idgenerator.MakeGenerator(config.Properties.Self),
	}
	contains := make(map[string]struct{})
	nodes := make([]string, 0, len(config.Properties.Peers)+1)
	for _, peer := range config.Properties.Peers {
		if _, ok := contains[peer]; ok {
			continue
		}
		contains[peer] = struct{}{}
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self)
	cluster.peerPicker.AddNode(nodes...)
	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		cluster.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: peer,
		})
	}
	cluster.nodes = nodes
	return cluster
}

// CmdFunc represents the handler of a redis command
type CmdFunc func(cluster *Cluster, c redis.Connection, cmdAndArgs [][]byte) redis.Reply

// Close stops current node of cluster
func (cluster *Cluster) Close() {
	cluster.db.Close()
}

var router = makeRouter()

func isAuthenticated(c redis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}

// Exec executes command on cluster
func (cluster *Cluster) Exec(c redis.Connection, cmdArgs [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()
	cmd := strings.ToLower(string(cmdArgs[0]))
	if cmd == "auth" {
		return godis.Auth(cluster.db, c, cmdArgs[1:])
	}
	if !isAuthenticated(c) {
		return reply.MakeErrReply("NOAUTH Authentication required")
	}
	cmdFunc, ok := router[cmd]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmd + "', or not supported in cluster mode")
	}
	result = cmdFunc(cluster, c, cmdArgs)
	return
}

// AfterClientClose does some clean after client close connection
func (cluster *Cluster) AfterClientClose(c redis.Connection) {
	cluster.db.AfterClientClose(c)
}

func ping(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	return godis.Ping(cluster.db, args[1:])
}

/*----- utils -------*/

func makeArgs(cmd string, args ...string) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(cmd)
	for i, arg := range args {
		result[i+1] = []byte(arg)
	}
	return result
}

// return peer -> writeKeys
func (cluster *Cluster) groupBy(keys []string) map[string][]string {
	result := make(map[string][]string)
	for _, key := range keys {
		peer := cluster.peerPicker.PickNode(key)
		group, ok := result[peer]
		if !ok {
			group = make([]string, 0)
		}
		group = append(group, key)
		result[peer] = group
	}
	return result
}
