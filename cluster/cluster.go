package cluster

import (
	"context"
	"fmt"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/db"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/consistenthash"
	"github.com/hdt3213/godis/lib/idgenerator"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/redis/reply"
	"github.com/jolestar/go-commons-pool/v2"
	"runtime/debug"
	"strings"
)

type Cluster struct {
	self string

	peers          []string
	peerPicker     *consistenthash.Map
	peerConnection map[string]*pool.ObjectPool

	db           *db.DB
	transactions *dict.SimpleDict // id -> Transaction

	idGenerator *idgenerator.IdGenerator
}

const (
	replicas = 4
	lockSize = 64
)

// start current processing as a node of cluster
func MakeCluster() *Cluster {
	cluster := &Cluster{
		self: config.Properties.Self,

		db:             db.MakeDB(),
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
	for _, peer := range nodes {
		cluster.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &ConnectionFactory{
			Peer: peer,
		})
	}
	cluster.peers = nodes
	return cluster
}

// args contains all
type CmdFunc func(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply

func (cluster *Cluster) Close() {
	cluster.db.Close()
}

var router = MakeRouter()

func (cluster *Cluster) Exec(c redis.Connection, args [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	cmd := strings.ToLower(string(args[0]))
	cmdFunc, ok := router[cmd]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmd + "', or not supported in cluster mode")
	}
	result = cmdFunc(cluster, c, args)
	return
}

func (cluster *Cluster) AfterClientClose(c redis.Connection) {

}

func Ping(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) == 1 {
		return &reply.PongReply{}
	} else if len(args) == 2 {
		return reply.MakeStatusReply("\"" + string(args[1]) + "\"")
	} else {
		return reply.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
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

// return peer -> keys
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
