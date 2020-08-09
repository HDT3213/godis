package cluster

import (
    "fmt"
    "github.com/HDT3213/godis/src/config"
    "github.com/HDT3213/godis/src/db"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/lib/consistenthash"
    "github.com/HDT3213/godis/src/lib/logger"
    "github.com/HDT3213/godis/src/redis/client"
    "github.com/HDT3213/godis/src/redis/reply"
    "runtime/debug"
    "strings"
)

type Cluster struct {
    self string

    db         *db.DB
    peerPicker *consistenthash.Map
    peers      map[string]*client.Client
}

const (
    replicas = 4
)

func MakeCluster() *Cluster {
    cluster := &Cluster{
        self: config.Properties.Self,

        db:         db.MakeDB(),
        peerPicker: consistenthash.New(replicas, nil),
        peers:      make(map[string]*client.Client),
    }
    if config.Properties.Peers != nil && len(config.Properties.Peers) > 0 && config.Properties.Self != "" {
        contains := make(map[string]bool)
        peers := make([]string, len(config.Properties.Peers)+1)[:]
        for _, peer := range config.Properties.Peers {
            if _, ok := contains[peer]; ok {
                continue
            }
            contains[peer] = true
            peers = append(peers, peer)
        }
        peers = append(peers, config.Properties.Self)
        cluster.peerPicker.Add(peers...)
    }
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
