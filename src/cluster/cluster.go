package cluster

import (
    "fmt"
    "github.com/HDT3213/godis/src/cluster/idgenerator"
    "github.com/HDT3213/godis/src/config"
    "github.com/HDT3213/godis/src/datastruct/dict"
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

    peerPicker *consistenthash.Map
    peers      map[string]*client.Client

    db           *db.DB
    transactions *dict.SimpleDict // id -> Transaction

    idGenerator *idgenerator.IdGenerator
}

const (
    replicas = 4
    lockSize = 64
)

func MakeCluster() *Cluster {
    cluster := &Cluster{
        self: config.Properties.Self,

        db:           db.MakeDB(),
        transactions: dict.MakeSimple(),
        peerPicker:   consistenthash.New(replicas, nil),
        peers:        make(map[string]*client.Client),

        idGenerator: idgenerator.MakeGenerator("godis", config.Properties.Self),
    }
    if config.Properties.Peers != nil && len(config.Properties.Peers) > 0 && config.Properties.Self != "" {
        contains := make(map[string]bool)
        peers := make([]string, 0, len(config.Properties.Peers)+1)
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

func (cluster *Cluster) getPeerClient(peer string) (*client.Client, error) {
    peerClient, ok := cluster.peers[peer]
    // lazy init
    if !ok {
        var err error
        peerClient, err = client.MakeClient(peer)
        if err != nil {
            return nil, err
        }
        peerClient.Start()
        cluster.peers[peer] = peerClient
    }
    return peerClient, nil
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

// relay command to peer
// cannot call Prepare, Commit, Rollback of self node
func (cluster *Cluster) Relay(peer string, c redis.Connection, args [][]byte) redis.Reply {
    if peer == cluster.self {
        // to self db
        return cluster.db.Exec(c, args)
    } else {
        peerClient, err := cluster.getPeerClient(peer)
        if err != nil {
            return reply.MakeErrReply(err.Error())
        }
        return peerClient.Send(args)
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
        peer := cluster.peerPicker.Get(key)
        group, ok := result[peer]
        if !ok {
            group = make([]string, 0)
        }
        group = append(group, key)
        result[peer] = group
    }
    return result
}