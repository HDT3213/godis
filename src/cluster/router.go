package cluster

import "github.com/HDT3213/godis/src/interface/redis"

func defaultFunc(cluster *Cluster, c redis.Client, args [][]byte) redis.Reply {
    key := string(args[1])
    return cluster.Relay(key, c, args)
}

func MakeRouter() map[string]CmdFunc {
    routerMap := make(map[string]CmdFunc)
    //routerMap["ping"] = defaultFunc

    //routerMap["del"] = Del
    routerMap["expire"] = defaultFunc
    routerMap["expireat"] = defaultFunc
    routerMap["pexpire"] = defaultFunc
    routerMap["pexpireat"] = defaultFunc
    routerMap["ttl"] = defaultFunc
    routerMap["pttl"] = defaultFunc
    routerMap["persist"] = defaultFunc
    routerMap["exists"] = defaultFunc
    routerMap["type"] = defaultFunc
    //routerMap["rename"] = Rename
    //routerMap["renamenx"] = RenameNx

    routerMap["set"] = defaultFunc
    routerMap["setnx"] = defaultFunc
    routerMap["setex"] = defaultFunc
    routerMap["psetex"] = defaultFunc
    //routerMap["mset"] = MSet
    //routerMap["mget"] = MGet
    //routerMap["msetnx"] = MSetNX
    routerMap["get"] = defaultFunc
    routerMap["getset"] = defaultFunc
    routerMap["incr"] = defaultFunc
    routerMap["incrby"] = defaultFunc
    routerMap["incrbyfloat"] = defaultFunc
    routerMap["decr"] = defaultFunc
    routerMap["decrby"] = defaultFunc

    routerMap["lpush"] = defaultFunc
    routerMap["lpushx"] = defaultFunc
    routerMap["rpush"] = defaultFunc
    routerMap["rpushx"] = defaultFunc
    routerMap["lpop"] = defaultFunc
    routerMap["rpop"] = defaultFunc
    //routerMap["rpoplpush"] = RPopLPush
    routerMap["lrem"] = defaultFunc
    routerMap["llen"] = defaultFunc
    routerMap["lindex"] = defaultFunc
    routerMap["lset"] = defaultFunc
    routerMap["lrange"] = defaultFunc

    routerMap["hset"] = defaultFunc
    routerMap["hsetnx"] = defaultFunc
    routerMap["hget"] = defaultFunc
    routerMap["hexists"] = defaultFunc
    routerMap["hdel"] = defaultFunc
    routerMap["hlen"] = defaultFunc
    routerMap["hmget"] = defaultFunc
    routerMap["hmset"] = defaultFunc
    routerMap["hkeys"] = defaultFunc
    routerMap["hvals"] = defaultFunc
    routerMap["hgetall"] = defaultFunc
    routerMap["hincrby"] = defaultFunc
    routerMap["hincrbyfloat"] = defaultFunc

    routerMap["sadd"] = defaultFunc
    routerMap["sismember"] = defaultFunc
    routerMap["srem"] = defaultFunc
    routerMap["scard"] = defaultFunc
    routerMap["smembers"] = defaultFunc
    routerMap["sinter"] = defaultFunc
    routerMap["sinterstore"] = defaultFunc
    routerMap["sunion"] = defaultFunc
    routerMap["sunionstore"] = defaultFunc
    routerMap["sdiff"] = defaultFunc
    routerMap["sdiffstore"] = defaultFunc
    routerMap["srandmember"] = defaultFunc

    routerMap["zadd"] = defaultFunc
    routerMap["zscore"] = defaultFunc
    routerMap["zincrby"] = defaultFunc
    routerMap["zrank"] = defaultFunc
    routerMap["zcount"] = defaultFunc
    routerMap["zrevrank"] = defaultFunc
    routerMap["zcard"] = defaultFunc
    routerMap["zrange"] = defaultFunc
    routerMap["zrevrange"] = defaultFunc
    routerMap["zrangebyscore"] = defaultFunc
    routerMap["zrevrangebyscore"] = defaultFunc
    routerMap["zrem"] = defaultFunc
    routerMap["zremrangebyscore"] = defaultFunc
    routerMap["zremrangebyrank"] = defaultFunc

    //routerMap["flushdb"] = FlushDB
    //routerMap["flushall"] = FlushAll
    //routerMap["keys"] = Keys

    return routerMap
}
