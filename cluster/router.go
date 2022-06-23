package cluster

import "github.com/hdt3213/godis/interface/redis"

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

func makeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)
	routerMap["ping"] = ping

	routerMap["prepare"] = execPrepare
	routerMap["commit"] = execCommit
	routerMap["rollback"] = execRollback
	routerMap["del"] = Del

	routerMap["expire"] = defaultFunc
	routerMap["expireat"] = defaultFunc
	routerMap["pexpire"] = defaultFunc
	routerMap["pexpireat"] = defaultFunc
	routerMap["ttl"] = defaultFunc
	routerMap["pttl"] = defaultFunc
	routerMap["persist"] = defaultFunc
	routerMap["exists"] = defaultFunc
	routerMap["type"] = defaultFunc
	routerMap["rename"] = Rename
	routerMap["renamenx"] = RenameNx
	routerMap["copy"] = Copy

	routerMap["set"] = defaultFunc
	routerMap["setnx"] = defaultFunc
	routerMap["setex"] = defaultFunc
	routerMap["psetex"] = defaultFunc
	routerMap["mset"] = MSet
	routerMap["mget"] = MGet
	routerMap["msetnx"] = MSetNX
	routerMap["get"] = defaultFunc
	routerMap["getex"] = defaultFunc
	routerMap["getset"] = defaultFunc
	routerMap["getdel"] = defaultFunc
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
	routerMap["hstrlen"] = defaultFunc
	routerMap["hmget"] = defaultFunc
	routerMap["hmset"] = defaultFunc
	routerMap["hkeys"] = defaultFunc
	routerMap["hvals"] = defaultFunc
	routerMap["hgetall"] = defaultFunc
	routerMap["hincrby"] = defaultFunc
	routerMap["hincrbyfloat"] = defaultFunc
	routerMap["hrandfield"] = defaultFunc

	routerMap["sadd"] = defaultFunc
	routerMap["sismember"] = defaultFunc
	routerMap["srem"] = defaultFunc
	routerMap["spop"] = defaultFunc
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

	routerMap["geoadd"] = defaultFunc
	routerMap["geopos"] = defaultFunc
	routerMap["geodist"] = defaultFunc
	routerMap["geohash"] = defaultFunc
	routerMap["georadius"] = defaultFunc
	routerMap["georadiusbymember"] = defaultFunc

	routerMap["publish"] = Publish
	routerMap[relayPublish] = onRelayedPublish
	routerMap["subscribe"] = Subscribe
	routerMap["unsubscribe"] = UnSubscribe

	routerMap["flushdb"] = FlushDB
	routerMap["flushall"] = FlushAll
	routerMap[relayMulti] = execRelayedMulti
	routerMap["getver"] = defaultFunc
	routerMap["watch"] = execWatch

	return routerMap
}

// relay command to responsible peer, and return its protocol to client
func defaultFunc(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	key := string(args[1])
	peer := cluster.peerPicker.PickNode(key)
	return cluster.relay(peer, c, args)
}
