package db

func MakeRouter()map[string]CmdFunc {
    routerMap := make(map[string]CmdFunc)
    routerMap["ping"] = Ping

    routerMap["del"] = Del
    routerMap["expire"] = Expire
    routerMap["expireat"] = ExpireAt
    routerMap["pexpire"] = PExpire
    routerMap["pexpireat"] = PExpireAt
    routerMap["ttl"] = TTL
    routerMap["pttl"] = PTTL
    routerMap["persist"] = Persist
    routerMap["exists"] = Exists
    routerMap["type"] = Type
    routerMap["rename"] = Rename
    routerMap["renamenx"] = RenameNx

    routerMap["set"] = Set
    routerMap["setnx"] = SetNX
    routerMap["setex"] = SetEX
    routerMap["psetex"] = PSetEX
    routerMap["mset"] = MSet
    routerMap["mget"] = MGet
    routerMap["msetnx"] = MSetNX
    routerMap["get"] = Get
    routerMap["getset"] = GetSet
    routerMap["incr"] = Incr
    routerMap["incrby"] = IncrBy
    routerMap["incrbyfloat"] = IncrByFloat
    routerMap["decr"] = Decr
    routerMap["decrby"] = DecrBy

    routerMap["lpush"] = LPush
    routerMap["lpushx"] = LPushX
    routerMap["rpush"] = RPush
    routerMap["rpushx"] = RPushX
    routerMap["lpop"] = LPop
    routerMap["rpop"] = RPop
    routerMap["rpoplpush"] = RPopLPush
    routerMap["lrem"] = LRem
    routerMap["llen"] = LLen
    routerMap["lindex"] = LIndex
    routerMap["lset"] = LSet
    routerMap["lrange"] = LRange

    routerMap["hset"] = HSet
    routerMap["hsetnx"] = HSetNX
    routerMap["hget"] = HGet
    routerMap["hexists"] = HExists
    routerMap["hdel"] = HDel
    routerMap["hlen"] = HLen
    routerMap["hmget"] = HMGet
    routerMap["hmset"] = HMSet
    routerMap["hkeys"] = HKeys
    routerMap["hvals"] = HVals
    routerMap["hgetall"] = HGetAll
    routerMap["hincrby"] = HIncrBy
    routerMap["hincrbyfloat"] = HIncrByFloat

    routerMap["sadd"] = SAdd
    routerMap["sismember"] = SIsMember
    routerMap["srem"] = SRem
    routerMap["scard"] = SCard
    routerMap["smembers"] = SMembers
    routerMap["sinter"] = SInter
    routerMap["sinterstore"] = SInterStore
    routerMap["sunion"] = SUnion
    routerMap["sunionstore"] = SUnionStore
    routerMap["sdiff"] = SDiff
    routerMap["sdiffstore"] = SDiffStore
    routerMap["srandmember"] = SRandMember

    routerMap["zadd"] = ZAdd
    routerMap["zscore"] = ZScore
    routerMap["zincrby"] = ZIncrBy
    routerMap["zrank"] = ZRank
    routerMap["zcount"] = ZCount
    routerMap["zrevrank"] = ZRevRank
    routerMap["zcard"] = ZCard
    routerMap["zrange"] = ZRange
    routerMap["zrevrange"] = ZRevRange
    routerMap["zrangebyscore"] = ZRangeByScore
    routerMap["zrevrangebyscore"] = ZRevRangeByScore
    routerMap["zrem"] = ZRem
    routerMap["zremrangebyscore"] = ZRemRangeByScore
    routerMap["zremrangebyrank"] = ZRemRangeByRank

    return routerMap
}
