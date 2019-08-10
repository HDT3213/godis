package db

import (
    "fmt"
    "github.com/HDT3213/godis/src/datastruct/dict"
    List "github.com/HDT3213/godis/src/datastruct/list"
    "github.com/HDT3213/godis/src/datastruct/lock"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/lib/logger"
    "github.com/HDT3213/godis/src/redis/reply"
    "runtime/debug"
    "strings"
    "time"
)

const (
    StringCode = iota // Data is []byte
    ListCode // *list.LinkedList
    SetCode
    DictCode // *dict.Dict
    SortedSetCode
)

type DataEntity struct {
    Code uint8
    Data interface{}
}

type DataEntityWithKey struct {
    DataEntity
    Key string
}

// args don't include cmd line
type CmdFunc func(db *DB, args [][]byte)redis.Reply

type DB struct {
    // key -> DataEntity
    Data *dict.Dict
    // key -> expireTime (time.Time)
    TTLMap *dict.Dict

    // dict will ensure thread safety of its method
    // use this mutex for complicated command only, eg. rpush, incr ...
    Locks *lock.LockMap

    // TimerTask interval
    interval time.Duration
}

var cmdMap = MakeCmdMap()

func MakeCmdMap()map[string]CmdFunc {
    cmdMap := make(map[string]CmdFunc)
    cmdMap["ping"] = Ping

    cmdMap["set"] = Set
    cmdMap["setnx"] = SetNX
    cmdMap["setex"] = SetEX
    cmdMap["psetex"] = PSetEX
    cmdMap["mset"] = MSet
    cmdMap["mget"] = MGet
    cmdMap["msetnx"] = MSetNX
    cmdMap["get"] = Get
    cmdMap["del"] = Del
    cmdMap["getset"] = GetSet
    cmdMap["incr"] = Incr
    cmdMap["incrby"] = IncrBy
    cmdMap["incrbyfloat"] = IncrByFloat
    cmdMap["decr"] = Decr
    cmdMap["decrby"] = DecrBy
    cmdMap["expire"] = Expire
    cmdMap["expireat"] = ExpireAt
    cmdMap["pexpire"] = PExpire
    cmdMap["pexpireat"] = PExpireAt
    cmdMap["ttl"] = TTL
    cmdMap["pttl"] = PTTL
    cmdMap["persist"] = Persist

    cmdMap["lpush"] = LPush
    cmdMap["lpushx"] = LPushX
    cmdMap["rpush"] = RPush
    cmdMap["rpushx"] = RPushX
    cmdMap["lpop"] = LPop
    cmdMap["rpop"] = RPop
    cmdMap["rpoplpush"] = RPopLPush
    cmdMap["lrem"] = LRem
    cmdMap["llen"] = LLen
    cmdMap["lindex"] = LIndex
    cmdMap["lset"] = LSet
    cmdMap["lrange"] = LRange

    return cmdMap
}

func MakeDB() *DB {
    db := &DB{
        Data: dict.Make(1024),
        TTLMap: dict.Make(512),
        Locks: &lock.LockMap{},
        interval: 5 * time.Second,
    }
    db.TimerTask()
    return db
}

func (db *DB)Exec(args [][]byte)(result redis.Reply) {
    defer func() {
        if err := recover(); err != nil {
            logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
            result = &reply.UnknownErrReply{}
        }
    }()

    cmd := strings.ToLower(string(args[0]))
    cmdFunc, ok := cmdMap[cmd]
    if !ok {
        return reply.MakeErrReply("ERR unknown command '" + cmd + "'")
    }
    if len(args) > 1 {
        result = cmdFunc(db, args[1:])
    } else {
        result = cmdFunc(db, [][]byte{})
    }
    return
}

func (db *DB)Get(key string)(*DataEntity, bool) {
    raw, ok := db.Data.Get(key)
    if !ok {
        return nil, false
    }
    if db.IsExpired(key) {
        return nil, false
    }
    entity, _ := raw.(*DataEntity)
    return entity, true
}

func (db *DB)Expire(key string, expireTime time.Time) {
    db.TTLMap.Put(key, expireTime)
}

func (db *DB)Persist(key string) {
    db.TTLMap.Remove(key)
}

func (db *DB)IsExpired(key string)bool {
    rawExpireTime, ok:= db.TTLMap.Get(key)
    if !ok {
        return false
    }
    expireTime, _ := rawExpireTime.(time.Time)
    expired := time.Now().After(expireTime)
    if expired {
        db.Remove(key)
    }
    return expired
}

func (db *DB)Remove(key string) {
    db.Data.Remove(key)
    db.TTLMap.Remove(key)
    db.Locks.Clean(key)
}

func (db *DB)Removes(keys ...string)(deleted int) {
    db.Locks.Locks(keys...)
    defer func() {
        db.Locks.UnLocks(keys...)
        db.Locks.Cleans(keys...)
    }()
    deleted = 0
    for _, key := range keys {
        _, exists := db.Data.Get(key)
        if exists {
            db.Data.Remove(key)
            db.TTLMap.Remove(key)
            deleted++
        }
    }
    return deleted
}

func (db *DB)CleanExpired() {
    now := time.Now()
    toRemove := &List.LinkedList{}
    db.TTLMap.ForEach(func(key string, val interface{})bool {
        expireTime, _ := val.(time.Time)
        if now.After(expireTime) {
            // expired
            db.Data.Remove(key)
            db.Locks.Clean(key)
            toRemove.Add(key)
        }
        return true
    })
    toRemove.ForEach(func(i int, val interface{})bool {
        key, _ := val.(string)
        db.TTLMap.Remove(key)
        return true
    })
}

func (db *DB)TimerTask() {
    ticker := time.NewTicker(db.interval)
    go func() {
        for range ticker.C {
            db.CleanExpired()
        }
    }()
}