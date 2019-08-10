package db

import (
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
    "github.com/shopspring/decimal"
    "strconv"
    "strings"
    "time"
)

func Get(db *DB, args [][]byte)redis.Reply {
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'get' command")
    }
    key := string(args[0])
    entity, ok := db.Get(key)
    if !ok {
        return &reply.NullBulkReply{}
    }
    if entity.Code == StringCode {
        bytes, ok := entity.Data.([]byte)
        if !ok {
            return &reply.UnknownErrReply{}
        }
        return reply.MakeBulkReply(bytes)
    } else {
        return &reply.WrongTypeErrReply{}
    }
}

const (
    upsertPolicy = iota // default
    insertPolicy // set nx
    updatePolicy // set ex
)

const unlimitedTTL int64 = 0

// SET key value [EX seconds] [PX milliseconds] [NX|XX]
func Set(db *DB, args [][]byte)redis.Reply {
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'set' command")
    }
    key := string(args[0])
    value := args[1]
    policy := upsertPolicy
    ttl := unlimitedTTL

    // parse options
    if len(args) > 2 {
        for i := 2; i < len(args); i++ {
            arg := strings.ToUpper(string(args[i]))
            if arg == "NX" { // insert
                if policy == updatePolicy {
                    return &reply.SyntaxErrReply{}
                }
                policy = insertPolicy
            } else if arg == "XX" { // update policy
                if policy == insertPolicy {
                    return &reply.SyntaxErrReply{}
                }
                policy = updatePolicy
            } else if arg == "EX" { // ttl in seconds
                if ttl != unlimitedTTL {
                    // ttl has been set
                    return &reply.SyntaxErrReply{}
                }
                if i + 1 >= len(args) {
                    return &reply.SyntaxErrReply{}
                }
                ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
                if err != nil {
                    return &reply.SyntaxErrReply{}
                }
                if ttlArg <= 0 {
                    return reply.MakeErrReply("ERR invalid expire time in set")
                }
                ttl = ttlArg * 1000
                i++ // skip next arg
            } else if arg == "PX" { // ttl in milliseconds
                if ttl != unlimitedTTL {
                    return &reply.SyntaxErrReply{}
                }
                if i + 1 >= len(args) {
                    return &reply.SyntaxErrReply{}
                }
                ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
                if err != nil {
                    return &reply.SyntaxErrReply{}
                }
                if ttlArg <= 0 {
                    return reply.MakeErrReply("ERR invalid expire time in set")
                }
                ttl = ttlArg
                i++ // skip next arg
            } else {
                return &reply.SyntaxErrReply{}
            }
        }
    }


    entity := &DataEntity{
        Code: StringCode,
        Data: value,
    }

    switch policy {
    case upsertPolicy:
        db.Data.Put(key, entity)
    case insertPolicy:
        db.Data.PutIfAbsent(key, entity)
    case updatePolicy:
        db.Data.PutIfExists(key, entity)
    }
    if ttl != unlimitedTTL {
        expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
        db.Expire(key, expireTime)
    } else {
        db.TTLMap.Remove(key) // override ttl
    }

    return &reply.OkReply{}
}

func SetNX(db *DB, args [][]byte)redis.Reply {
    if len(args) != 2 {
        reply.MakeErrReply("ERR wrong number of arguments for 'setnx' command")
    }
    key := string(args[0])
    value := args[1]
    entity := &DataEntity{
        Code: StringCode,
        Data: value,
    }
    result := db.Data.PutIfAbsent(key, entity)
    return reply.MakeIntReply(int64(result))
}

func SetEX(db *DB, args [][]byte)redis.Reply {
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'setex' command")
    }
    key := string(args[0])
    value := args[1]

    ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return &reply.SyntaxErrReply{}
    }
    if ttlArg <= 0 {
        return reply.MakeErrReply("ERR invalid expire time in setex")
    }
    ttl := ttlArg * 1000

    entity := &DataEntity{
        Code: StringCode,
        Data: value,
    }
    if db.Data.PutIfExists(key, entity) > 0 && ttl != unlimitedTTL{
        expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
        db.Expire(key, expireTime)
    }
    return &reply.OkReply{}
}

func PSetEX(db *DB, args [][]byte)redis.Reply {
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'psetex' command")
    }
    key := string(args[0])
    value := args[1]

    ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return &reply.SyntaxErrReply{}
    }
    if ttl <= 0 {
        return reply.MakeErrReply("ERR invalid expire time in psetex")
    }

    entity := &DataEntity{
        Code: StringCode,
        Data: value,
    }
    if db.Data.PutIfExists(key, entity) > 0 && ttl != unlimitedTTL{
        expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
        db.Expire(key, expireTime)
    }
    return &reply.OkReply{}
}

func MSet(db *DB, args [][]byte)redis.Reply {
    if len(args) % 2 != 0 || len(args) == 0 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'mset' command")
    }
    size := len(args) / 2
    entities := make([]*DataEntityWithKey, size)
    for i := 0; i < size; i++ {
        key := string(args[2 * i])
        value := args[2 * i + 1]
        entity := &DataEntityWithKey{
            DataEntity: DataEntity{
                Code: StringCode,
                Data: value,
            },
            Key: key,
        }
        entities[i] = entity
    }

    for _, entity := range entities {
        db.Data.Put(entity.Key, &entity.DataEntity)
    }

    return &reply.OkReply{}
}

func MGet(db *DB, args [][]byte)redis.Reply {
    if len(args) == 0 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'mget' command")
    }
    keys := make([]string, len(args))
    for i, v := range args {
        keys[i] = string(v)
    }

    result := make([][]byte, len(args))
    for i, key := range keys {
        entity, exists := db.Get(key)
        if !exists {
            result[i] = nil
            continue
        }
        if entity.Code != StringCode {
            result[i] = nil
            continue
        }
        bytes, _ := entity.Data.([]byte)
        result[i] = bytes
    }

    return reply.MakeMultiBulkReply(result)
}

func MSetNX(db *DB, args [][]byte)redis.Reply {
    // parse args
    if len(args) % 2 != 0 || len(args) == 0 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'msetnx' command")
    }
    size := len(args) / 2
    entities := make([]*DataEntityWithKey, size)
    keys := make([]string, size)
    for i := 0; i < size; i++ {
        key := string(args[2 * i])
        value := args[2 * i + 1]
        entity := &DataEntityWithKey{
            DataEntity: DataEntity{
                Code: StringCode,
                Data: value,
            },
            Key: key,
        }
        entities[i] = entity
        keys[i] = key
    }

    // lock keys
    db.Locks.Locks(keys...)
    defer db.Locks.UnLocks(keys...)

    for _, key := range keys {
        _, exists := db.Get(key)
        if exists {
            return reply.MakeIntReply(0)
        }
    }

    for _, entity := range entities {
        db.Data.Put(entity.Key, &entity.DataEntity)
    }

    return reply.MakeIntReply(1)
}

func GetSet(db *DB, args [][]byte)redis.Reply {
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'getset' command")
    }
    key := string(args[0])
    value := args[1]

    entity, exists := db.Get(key)
    var old []byte = nil
    if exists {
        if entity.Code != StringCode {
            return &reply.WrongTypeErrReply{}
        }
        old, _ = entity.Data.([]byte)
    }

    entity = &DataEntity{
        Code: StringCode,
        Data: value,
    }
    db.Data.Put(key, entity)
    db.TTLMap.Remove(key) // override ttl

    return reply.MakeBulkReply(old)
}

func Incr(db *DB, args [][]byte)redis.Reply {
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'incr' command")
    }
    key := string(args[0])

    db.Locks.Lock(key)
    defer db.Locks.UnLock(key)

    entity, exists := db.Get(key)
    if exists {
        if entity.Code != StringCode {
            return &reply.WrongTypeErrReply{}
        }
        bytes, _ := entity.Data.([]byte)
        val, err := strconv.ParseInt(string(bytes), 10, 64)
        if err != nil {
            return reply.MakeErrReply("ERR value is not an integer or out of range")
        }
        entity.Data = []byte(strconv.FormatInt(val + 1, 10))
        return reply.MakeIntReply(val + 1)
    } else {
        entity := &DataEntity{
            Code: StringCode,
            Data: []byte("1"),
        }
        db.Data.Put(key, entity)
        return reply.MakeIntReply(1)
    }
}

func IncrBy(db *DB, args [][]byte)redis.Reply {
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'incrby' command")
    }
    key := string(args[0])
    rawDelta := string(args[1])
    delta, err := strconv.ParseInt(rawDelta, 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range")
    }

    db.Locks.Lock(key)
    defer db.Locks.UnLock(key)

    entity, exists := db.Get(key)
    if exists {
        if entity.Code != StringCode {
            return &reply.WrongTypeErrReply{}
        }
        bytes, _ := entity.Data.([]byte)
        val, err := strconv.ParseInt(string(bytes), 10, 64)
        if err != nil {
            return reply.MakeErrReply("ERR value is not an integer or out of range")
        }
        entity.Data = []byte(strconv.FormatInt(val + delta, 10))
        return reply.MakeIntReply(val + delta)
    } else {
        entity := &DataEntity{
            Code: StringCode,
            Data: args[1],
        }
        db.Data.Put(key, entity)
        return reply.MakeIntReply(delta)
    }
}

func IncrByFloat(db *DB, args [][]byte)redis.Reply {
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'incrbyfloat' command")
    }
    key := string(args[0])
    rawDelta := string(args[1])
    delta, err := decimal.NewFromString(rawDelta)
    if err != nil {
        return reply.MakeErrReply("ERR value is not a valid float")
    }

    db.Locks.Lock(key)
    defer db.Locks.UnLock(key)

    entity, exists := db.Get(key)
    if exists {
        if entity.Code != StringCode {
            return &reply.WrongTypeErrReply{}
        }
        bytes, _ := entity.Data.([]byte)
        val, err := decimal.NewFromString(string(bytes))
        if err != nil {
            return reply.MakeErrReply("ERR value is not a valid float")
        }
        result := val.Add(delta)
        resultBytes:= []byte(result.String())
        entity.Data = resultBytes
        return reply.MakeBulkReply(resultBytes)
    } else {
        entity := &DataEntity{
            Code: StringCode,
            Data: args[1],
        }
        db.Data.Put(key, entity)
        return reply.MakeBulkReply(args[1])
    }
}

func Decr(db *DB, args [][]byte)redis.Reply {
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'decr' command")
    }
    key := string(args[0])

    db.Locks.Lock(key)
    defer db.Locks.UnLock(key)

    entity, exists := db.Get(key)
    if exists {
        if entity.Code != StringCode {
            return &reply.WrongTypeErrReply{}
        }
        bytes, _ := entity.Data.([]byte)
        val, err := strconv.ParseInt(string(bytes), 10, 64)
        if err != nil {
            return reply.MakeErrReply("ERR value is not an integer or out of range")
        }
        entity.Data = []byte(strconv.FormatInt(val - 1, 10))
        return reply.MakeIntReply(val - 1)
    } else {
        entity := &DataEntity{
            Code: StringCode,
            Data: []byte("-1"),
        }
        db.Data.Put(key, entity)
        return reply.MakeIntReply(-1)
    }
}

func DecrBy(db *DB, args [][]byte)redis.Reply {
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'decrby' command")
    }
    key := string(args[0])
    rawDelta := string(args[1])
    delta, err := strconv.ParseInt(rawDelta, 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range")
    }

    db.Locks.Lock(key)
    defer db.Locks.UnLock(key)

    entity, exists := db.Get(key)
    if exists {
        if entity.Code != StringCode {
            return &reply.WrongTypeErrReply{}
        }
        bytes, _ := entity.Data.([]byte)
        val, err := strconv.ParseInt(string(bytes), 10, 64)
        if err != nil {
            return reply.MakeErrReply("ERR value is not an integer or out of range")
        }
        entity.Data = []byte(strconv.FormatInt(val - delta, 10))
        return reply.MakeIntReply(val - delta)
    } else {
        valueStr := strconv.FormatInt(-delta, 10)
        entity := &DataEntity{
            Code: StringCode,
            Data: []byte(valueStr),
        }
        db.Data.Put(key, entity)
        return reply.MakeIntReply(-delta)
    }
}