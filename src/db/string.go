package db

import (
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
    "github.com/shopspring/decimal"
    "strconv"
    "strings"
    "time"
)

func (db *DB) getAsString(key string) ([]byte, reply.ErrorReply) {
    entity, ok := db.Get(key)
    if !ok {
        return nil, nil
    }
    bytes, ok := entity.Data.([]byte)
    if !ok {
        return nil, &reply.WrongTypeErrReply{}
    }
    return bytes, nil
}

func Get(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'get' command"), nil
    }
    key := string(args[0])
    bytes, err := db.getAsString(key)
    if err != nil {
        return err, nil
    }
    if bytes == nil {
        return &reply.NullBulkReply{}, nil
    }
    return reply.MakeBulkReply(bytes), nil
}

const (
    upsertPolicy = iota // default
    insertPolicy        // set nx
    updatePolicy        // set ex
)

const unlimitedTTL int64 = 0

// SET key value [EX seconds] [PX milliseconds] [NX|XX]
func Set(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'set' command"), nil
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
                    return &reply.SyntaxErrReply{}, nil
                }
                policy = insertPolicy
            } else if arg == "XX" { // update policy
                if policy == insertPolicy {
                    return &reply.SyntaxErrReply{}, nil
                }
                policy = updatePolicy
            } else if arg == "EX" { // ttl in seconds
                if ttl != unlimitedTTL {
                    // ttl has been set
                    return &reply.SyntaxErrReply{}, nil
                }
                if i+1 >= len(args) {
                    return &reply.SyntaxErrReply{}, nil
                }
                ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
                if err != nil {
                    return &reply.SyntaxErrReply{}, nil
                }
                if ttlArg <= 0 {
                    return reply.MakeErrReply("ERR invalid expire time in set"), nil
                }
                ttl = ttlArg * 1000
                i++ // skip next arg
            } else if arg == "PX" { // ttl in milliseconds
                if ttl != unlimitedTTL {
                    return &reply.SyntaxErrReply{}, nil
                }
                if i+1 >= len(args) {
                    return &reply.SyntaxErrReply{}, nil
                }
                ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
                if err != nil {
                    return &reply.SyntaxErrReply{}, nil
                }
                if ttlArg <= 0 {
                    return reply.MakeErrReply("ERR invalid expire time in set"), nil
                }
                ttl = ttlArg
                i++ // skip next arg
            } else {
                return &reply.SyntaxErrReply{}, nil
            }
        }
    }

    entity := &DataEntity{
        Data: value,
    }

    db.Persist(key) // clean ttl
    var result int
    switch policy {
    case upsertPolicy:
        result = db.Put(key, entity)
    case insertPolicy:
        result = db.PutIfAbsent(key, entity)
    case updatePolicy:
        result = db.PutIfExists(key, entity)
    }
    extra := &extra{toPersist: result > 0}
    if result > 0 {
        if ttl != unlimitedTTL {
            expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
            db.Expire(key, expireTime)
            extra.specialAof = []*reply.MultiBulkReply{ // for aof
                reply.MakeMultiBulkReply([][]byte{
                    []byte("SET"),
                    args[0],
                    args[1],
                }),
                makeExpireCmd(key, expireTime),
            }
        } else {
            db.Persist(key) // override ttl
        }
    }
    if policy == upsertPolicy || result > 0 {
        return &reply.OkReply{}, extra
    } else {
        return &reply.NullBulkReply{}, extra
    }
}

func SetNX(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'setnx' command"), nil
    }
    key := string(args[0])
    value := args[1]
    entity := &DataEntity{
        Data: value,
    }
    result := db.PutIfAbsent(key, entity)
    return reply.MakeIntReply(int64(result)), &extra{toPersist: result > 0}
}

func SetEX(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'setex' command"), nil
    }
    key := string(args[0])
    value := args[1]

    ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return &reply.SyntaxErrReply{}, nil
    }
    if ttlArg <= 0 {
        return reply.MakeErrReply("ERR invalid expire time in setex"), nil
    }
    ttl := ttlArg * 1000

    entity := &DataEntity{
        Data: value,
    }
    result := db.PutIfExists(key, entity)
    if result > 0 && ttl != unlimitedTTL {
        expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
        db.Expire(key, expireTime)
    }
    return &reply.OkReply{}, &extra{toPersist: result > 0}
}

func PSetEX(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'psetex' command"), nil
    }
    key := string(args[0])
    value := args[1]

    ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return &reply.SyntaxErrReply{}, nil
    }
    if ttl <= 0 {
        return reply.MakeErrReply("ERR invalid expire time in psetex"), nil
    }

    entity := &DataEntity{
        Data: value,
    }
    result := db.PutIfExists(key, entity)
    if result > 0 && ttl != unlimitedTTL {
        expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
        db.Expire(key, expireTime)
    }
    return &reply.OkReply{}, &extra{toPersist: result > 0}
}

func MSet(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args)%2 != 0 || len(args) == 0 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'mset' command"), nil
    }

    size := len(args) / 2
    keys := make([]string, size)
    values := make([][]byte, size)
    for i := 0; i < size; i++ {
        keys[i] = string(args[2*i])
        values[i] = args[2*i+1]
    }

    db.Locks(keys...)
    defer db.UnLocks(keys...)

    for i, key := range keys {
        value := values[i]
        db.Put(key, &DataEntity{Data: value})
    }

    return &reply.OkReply{}, &extra{toPersist: true}
}

func MGet(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) == 0 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'mget' command"), nil
    }
    keys := make([]string, len(args))
    for i, v := range args {
        keys[i] = string(v)
    }

    result := make([][]byte, len(args))
    for i, key := range keys {
        bytes, err := db.getAsString(key)
        if err != nil {
            _, isWrongType := err.(*reply.WrongTypeErrReply)
            if isWrongType {
                result[i] = nil
                continue
            } else {
                return err, nil
            }
        }
        result[i] = bytes // nil or []byte
    }

    return reply.MakeMultiBulkReply(result), nil
}

func MSetNX(db *DB, args [][]byte) (redis.Reply, *extra) {
    // parse args
    if len(args)%2 != 0 || len(args) == 0 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'msetnx' command"), nil
    }
    size := len(args) / 2
    values := make([][]byte, size)
    keys := make([]string, size)
    for i := 0; i < size; i++ {
        keys[i] = string(args[2*i])
        values[i] = args[2*i+1]
    }

    // lock keys
    db.Locks(keys...)
    defer db.UnLocks(keys...)

    for _, key := range keys {
        _, exists := db.Get(key)
        if exists {
            return reply.MakeIntReply(0), nil
        }
    }

    for i, key := range keys {
        value := values[i]
        db.Put(key, &DataEntity{Data: value})
    }
    return reply.MakeIntReply(1), &extra{toPersist: true}
}

func GetSet(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'getset' command"), nil
    }
    key := string(args[0])
    value := args[1]

    old, err := db.getAsString(key)
    if err != nil {
        return err, nil
    }

    db.Put(key, &DataEntity{Data: value})
    db.Persist(key) // override ttl

    return reply.MakeBulkReply(old), &extra{toPersist: true}
}

func Incr(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'incr' command"), nil
    }
    key := string(args[0])

    db.Lock(key)
    defer db.UnLock(key)

    bytes, err := db.getAsString(key)
    if err != nil {
        return err, nil
    }
    if bytes != nil {
        val, err := strconv.ParseInt(string(bytes), 10, 64)
        if err != nil {
            return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
        }
        db.Put(key, &DataEntity{
            Data: []byte(strconv.FormatInt(val+1, 10)),
        })
        return reply.MakeIntReply(val + 1), &extra{toPersist: true}
    } else {
        db.Put(key, &DataEntity{
            Data: []byte("1"),
        })
        return reply.MakeIntReply(1), &extra{toPersist: true}
    }
}

func IncrBy(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'incrby' command"), nil
    }
    key := string(args[0])
    rawDelta := string(args[1])
    delta, err := strconv.ParseInt(rawDelta, 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
    }

    db.Lock(key)
    defer db.UnLock(key)

    bytes, errReply := db.getAsString(key)
    if errReply != nil {
        return errReply, nil
    }
    if bytes != nil {
        val, err := strconv.ParseInt(string(bytes), 10, 64)
        if err != nil {
            return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
        }
        db.Put(key, &DataEntity{
            Data: []byte(strconv.FormatInt(val+delta, 10)),
        })
        return reply.MakeIntReply(val + delta), &extra{toPersist: true}
    } else {
        db.Put(key, &DataEntity{
            Data: args[1],
        })
        return reply.MakeIntReply(delta), &extra{toPersist: true}
    }
}

func IncrByFloat(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'incrbyfloat' command"), nil
    }
    key := string(args[0])
    rawDelta := string(args[1])
    delta, err := decimal.NewFromString(rawDelta)
    if err != nil {
        return reply.MakeErrReply("ERR value is not a valid float"), nil
    }

    db.Lock(key)
    defer db.UnLock(key)

    bytes, errReply := db.getAsString(key)
    if errReply != nil {
        return errReply, nil
    }
    if bytes != nil {
        val, err := decimal.NewFromString(string(bytes))
        if err != nil {
            return reply.MakeErrReply("ERR value is not a valid float"), nil
        }
        resultBytes := []byte(val.Add(delta).String())
        db.Put(key, &DataEntity{
            Data: resultBytes,
        })
        return reply.MakeBulkReply(resultBytes), &extra{toPersist: true}
    } else {
        db.Put(key, &DataEntity{
            Data: args[1],
        })
        return reply.MakeBulkReply(args[1]), &extra{toPersist: true}
    }
}

func Decr(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'decr' command"), nil
    }
    key := string(args[0])

    db.Lock(key)
    defer db.UnLock(key)

    bytes, errReply := db.getAsString(key)
    if errReply != nil {
        return errReply, nil
    }
    if bytes != nil {
        val, err := strconv.ParseInt(string(bytes), 10, 64)
        if err != nil {
            return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
        }
        db.Put(key, &DataEntity{
            Data: []byte(strconv.FormatInt(val-1, 10)),
        })
        return reply.MakeIntReply(val - 1), &extra{toPersist: true}
    } else {
        entity := &DataEntity{
            Data: []byte("-1"),
        }
        db.Put(key, entity)
        return reply.MakeIntReply(-1), &extra{toPersist: true}
    }
}

func DecrBy(db *DB, args [][]byte) (redis.Reply, *extra){
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'decrby' command"), nil
    }
    key := string(args[0])
    rawDelta := string(args[1])
    delta, err := strconv.ParseInt(rawDelta, 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
    }

    db.Lock(key)
    defer db.UnLock(key)

    bytes, errReply := db.getAsString(key)
    if errReply != nil {
        return errReply, nil
    }
    if bytes != nil {
        val, err := strconv.ParseInt(string(bytes), 10, 64)
        if err != nil {
            return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
        }
        db.Put(key, &DataEntity{
            Data: []byte(strconv.FormatInt(val-delta, 10)),
        })
        return reply.MakeIntReply(val - delta), &extra{toPersist: true}
    } else {
        valueStr := strconv.FormatInt(-delta, 10)
        db.Put(key, &DataEntity{
            Data: []byte(valueStr),
        })
        return reply.MakeIntReply(-delta), &extra{toPersist: true}
    }
}
