package db

import (
    Dict "github.com/HDT3213/godis/src/datastruct/dict"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
    "github.com/shopspring/decimal"
    "strconv"
)

func (db *DB)getAsDict(key string)(*Dict.Dict, reply.ErrorReply) {
    entity, exists := db.Get(key)
    if !exists {
        return nil, nil
    }
    dict, ok := entity.Data.(*Dict.Dict)
    if !ok {
        return nil, &reply.WrongTypeErrReply{}
    }
    return dict, nil
}

func (db *DB) getOrInitDict(key string)(dict *Dict.Dict, inited bool, errReply reply.ErrorReply) {
    dict, errReply = db.getAsDict(key)
    if errReply != nil {
        return nil, false, errReply
    }
    inited = false
    if dict == nil {
        dict = Dict.Make(1)
        db.Put(key, &DataEntity{
            Data: dict,
        })
        inited = true
    }
    return dict, inited, nil
}

func HSet(db *DB, args [][]byte) (redis.Reply, *extra) {
    // parse args
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hset' command"), nil
    }
    key := string(args[0])
    field := string(args[1])
    value := args[2]

    // lock
    db.Locker.Lock(key)
    defer db.Locker.UnLock(key)

    // get or init entity
    dict, _, errReply := db.getOrInitDict(key)
    if errReply != nil {
        return errReply, nil
    }

    result := dict.Put(field, value)
    return reply.MakeIntReply(int64(result)), &extra{toPersist: true}
}

func HSetNX(db *DB, args [][]byte) (redis.Reply, *extra) {
    // parse args
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hsetnx' command"), nil
    }
    key := string(args[0])
    field := string(args[1])
    value := args[2]

    db.Lock(key)
    defer db.UnLock(key)

    dict, _, errReply := db.getOrInitDict(key)
    if errReply != nil {
        return errReply, nil
    }

    result := dict.PutIfAbsent(field, value)
    return reply.MakeIntReply(int64(result)), &extra{toPersist: result > 0}
}

func HGet(db *DB, args [][]byte) (redis.Reply, *extra) {
    // parse args
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hget' command"), nil
    }
    key := string(args[0])
    field := string(args[1])

    // get entity
    dict, errReply := db.getAsDict(key)
    if errReply != nil {
        return errReply, nil
    }
    if dict == nil {
        return &reply.NullBulkReply{}, nil
    }

    raw, exists := dict.Get(field)
    if !exists {
        return &reply.NullBulkReply{}, nil
    }
    value, _ := raw.([]byte)
    return reply.MakeBulkReply(value), nil
}

func HExists(db *DB, args [][]byte) (redis.Reply, *extra) {
    // parse args
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hexists' command"), nil
    }
    key := string(args[0])
    field := string(args[1])

    // get entity
    dict, errReply := db.getAsDict(key)
    if errReply != nil {
        return errReply, nil
    }
    if dict == nil {
        return reply.MakeIntReply(0), nil
    }

    _, exists := dict.Get(field)
    if exists {
        return reply.MakeIntReply(1), nil
    }
    return reply.MakeIntReply(0), nil
}

func HDel(db *DB, args [][]byte) (redis.Reply, *extra) {
    // parse args
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hdel' command"), nil
    }
    key := string(args[0])
    fields := make([]string, len(args) - 1)
    fieldArgs := args[1:]
    for i, v := range fieldArgs {
        fields[i] = string(v)
    }

    db.Locker.Lock(key)
    defer db.Locker.UnLock(key)

    // get entity
    dict, errReply := db.getAsDict(key)
    if errReply != nil {
        return errReply, nil
    }
    if dict == nil {
        return reply.MakeIntReply(0), nil
    }

    deleted := 0
    for _, field := range fields {
        result := dict.Remove(field)
        deleted += result
    }
    if dict.Len() == 0 {
        db.Remove(key)
    }

    return reply.MakeIntReply(int64(deleted)), &extra{toPersist: deleted > 0}
}

func HLen(db *DB, args [][]byte) (redis.Reply, *extra) {
    // parse args
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hlen' command"), nil
    }
    key := string(args[0])

    dict, errReply := db.getAsDict(key)
    if errReply != nil {
        return errReply, nil
    }
    if dict == nil {
        return reply.MakeIntReply(0), nil
    }
    return reply.MakeIntReply(int64(dict.Len())), nil
}

func HMSet(db *DB, args [][]byte) (redis.Reply, *extra) {
    // parse args
    if len(args) < 3 || len(args) % 2 != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hmset' command"), nil
    }
    key := string(args[0])
    size := (len(args) - 1) / 2
    fields := make([]string, size)
    values := make([][]byte, size)
    for i := 0; i < size; i++ {
        fields[i] = string(args[2 * i + 1])
        values[i] = args[2 * i + 2]
    }

    // lock key
    db.Locker.Lock(key)
    defer db.Locker.UnLock(key)

    // get or init entity
    dict, _, errReply := db.getOrInitDict(key)
    if errReply != nil {
        return errReply, nil
    }

    // put data
    for i, field := range fields {
        value := values[i]
        dict.Put(field, value)
    }
    return &reply.OkReply{}, &extra{toPersist: true}
}

func HMGet(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hmget' command"), nil
    }
    key := string(args[0])
    size := len(args) - 1
    fields := make([]string, size)
    for i := 0; i < size; i++ {
        fields[i] = string(args[i + 1])
    }

    db.Locker.RLock(key)
    defer db.Locker.RUnLock(key)

    // get entity
    result := make([][]byte, size)
    dict, errReply := db.getAsDict(key)
    if errReply != nil {
        return errReply, nil
    }
    if dict == nil {
        return reply.MakeMultiBulkReply(result), nil
    }

    for i, field := range fields {
        value, ok := dict.Get(field)
        if !ok {
            result[i] = nil
        } else {
            bytes, _ := value.([]byte)
            result[i] = bytes
        }
    }
    return reply.MakeMultiBulkReply(result), nil
}

func HKeys(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hkeys' command"), nil
    }
    key := string(args[0])

    db.Locker.RLock(key)
    defer db.Locker.RUnLock(key)

    dict, errReply := db.getAsDict(key)
    if errReply != nil {
        return errReply, nil
    }
    if dict == nil {
        return &reply.EmptyMultiBulkReply{}, nil
    }

    fields := make([][]byte, dict.Len())
    i := 0
    dict.ForEach(func(key string, val interface{})bool {
        fields[i] = []byte(key)
        i++
        return true
    })
    return reply.MakeMultiBulkReply(fields[:i]), nil
}

func HVals(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hvals' command"), nil
    }
    key := string(args[0])

    db.Locker.RLock(key)
    defer db.Locker.RUnLock(key)

    // get entity
    dict, errReply := db.getAsDict(key)
    if errReply != nil {
        return errReply, nil
    }
    if dict == nil {
        return &reply.EmptyMultiBulkReply{}, nil
    }

    values := make([][]byte, dict.Len())
    i := 0
    dict.ForEach(func(key string, val interface{})bool {
        values[i], _ = val.([]byte)
        i++
        return true
    })
    return reply.MakeMultiBulkReply(values[:i]), nil
}

func HGetAll(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hgetAll' command"), nil
    }
    key := string(args[0])

    db.Locker.RLock(key)
    defer db.Locker.RUnLock(key)

    // get entity
    dict, errReply := db.getAsDict(key)
    if errReply != nil {
        return errReply, nil
    }
    if dict == nil {
        return &reply.EmptyMultiBulkReply{}, nil
    }

    size := dict.Len()
    result := make([][]byte, size * 2)
    i := 0
    dict.ForEach(func(key string, val interface{})bool {
        result[i] = []byte(key)
        i++
        result[i], _ = val.([]byte)
        i++
        return true
    })
    return reply.MakeMultiBulkReply(result[:i]), nil
}

func HIncrBy(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hincrby' command"), nil
    }
    key := string(args[0])
    field := string(args[1])
    rawDelta := string(args[2])
    delta, err := strconv.ParseInt(rawDelta, 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
    }

    db.Locker.Lock(key)
    defer db.Locker.UnLock(key)

    dict, _, errReply := db.getOrInitDict(key)
    if errReply != nil {
        return errReply, nil
    }

    value, exists := dict.Get(field)
    if !exists {
        dict.Put(field, args[2])
        return reply.MakeBulkReply(args[2]), nil
    } else {
        val, err := strconv.ParseInt(string(value.([]byte)), 10, 64)
        if err != nil {
            return reply.MakeErrReply("ERR hash value is not an integer"), nil
        }
        val += delta
        bytes := []byte(strconv.FormatInt(val, 10))
        dict.Put(field, bytes)
        return reply.MakeBulkReply(bytes), &extra{toPersist: true}
    }
}

func HIncrByFloat(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hincrbyfloat' command"), nil
    }
    key := string(args[0])
    field := string(args[1])
    rawDelta := string(args[2])
    delta, err := decimal.NewFromString(rawDelta)
    if err != nil {
        return reply.MakeErrReply("ERR value is not a valid float"), nil
    }

    db.Locker.Lock(key)
    defer db.Locker.UnLock(key)

    // get or init entity
    dict, _, errReply := db.getOrInitDict(key)
    if errReply != nil {
        return errReply, nil
    }

    value, exists := dict.Get(field)
    if !exists {
        dict.Put(field, args[2])
        return reply.MakeBulkReply(args[2]), nil
    } else {
        val, err := decimal.NewFromString(string(value.([]byte)))
        if err != nil {
            return reply.MakeErrReply("ERR hash value is not a float"), nil
        }
        result := val.Add(delta)
        resultBytes:= []byte(result.String())
        dict.Put(field, resultBytes)
        return reply.MakeBulkReply(resultBytes), &extra{toPersist: true}
    }
}