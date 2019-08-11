package db

import (
    Dict "github.com/HDT3213/godis/src/datastruct/dict"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
    "github.com/shopspring/decimal"
    "strconv"
)

func HSet(db *DB, args [][]byte)redis.Reply {
    // parse args
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hset' command")
    }
    key := string(args[0])
    field := string(args[1])
    value := args[2]

    // lock
    db.Locks.Lock(key)
    defer db.Locks.UnLock(key)

    // get or init entity
    entity, exists := db.Get(key)
    if !exists {
        entity = &DataEntity{
            Code: DictCode,
            Data: Dict.Make(0),
        }
        db.Data.Put(key, entity)
    }

    // check type
    if entity.Code != DictCode {
        return &reply.WrongTypeErrReply{}
    }

    dict, _ := entity.Data.(*Dict.Dict)
    result := dict.Put(field, value)
    return reply.MakeIntReply(int64(result))
}

func HSetNX(db *DB, args [][]byte)redis.Reply {
    // parse args
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hsetnx' command")
    }
    key := string(args[0])
    field := string(args[1])
    value := args[2]

    // lock
    db.Locks.Lock(key)
    defer db.Locks.UnLock(key)

    // get or init entity
    entity, exists := db.Get(key)
    if !exists {
        entity = &DataEntity{
            Code: DictCode,
            Data: Dict.Make(0),
        }
        db.Data.Put(key, entity)
    }

    // check type
    if entity.Code != DictCode {
        return &reply.WrongTypeErrReply{}
    }

    dict, _ := entity.Data.(*Dict.Dict)
    result := dict.PutIfAbsent(field, value)
    return reply.MakeIntReply(int64(result))
}

func HGet(db *DB, args [][]byte)redis.Reply {
    // parse args
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hget' command")
    }
    key := string(args[0])
    field := string(args[1])

    // get entity
    entity, exists := db.Get(key)
    if !exists {
        return &reply.NullBulkReply{}
    }
    // check type
    if entity.Code != DictCode {
        return &reply.WrongTypeErrReply{}
    }

    dict, _ := entity.Data.(*Dict.Dict)
    raw, exists := dict.Get(field)
    if !exists {
        return &reply.NullBulkReply{}
    }
    value, _ := raw.([]byte)
    return reply.MakeBulkReply(value)
}

func HExists(db *DB, args [][]byte)redis.Reply {
    // parse args
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hexists' command")
    }
    key := string(args[0])
    field := string(args[1])

    // get entity
    entity, exists := db.Get(key)
    if !exists {
        return reply.MakeIntReply(0)
    }
    // check type
    if entity.Code != DictCode {
        return &reply.WrongTypeErrReply{}
    }

    dict, _ := entity.Data.(*Dict.Dict)
    _, exists = dict.Get(field)
    if exists {
        return reply.MakeIntReply(1)
    }
    return reply.MakeIntReply(0)
}

func HDel(db *DB, args [][]byte)redis.Reply {
    // parse args
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hdel' command")
    }
    key := string(args[0])
    fields := make([]string, len(args) - 1)
    fieldArgs := args[1:]
    for i, v := range fieldArgs {
        fields[i] = string(v)
    }

    db.Locks.Lock(key)
    defer db.Locks.UnLock(key)

    // get entity
    entity, exists := db.Get(key)
    if !exists {
        return reply.MakeIntReply(0)
    }
    // check type
    if entity.Code != DictCode {
        return &reply.WrongTypeErrReply{}
    }

    dict, _ := entity.Data.(*Dict.Dict)
    deleted := 0
    for _, field := range fields {
        result := dict.Remove(field)
        deleted += result
    }
    if dict.Len() == 0 {
        db.Remove(key)
    }

    return reply.MakeIntReply(int64(deleted))
}

func HLen(db *DB, args [][]byte)redis.Reply {
    // parse args
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hlen' command")
    }
    key := string(args[0])

    // get entity
    entity, exists := db.Get(key)
    if !exists {
        return reply.MakeIntReply(0)
    }
    // check type
    if entity.Code != DictCode {
        return &reply.WrongTypeErrReply{}
    }

    dict, _ := entity.Data.(*Dict.Dict)
    return reply.MakeIntReply(int64(dict.Len()))
}

func HMSet(db *DB, args [][]byte)redis.Reply {
    // parse args
    if len(args) < 3 || len(args) % 2 != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hmset' command")
    }
    key := string(args[0])
    size := (len(args) - 1) / 2
    entities := make([]*DataEntityWithKey, size)
    for i := 0; i < size; i++ {
        field := string(args[2 * i + 1])
        value := args[2 * i + 2]
        entity := &DataEntityWithKey{
            DataEntity: DataEntity{
                Code: StringCode,
                Data: value,
            },
            Key: field,
        }
        entities[i] = entity
    }

    // lock key
    db.Locks.Lock(key)
    defer db.Locks.UnLock(key)

    // get or init entity
    entity, exists := db.Get(key)
    if !exists {
        entity = &DataEntity{
            Code: DictCode,
            Data: Dict.Make(0),
        }
        db.Data.Put(key, entity)
    }

    // check type
    if entity.Code != DictCode {
        return &reply.WrongTypeErrReply{}
    }

    // put data
    dict, _ := entity.Data.(*Dict.Dict)
    for _, e := range entities {
        dict.Put(e.Key, e.Data)
    }
    return &reply.OkReply{}
}

func HMGet(db *DB, args [][]byte)redis.Reply {
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hmget' command")
    }
    key := string(args[0])
    size := len(args) - 1
    fields := make([]string, size)
    for i := 0; i < size; i++ {
        fields[i] = string(args[i + 1])
    }

    db.Locks.RLock(key)
    defer db.Locks.RUnLock(key)

    // get entity
    result := make([][]byte, size)
    entity, exists := db.Get(key)
    if !exists {
        return reply.MakeMultiBulkReply(result)
    }

    // check type
    if entity.Code != DictCode {
        return &reply.WrongTypeErrReply{}
    }

    dict, _ := entity.Data.(*Dict.Dict)
    for i, field := range fields {
        value, ok := dict.Get(field)
        if !ok {
            result[i] = nil
        } else {
            bytes, _ := value.([]byte)
            result[i] = bytes
        }
    }
    return reply.MakeMultiBulkReply(result)
}

func HKeys(db *DB, args [][]byte)redis.Reply {
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hkeys' command")
    }
    key := string(args[0])

    db.Locks.RLock(key)
    defer db.Locks.RUnLock(key)

    // get entity
    entity, exists := db.Get(key)
    if !exists {
        return &reply.EmptyMultiBulkReply{}
    }

    // check type
    if entity.Code != DictCode {
        return &reply.WrongTypeErrReply{}
    }

    dict, _ := entity.Data.(*Dict.Dict)
    fields := make([][]byte, dict.Len())
    i := 0
    dict.ForEach(func(key string, val interface{})bool {
        fields[i] = []byte(key)
        i++
        return true
    })
    return reply.MakeMultiBulkReply(fields[:i])
}

func HVals(db *DB, args [][]byte)redis.Reply {
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hvals' command")
    }
    key := string(args[0])

    db.Locks.RLock(key)
    defer db.Locks.RUnLock(key)

    // get entity
    entity, exists := db.Get(key)
    if !exists {
        return &reply.EmptyMultiBulkReply{}
    }

    // check type
    if entity.Code != DictCode {
        return &reply.WrongTypeErrReply{}
    }

    dict, _ := entity.Data.(*Dict.Dict)
    values := make([][]byte, dict.Len())
    i := 0
    dict.ForEach(func(key string, val interface{})bool {
        values[i], _ = val.([]byte)
        i++
        return true
    })
    return reply.MakeMultiBulkReply(values[:i])
}

func HGetAll(db *DB, args [][]byte)redis.Reply {
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hgetAll' command")
    }
    key := string(args[0])

    db.Locks.RLock(key)
    defer db.Locks.RUnLock(key)

    // get entity
    entity, exists := db.Get(key)
    if !exists {
        return &reply.EmptyMultiBulkReply{}
    }

    // check type
    if entity.Code != DictCode {
        return &reply.WrongTypeErrReply{}
    }

    dict, _ := entity.Data.(*Dict.Dict)
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
    return reply.MakeMultiBulkReply(result[:i])
}

func HIncrBy(db *DB, args [][]byte)redis.Reply {
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hincrby' command")
    }
    key := string(args[0])
    field := string(args[1])
    rawDelta := string(args[2])
    delta, err := strconv.ParseInt(rawDelta, 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range")
    }

    db.Locks.Lock(key)
    defer db.Locks.UnLock(key)

    // get or init entity
    entity, exists := db.Get(key)
    if !exists {
        entity = &DataEntity{
            Code: DictCode,
            Data: Dict.Make(0),
        }
        db.Data.Put(key, entity)
    }

    // check type
    if entity.Code != DictCode {
        return &reply.WrongTypeErrReply{}
    }

    // put data
    dict, _ := entity.Data.(*Dict.Dict)
    value, exists := dict.Get(field)
    if !exists {
        dict.Put(field, args[2])
        return reply.MakeBulkReply(args[2])
    } else {
        val, err := strconv.ParseInt(string(value.([]byte)), 10, 64)
        if err != nil {
            return reply.MakeErrReply("ERR hash value is not an integer")
        }
        val += delta
        bytes := []byte(strconv.FormatInt(val, 10))
        dict.Put(field, bytes)
        return reply.MakeBulkReply(bytes)
    }
}

func HIncrByFloat(db *DB, args [][]byte)redis.Reply {
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'hincrbyfloat' command")
    }
    key := string(args[0])
    field := string(args[1])
    rawDelta := string(args[2])
    delta, err := decimal.NewFromString(rawDelta)
    if err != nil {
        return reply.MakeErrReply("ERR value is not a valid float")
    }

    db.Locks.Lock(key)
    defer db.Locks.UnLock(key)

    // get or init entity
    entity, exists := db.Get(key)
    if !exists {
        entity = &DataEntity{
            Code: DictCode,
            Data: Dict.Make(0),
        }
        db.Data.Put(key, entity)
    }

    // check type
    if entity.Code != DictCode {
        return &reply.WrongTypeErrReply{}
    }

    // put data
    dict, _ := entity.Data.(*Dict.Dict)
    value, exists := dict.Get(field)
    if !exists {
        dict.Put(field, args[2])
        return reply.MakeBulkReply(args[2])
    } else {
        val, err := decimal.NewFromString(string(value.([]byte)))
        if err != nil {
            return reply.MakeErrReply("ERR hash value is not a float")
        }
        result := val.Add(delta)
        resultBytes:= []byte(result.String())
        dict.Put(field, resultBytes)
        return reply.MakeBulkReply(resultBytes)
    }
}