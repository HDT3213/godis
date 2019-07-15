package db

import (
    List "github.com/HDT3213/godis/src/datastruct/list"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
    "strconv"
)

func LRange(db *DB, args [][]byte)redis.Reply {
    // parse args
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'lrange' command")
    }
    key := string(args[0])
    start64, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range")
    }
    start := int(start64)
    stop64, err := strconv.ParseInt(string(args[2]), 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range")
    }
    stop := int(stop64)

    // get data
    rawEntity, exists := db.Data.Get(key)
    var entity *DataEntity
    if !exists {
        return &reply.EmptyMultiBulkReply{}
    } else {
        entity, _ = rawEntity.(*DataEntity)
    }
    if entity.Code != ListCode {
        return &reply.WrongTypeErrReply{}
    }
    entity.RLock()
    defer entity.RUnlock()

    // compute index
    list, _ := entity.Data.(*List.LinkedList)
    size := list.Len() // assert: size > 0
    if start < -1 * size {
        start = 0
    } else if start < 0 {
        start = size + start
    } else if start >= size {
        return &reply.EmptyMultiBulkReply{}
    }
    if stop < -1 * size {
        stop = 0
    } else if stop < 0 {
        stop = size + stop + 1
    } else if stop < size {
        stop = stop + 1
    } else {
        stop = size
    }
    if stop < start {
        stop = start
    }

    // assert: start in [0, size - 1], stop in [start, size]
    slice := list.Range(start, stop)
    result := make([][]byte, len(slice))
    for i, raw := range slice {
        bytes, _ := raw.([]byte)
        result[i] = bytes
    }
    return reply.MakeMultiBulkReply(result)
}
