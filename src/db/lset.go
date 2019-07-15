package db

import (
    List "github.com/HDT3213/godis/src/datastruct/list"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
    "strconv"
)

func LSet(db *DB, args [][]byte)redis.Reply {
    // parse args
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'lset' command")
    }
    key := string(args[0])
    index64, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range")
    }
    index := int(index64)
    value := args[2]

    // get data
    rawEntity, exists := db.Data.Get(key)
    var entity *DataEntity
    if !exists {
        return reply.MakeErrReply("ERR no such key")
    } else {
        entity, _ = rawEntity.(*DataEntity)
    }
    if entity.Code != ListCode {
        return &reply.WrongTypeErrReply{}
    }
    entity.Lock()
    defer entity.Unlock()

    list, _ := entity.Data.(*List.LinkedList)
    size := list.Len() // assert: size > 0
    if index < -1 * size {
        return reply.MakeErrReply("ERR index out of range")
    } else if index < 0 {
        index = size + index
    } else if index >= size {
        return reply.MakeErrReply("ERR index out of range")
    }

    list.Set(index, value)
    return &reply.OkReply{}
}