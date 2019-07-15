package db

import (
    List "github.com/HDT3213/godis/src/datastruct/list"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
)

func RPop(db *DB, args [][]byte)redis.Reply {
    // parse args
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'lindex' command")
    }
    key := string(args[0])

    // get data
    rawEntity, exists := db.Data.Get(key)
    var entity *DataEntity
    if !exists {
        return &reply.NullBulkReply{}
    } else {
        entity, _ = rawEntity.(*DataEntity)
    }
    if entity.Code != ListCode {
        return &reply.WrongTypeErrReply{}
    }
    entity.Lock()
    defer entity.Unlock()

    list, _ := entity.Data.(*List.LinkedList)
    val, _ := list.RemoveLast().([]byte)
    if list.Len() == 0 {
        db.Data.Remove(key)
    }
    return reply.MakeBulkReply(val)
}
