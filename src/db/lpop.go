package db

import (
    List "github.com/HDT3213/godis/src/datastruct/list"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
)

func LPop(db *DB, args [][]byte)redis.Reply {
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
    entity.Lock()
    defer entity.Unlock()

    // check type
    if entity.Code != ListCode {
        return &reply.WrongTypeErrReply{}
    }

    list, _ := entity.Data.(*List.LinkedList)
    val, _ := list.Remove(0).([]byte)
    if list.Len() == 0 {
        db.Data.Remove(key)
    }
    return reply.MakeBulkReply(val)
}
