package db

import (
    List "github.com/HDT3213/godis/src/datastruct/list"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
)

func RPopLPush(db *DB, args [][]byte)redis.Reply {
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'rpoplpush' command")
    }
    sourceKey := string(args[0])
    destKey := string(args[1])

    // get source entity
    rawEntity, exists := db.Data.Get(sourceKey)
    var sourceEntity *DataEntity
    if !exists {
        return &reply.NullBulkReply{}
    } else {
        sourceEntity, _ = rawEntity.(*DataEntity)
    }
    sourceList, _ := sourceEntity.Data.(*List.LinkedList)
    sourceEntity.Lock()
    defer sourceEntity.Unlock()

    // get dest entity
    rawEntity, exists = db.Data.Get(destKey)
    var destEntity *DataEntity
    if !exists {
        destEntity = &DataEntity{
            Code: ListCode,
            Data: &List.LinkedList{},
        }
        db.Data.Put(destKey, destEntity)
    } else {
        destEntity, _ = rawEntity.(*DataEntity)
    }
    destList, _ := destEntity.Data.(*List.LinkedList)
    destEntity.Lock()
    defer destEntity.Unlock()

    // pop and push
    val, _ := sourceList.RemoveLast().([]byte)
    destList.Insert(0, val)

    return reply.MakeBulkReply(val)
}
