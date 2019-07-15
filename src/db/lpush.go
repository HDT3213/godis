package db

import (
    List "github.com/HDT3213/godis/src/datastruct/list"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
)

func LPush(db *DB, args [][]byte)redis.Reply {
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'lpush' command")
    }
    key := string(args[0])
    values := args[1:]

    // get or init entity
    rawEntity, exists := db.Data.Get(key)
    var entity *DataEntity
    if !exists {
        entity = &DataEntity{
            Code: ListCode,
            Data: &List.LinkedList{},
        }
    } else {
        entity, _ = rawEntity.(*DataEntity)
    }
    entity.Lock()
    defer entity.Unlock()

    if entity.Code != ListCode {
        return &reply.WrongTypeErrReply{}
    }

    // insert
    list, _ := entity.Data.(*List.LinkedList)
    for _, value := range values {
        list.Insert(0, value)
    }
    db.Data.Put(key, entity)

    return reply.MakeIntReply(int64(list.Len()))
}

func LPushX(db *DB, args [][]byte)redis.Reply {
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'lpush' command")
    }
    key := string(args[0])
    values := args[1:]

    // get or init entity
    rawEntity, exists := db.Data.Get(key)
    var entity *DataEntity
    if !exists {
        return reply.MakeIntReply(0)
    } else {
        entity, _ = rawEntity.(*DataEntity)
    }
    if entity.Code != ListCode {
        return &reply.WrongTypeErrReply{}
    }
    entity.Lock()
    defer entity.Unlock()

    // insert
    list, _ := entity.Data.(*List.LinkedList)
    for _, value := range values {
        list.Insert(0, value)
    }
    db.Data.Put(key, entity)

    return reply.MakeIntReply(int64(list.Len()))
}