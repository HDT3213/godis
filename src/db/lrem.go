package db

import (
    List "github.com/HDT3213/godis/src/datastruct/list"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
    "strconv"
)

func LRem(db *DB, args [][]byte)redis.Reply {
    // parse args
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'lrem' command")
    }
    key := string(args[0])
    count64, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range")
    }
    count := int(count64)
    value := args[2]

    // get data entity
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

    list, _ := entity.Data.(*List.LinkedList)
    var removed int
    if count == 0 {
        removed = list.RemoveAllByVal(value)
    } else if count > 0 {
        removed = list.RemoveByVal(value, count)
    } else {
        removed = list.ReverseRemoveByVal(value, -count)
    }

    if list.Len() == 0 {
        db.Data.Remove(key)
    }

    return reply.MakeIntReply(int64(removed))
}