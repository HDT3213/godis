package db

import (
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
)


func MSet(db *DB, args [][]byte)redis.Reply {
    if len(args) % 2 != 0 || len(args) == 0 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'mset' command")
    }
    size := len(args) / 2
    entities := make([]*DataEntityWithKey, size)
    for i := 0; i < size; i++ {
        key := string(args[2 * i])
        value := args[2 * i + 1]
        entity := &DataEntityWithKey{
            DataEntity: DataEntity{
                Code: StringCode,
                Data: value,
            },
            Key: key,
        }
        entities[i] = entity
    }

    for _, entity := range entities {
        db.Data.Put(entity.Key, &entity.DataEntity)
    }

    return &reply.OkReply{}
}
