package db

import (
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
)

func MGet(db *DB, args [][]byte)redis.Reply {
    if len(args) == 0 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'mget' command")
    }
    keys := make([]string, len(args))
    for i, v := range args {
        keys[i] = string(v)
    }

    result := make([][]byte, len(args))
    for i, key := range keys {
        val, exists := db.Data.Get(key)
        if !exists {
            result[i] = nil
            continue
        }
        entity, _ := val.(*DataEntity)
        if entity.Code != StringCode {
            result[i] = nil
            continue
        }
        bytes, _ := entity.Data.([]byte)
        result[i] = bytes
    }

    return reply.MakeMultiBulkReply(result)
}