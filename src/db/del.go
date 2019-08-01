package db

import (
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
)

func Del(db *DB, args [][]byte)redis.Reply {
    if len(args) == 0 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'del' command")
    }
    keys := make([]string, len(args))
    for i, v := range args {
        keys[i] = string(v)
    }

    deleted := 0
    for _, key := range keys {
        _, exists := db.Data.Get(key)
        if exists {
            db.Data.Remove(key)
            deleted++
        }
    }

    return reply.MakeIntReply(int64(deleted))
}
