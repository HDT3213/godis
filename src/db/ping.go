package db

import (
    "github.com/HDT3213/godis/src/redis/reply"
    "github.com/HDT3213/godis/src/interface/redis"
)

func Ping(db *DB, args [][]byte)redis.Reply {
    if len(args) == 0 {
        return &reply.PongReply{}
    } else if len(args) == 1 {
        return reply.MakeStatusReply("\"" + string(args[0]) + "\"")
    } else {
        return reply.MakeErrReply("ERR wrong number of arguments for 'ping' command")
    }
}
