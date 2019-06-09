package db

import (
    "github.com/HDT3213/godis/src/redis/reply"
    "github.com/HDT3213/godis/src/interface/redis"
)

type PongReply struct {}

func (r *PongReply)ToBytes()[]byte {
    return []byte("+PONG\r\n")
}

type ArgNumErrReply struct {}

func (r *ArgNumErrReply)ToBytes()[]byte {
    return []byte("-ERR wrong number of arguments for 'ping' command\r\n")
}

func Ping(args [][]byte)redis.Reply {
    if len(args) == 0 {
        return &PongReply{}
    } else if len(args) == 1 {
        return reply.MakeStatusReply("\"" + string(args[0]) + "\"")
    } else {
        return &ArgNumErrReply{}
    }
}
