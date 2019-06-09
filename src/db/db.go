package db

import (
    "strings"
    "github.com/HDT3213/godis/src/redis/reply"
    "github.com/HDT3213/godis/src/db/db"
    "fmt"
    "runtime/debug"
    "github.com/HDT3213/godis/src/lib/logger"
    "github.com/HDT3213/godis/src/interface/redis"
)

// args don't include cmd line
type CmdFunc func(args [][]byte)redis.Reply


type DB struct {
    cmdMap map[string]CmdFunc
}

type UnknownErrReply struct {}

func (r *UnknownErrReply)ToBytes()[]byte {
    return []byte("-Err unknown\r\n")
}

func (db *DB)Exec(args [][]byte)(result redis.Reply) {
    defer func() {
        if err := recover(); err != nil {
            logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
            result = &UnknownErrReply{}
        }
    }()

    cmd := strings.ToLower(string(args[0]))
    cmdFunc, ok := db.cmdMap[cmd]
    if !ok {
        return reply.MakeErrReply("ERR unknown command '" + cmd + "'")
    }
    if len(args) > 1 {
        result = cmdFunc(args[1:])
    } else {
        result = cmdFunc([][]byte{})
    }
    return
}

func MakeDB() *DB {
    cmdMap := make(map[string]CmdFunc)
    cmdMap["ping"] = db.Ping

    return &DB{
        cmdMap:cmdMap,
    }
}
