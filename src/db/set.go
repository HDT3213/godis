package db

import (
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
    "strconv"
    "strings"
)

const (
    upsertPolicy = iota // default
    insertPolicy // set nx
    updatePolicy // set ex
)

const unlimitedTTL int64 = 0

// SET key value [EX seconds] [PX milliseconds] [NX|XX]
func Set(db *DB, args [][]byte)redis.Reply {
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'set' command")
    }
    key := string(args[0])
    value := args[1]
    policy := upsertPolicy
    ttl := unlimitedTTL

    // parse options
    if len(args) > 2 {
        for i := 2; i < len(args); i++ {
            arg := strings.ToUpper(string(args[i]))
            if arg == "NX" { // insert
                if policy == updatePolicy {
                    return &reply.SyntaxErrReply{}
                }
                policy = insertPolicy
            } else if arg == "XX" { // update policy
                if policy == insertPolicy {
                    return &reply.SyntaxErrReply{}
                }
                policy = updatePolicy
            } else if arg == "EX" { // ttl in seconds
                if ttl != unlimitedTTL {
                    return &reply.SyntaxErrReply{}
                }
                if i + 1 >= len(args) {
                    return &reply.SyntaxErrReply{}
                }
                ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
                if err != nil {
                    return &reply.SyntaxErrReply{}
                }
                if ttlArg <= 0 {
                    return reply.MakeErrReply("ERR invalid expire time in set")
                }
                ttl = ttlArg * 1000
                i++ // skip next arg
            } else if arg == "PX" { // ttl in milliseconds
                if ttl != unlimitedTTL {
                    return &reply.SyntaxErrReply{}
                }
                if i + 1 >= len(args) {
                    return &reply.SyntaxErrReply{}
                }
                ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
                if err != nil {
                    return &reply.SyntaxErrReply{}
                }
                if ttlArg <= 0 {
                    return reply.MakeErrReply("ERR invalid expire time in set")
                }
                ttl = ttlArg
                i++ // skip next arg
            } else {
                return &reply.SyntaxErrReply{}
            }
        }
    }


    entity := &DataEntity{
        Code: StringCode,
        TTL: ttl,
        Data: value,
    }

    switch policy {
    case upsertPolicy:
        db.Data.Put(key, entity)
    case insertPolicy:
        db.Data.PutIfAbsent(key, entity)
    case updatePolicy:
        db.Data.PutIfExists(key, entity)
    }
    return &reply.OkReply{}
}

func SetNX(db *DB, args [][]byte)redis.Reply {
    if len(args) != 2 {
        reply.MakeErrReply("ERR wrong number of arguments for 'setnx' command")
    }
    key := string(args[0])
    value := args[1]
    entity := &DataEntity{
        Code: StringCode,
        Data: value,
    }
    result := db.Data.PutIfAbsent(key, entity)
    return reply.MakeIntReply(int64(result))
}

func SetEX(db *DB, args [][]byte)redis.Reply {
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'setex' command")
    }
    key := string(args[0])
    value := args[1]

    ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return &reply.SyntaxErrReply{}
    }
    if ttlArg <= 0 {
        return reply.MakeErrReply("ERR invalid expire time in setex")
    }
    ttl := ttlArg * 1000

    entity := &DataEntity{
        Code: StringCode,
        TTL: ttl,
        Data: value,
    }
    db.Data.PutIfExists(key, entity)
    return &reply.OkReply{}
}

func PSetEX(db *DB, args [][]byte)redis.Reply {
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'psetex' command")
    }
    key := string(args[0])
    value := args[1]

    ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return &reply.SyntaxErrReply{}
    }
    if ttl <= 0 {
        return reply.MakeErrReply("ERR invalid expire time in psetex")
    }

    entity := &DataEntity{
        Code: StringCode,
        TTL: ttl,
        Data: value,
    }
    db.Data.PutIfExists(key, entity)
    return &reply.OkReply{}
}