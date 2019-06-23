package db

import "github.com/HDT3213/godis/src/interface/redis"

type DB interface {
    Exec([][]byte)redis.Reply
}
