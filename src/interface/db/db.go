package db

import "github.com/HDT3213/godis/src/interface/redis"

type DB interface {
    Exec(client redis.Connection, args [][]byte) redis.Reply
    AfterClientClose(c redis.Connection)
    Close()
}
