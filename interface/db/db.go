package db

import "github.com/hdt3213/godis/interface/redis"

// DB is the interface for redis style storage engine
type DB interface {
	Exec(client redis.Connection, args [][]byte) redis.Reply
	AfterClientClose(c redis.Connection)
	Close()
}
