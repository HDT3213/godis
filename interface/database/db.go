package database

import (
	"github.com/hdt3213/godis/interface/redis"
	"time"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// DB is the interface for redis style storage engine
type DB interface {
	Exec(client redis.Connection, args [][]byte) redis.Reply
	AfterClientClose(c redis.Connection)
	Close()
}

// EmbedDB is the embedding storage engine exposing more methods for complex application
type EmbedDB interface {
	DB
	ExecWithLock(conn redis.Connection, args [][]byte) redis.Reply
	ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply
	GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine
	ForEach(dbIndex int, cb func(key string, data *DataEntity, expiration *time.Time) bool)
	RWLocks(dbIndex int, writeKeys []string, readKeys []string)
	RWUnLocks(dbIndex int, writeKeys []string, readKeys []string)
}

// DataEntity stores data bound to a key, including a string, list, hash, set and so on
type DataEntity struct {
	Data interface{}
}
