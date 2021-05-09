package db

import (
	"fmt"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/datastruct/lock"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/timewheel"
	"github.com/hdt3213/godis/pubsub"
	"github.com/hdt3213/godis/redis/reply"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

// DataEntity stores data bound to a key, may be a string, list, hash, set and so on
type DataEntity struct {
	Data interface{}
}

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
	lockerSize   = 128
	aofQueueSize = 1 << 16
)

// args don't include cmd line
type cmdFunc func(db *DB, args [][]byte) redis.Reply

// DB stores data and execute user's commands
type DB struct {
	// key -> DataEntity
	data dict.Dict
	// key -> expireTime (time.Time)
	ttlMap dict.Dict

	// dict.Dict will ensure concurrent-safety of its method
	// use this mutex for complicated command only, eg. rpush, incr ...
	locker *lock.Locks
	// stop all data access for FlushDB
	stopWorld sync.WaitGroup
	// handle publish/subscribe
	hub *pubsub.Hub

	// main goroutine send commands to aof goroutine through aofChan
	aofChan     chan *reply.MultiBulkReply
	aofFile     *os.File
	aofFilename string
	// aof goroutine will send msg to main goroutine through this channel when aof tasks finished and ready to shutdown
	aofFinished chan struct{}
	// buffer commands received during aof rewrite progress
	aofRewriteBuffer chan *reply.MultiBulkReply
	// pause aof for start/finish aof rewrite progress
	pausingAof sync.RWMutex
}

var router = makeRouter()

// MakeDB create DB instance and start it
func MakeDB() *DB {
	db := &DB{
		data:   dict.MakeConcurrent(dataDictSize),
		ttlMap: dict.MakeConcurrent(ttlDictSize),
		locker: lock.Make(lockerSize),
		hub:    pubsub.MakeHub(),
	}

	// aof
	if config.Properties.AppendOnly {
		db.aofFilename = config.Properties.AppendFilename
		db.loadAof(0)
		aofFile, err := os.OpenFile(db.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			logger.Warn(err)
		} else {
			db.aofFile = aofFile
			db.aofChan = make(chan *reply.MultiBulkReply, aofQueueSize)
		}
		db.aofFinished = make(chan struct{})
		go func() {
			db.handleAof()
		}()
	}
	return db
}

// Close graceful shutdown database
func (db *DB) Close() {
	if db.aofFile != nil {
		close(db.aofChan)
		<-db.aofFinished // wait for aof finished
		err := db.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
}

// Exec execute command
// parameter `args` is a RESP message including command and its params
func (db *DB) Exec(c redis.Connection, args [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	cmd := strings.ToLower(string(args[0]))
	if cmd == "auth" {
		return Auth(db, c, args[1:])
	}
	if !isAuthenticated(c) {
		return reply.MakeErrReply("NOAUTH Authentication required")
	}
	// special commands
	if cmd == "subscribe" {
		if len(args) < 2 {
			return &reply.ArgNumErrReply{Cmd: "subscribe"}
		}
		return pubsub.Subscribe(db.hub, c, args[1:])
	} else if cmd == "publish" {
		return pubsub.Publish(db.hub, args[1:])
	} else if cmd == "unsubscribe" {
		return pubsub.UnSubscribe(db.hub, c, args[1:])
	} else if cmd == "bgrewriteaof" {
		// aof.go imports router.go, router.go cannot import BGRewriteAOF from aof.go
		return BGRewriteAOF(db, args[1:])
	}

	// normal commands
	fun, ok := router[cmd]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmd + "'")
	}
	if len(args) > 1 {
		result = fun(db, args[1:])
	} else {
		result = fun(db, [][]byte{})
	}
	return
}

/* ---- Data Access ----- */

// Get returns DataEntity bind to given key
func (db *DB) Get(key string) (*DataEntity, bool) {
	db.stopWorld.Wait()

	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	if db.IsExpired(key) {
		return nil, false
	}
	entity, _ := raw.(*DataEntity)
	return entity, true
}

// Put a DataEntity into DB
func (db *DB) Put(key string, entity *DataEntity) int {
	db.stopWorld.Wait()
	return db.data.Put(key, entity)
}

// PutIfExists edit an existing DataEntity
func (db *DB) PutIfExists(key string, entity *DataEntity) int {
	db.stopWorld.Wait()
	return db.data.PutIfExists(key, entity)
}

// PutIfAbsent insert an DataEntity only if the key not exists
func (db *DB) PutIfAbsent(key string, entity *DataEntity) int {
	db.stopWorld.Wait()
	return db.data.PutIfAbsent(key, entity)
}

// Remove the given key from db
func (db *DB) Remove(key string) {
	db.stopWorld.Wait()
	db.data.Remove(key)
	db.ttlMap.Remove(key)
}

// Removes the given keys from db
func (db *DB) Removes(keys ...string) (deleted int) {
	db.stopWorld.Wait()
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			db.data.Remove(key)
			db.ttlMap.Remove(key)
			deleted++
		}
	}
	return deleted
}

// Flush clean database
func (db *DB) Flush() {
	db.stopWorld.Add(1)
	defer db.stopWorld.Done()

	db.data = dict.MakeConcurrent(dataDictSize)
	db.ttlMap = dict.MakeConcurrent(ttlDictSize)
	db.locker = lock.Make(lockerSize)

}

/* ---- Lock Function ----- */

// Lock locks key for writing (exclusive lock)
func (db *DB) Lock(key string) {
	db.locker.Lock(key)
}

// RLock locks key for read (shared lock)
func (db *DB) RLock(key string) {
	db.locker.RLock(key)
}

// UnLock release exclusive lock
func (db *DB) UnLock(key string) {
	db.locker.UnLock(key)
}

// RUnLock release shared lock
func (db *DB) RUnLock(key string) {
	db.locker.RUnLock(key)
}

// Locks lock keys for writing (exclusive lock)
func (db *DB) Locks(keys ...string) {
	db.locker.Locks(keys...)
}

// RLocks lock keys for read (shared lock)
func (db *DB) RLocks(keys ...string) {
	db.locker.RLocks(keys...)
}

// UnLocks release exclusive locks
func (db *DB) UnLocks(keys ...string) {
	db.locker.UnLocks(keys...)
}

// RUnLocks release shared locks
func (db *DB) RUnLocks(keys ...string) {
	db.locker.RUnLocks(keys...)
}

/* ---- TTL Functions ---- */

func genExpireTask(key string) string {
	return "expire:" + key
}

// Expire sets TTL of key
func (db *DB) Expire(key string, expireTime time.Time) {
	db.stopWorld.Wait()
	db.ttlMap.Put(key, expireTime)
	taskKey := genExpireTask(key)
	timewheel.At(expireTime, taskKey, func() {
		logger.Info("expire " + key)
		db.ttlMap.Remove(key)
		db.data.Remove(key)
	})
}

// Persist cancel TTL of key
func (db *DB) Persist(key string) {
	db.stopWorld.Wait()
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

// IsExpired check whether a key is expired
func (db *DB) IsExpired(key string) bool {
	rawExpireTime, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		db.Remove(key)
	}
	return expired
}

/* ---- Subscribe Functions ---- */

// AfterClientClose does some clean after client close connection
func (db *DB) AfterClientClose(c redis.Connection) {
	pubsub.UnsubscribeAll(db.hub, c)
}
