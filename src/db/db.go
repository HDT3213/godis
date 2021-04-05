package db

import (
	"fmt"
	"github.com/HDT3213/godis/src/config"
	"github.com/HDT3213/godis/src/datastruct/dict"
	"github.com/HDT3213/godis/src/datastruct/lock"
	"github.com/HDT3213/godis/src/interface/redis"
	"github.com/HDT3213/godis/src/lib/logger"
	"github.com/HDT3213/godis/src/lib/timewheel"
	"github.com/HDT3213/godis/src/pubsub"
	"github.com/HDT3213/godis/src/redis/reply"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

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
type CmdFunc func(db *DB, args [][]byte) redis.Reply

type DB struct {
	// key -> DataEntity
	Data dict.Dict
	// key -> expireTime (time.Time)
	TTLMap dict.Dict
	// channel -> list<*client>
	SubMap dict.Dict

	// dict will ensure thread safety of its method
	// use this mutex for complicated command only, eg. rpush, incr ...
	Locker *lock.Locks

	// TimerTask interval
	interval time.Duration

	stopWorld sync.WaitGroup

	hub *pubsub.Hub

	// main goroutine send commands to aof goroutine through aofChan
	aofChan     chan *reply.MultiBulkReply
	aofFile     *os.File
	aofFilename string

	aofRewriteChan chan *reply.MultiBulkReply
	pausingAof     sync.RWMutex
}

var router = MakeRouter()

func MakeDB() *DB {
	db := &DB{
		Data:     dict.MakeConcurrent(dataDictSize),
		TTLMap:   dict.MakeConcurrent(ttlDictSize),
		Locker:   lock.Make(lockerSize),
		interval: 5 * time.Second,
		hub:      pubsub.MakeHub(),
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
		go func() {
			db.handleAof()
		}()
	}

	// start timer
	db.TimerTask()
	return db
}

func (db *DB) Close() {
	if db.aofFile != nil {
		err := db.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
}

// execute command,
// parameter `args` is a RESP message including command and its params
func (db *DB) Exec(c redis.Connection, args [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	cmd := strings.ToLower(string(args[0]))

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
		reply := BGRewriteAOF(db, args[1:])
		return reply
	}

	// normal commands
	cmdFunc, ok := router[cmd]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmd + "'")
	}
	if len(args) > 1 {
		result = cmdFunc(db, args[1:])
	} else {
		result = cmdFunc(db, [][]byte{})
	}

	// aof

	return
}

/* ---- Data Access ----- */

func (db *DB) Get(key string) (*DataEntity, bool) {
	db.stopWorld.Wait()

	raw, ok := db.Data.Get(key)
	if !ok {
		return nil, false
	}
	if db.IsExpired(key) {
		return nil, false
	}
	entity, _ := raw.(*DataEntity)
	return entity, true
}

func (db *DB) Put(key string, entity *DataEntity) int {
	db.stopWorld.Wait()
	return db.Data.Put(key, entity)
}

func (db *DB) PutIfExists(key string, entity *DataEntity) int {
	db.stopWorld.Wait()
	return db.Data.PutIfExists(key, entity)
}

func (db *DB) PutIfAbsent(key string, entity *DataEntity) int {
	db.stopWorld.Wait()
	return db.Data.PutIfAbsent(key, entity)
}

func (db *DB) Remove(key string) {
	db.stopWorld.Wait()
	db.Data.Remove(key)
	db.TTLMap.Remove(key)
}

func (db *DB) Removes(keys ...string) (deleted int) {
	db.stopWorld.Wait()
	deleted = 0
	for _, key := range keys {
		_, exists := db.Data.Get(key)
		if exists {
			db.Data.Remove(key)
			db.TTLMap.Remove(key)
			deleted++
		}
	}
	return deleted
}

func (db *DB) Flush() {
	db.stopWorld.Add(1)
	defer db.stopWorld.Done()

	db.Data = dict.MakeConcurrent(dataDictSize)
	db.TTLMap = dict.MakeConcurrent(ttlDictSize)
	db.Locker = lock.Make(lockerSize)

}

/* ---- Lock Function ----- */

func (db *DB) Lock(key string) {
	db.Locker.Lock(key)
}

func (db *DB) RLock(key string) {
	db.Locker.RLock(key)
}

func (db *DB) UnLock(key string) {
	db.Locker.UnLock(key)
}

func (db *DB) RUnLock(key string) {
	db.Locker.RUnLock(key)
}

func (db *DB) Locks(keys ...string) {
	db.Locker.Locks(keys...)
}

func (db *DB) RLocks(keys ...string) {
	db.Locker.RLocks(keys...)
}

func (db *DB) UnLocks(keys ...string) {
	db.Locker.UnLocks(keys...)
}

func (db *DB) RUnLocks(keys ...string) {
	db.Locker.RUnLocks(keys...)
}

/* ---- TTL Functions ---- */

func genExpireTask(key string) string {
	return "expire:" + key
}

func (db *DB) Expire(key string, expireTime time.Time) {
	db.stopWorld.Wait()
	db.TTLMap.Put(key, expireTime)
	taskKey := genExpireTask(key)
	timewheel.At(expireTime, taskKey, func() {
		logger.Info("expire " + key)
		db.TTLMap.Remove(key)
		db.Data.Remove(key)
	})
}

func (db *DB) Persist(key string) {
	db.stopWorld.Wait()
	db.TTLMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

func (db *DB) IsExpired(key string) bool {
	rawExpireTime, ok := db.TTLMap.Get(key)
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

func (db *DB) TimerTask() {
}

/* ---- Subscribe Functions ---- */

func (db *DB) AfterClientClose(c redis.Connection) {
	pubsub.UnsubscribeAll(db.hub, c)
}
