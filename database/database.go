// Package database is a memory database with redis compatible interface
package database

import (
	"strings"
	"time"

	"github.com/hdt3213/godis/datastruct/dict" // 并发安全的字典结构
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/timewheel" // 时间轮实现，用于处理键的过期
	"github.com/hdt3213/godis/redis/protocol"
)

const (
	dataDictSize = 1 << 16 // 数据字典的初始大小 65536
	ttlDictSize  = 1 << 10 // TTL字典的初始大小
)

// DB 存储数据并执行用户命令
type DB struct {
	index          int                  // 数据库索引
	data           *dict.ConcurrentDict // 用于存储键值对的字典
	ttlMap         *dict.ConcurrentDict // 存储键的 TTL 信息的字典
	versionMap     *dict.ConcurrentDict
	addAof         func(CmdLine)             // 函数，用于将命令添加到 AOF，用于分db在执行命令的同时可以调用addaof命令把执行的命令放入到aof管道中
	insertCallback database.KeyEventCallback // 插入键时的回调
	deleteCallback database.KeyEventCallback // 删除键时的回调
}

// ExecFunc is interface for command executor
// args don't include cmd line
// ExecFunc 定义了命令执行器的接口，用于执行具体的数据库命令。
type ExecFunc func(db *DB, args [][]byte) redis.Reply

// PreFunc analyses command line when queued command to `multi`
// returns related write keys and read keys
// PreFunc 定义了命令预处理的接口，用于分析命令行和相关键。
type PreFunc func(args [][]byte) ([]string, []string)

// CmdLine is alias for [][]byte, represents a command line
// CmdLine 是二维字节数组的别名，表示命令行。
type CmdLine = [][]byte

// UndoFunc returns undo logs for the given command line
// execute from head to tail when undo
// UndoFunc 定义了撤销命令的接口，用于生成撤销操作的日志。
type UndoFunc func(db *DB, args [][]byte) []CmdLine

// makeDB 创建一个 DB 实例
func makeDB() *DB {
	db := &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		addAof:     func(line CmdLine) {},
	}
	return db
}

// makeBasicDB 创建一个基础功能的DB实例，只包括基础数据结构，没有额外的功能配置。
func makeBasicDB() *DB {
	db := &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		addAof:     func(line CmdLine) {},
	}
	return db
}

// Exec 执行单个数据库内的命令
func (db *DB) Exec(c redis.Connection, cmdLine [][]byte) redis.Reply {
	// 将命令行的第一个词（通常是命令名）转换为小写。
	cmdName := strings.ToLower(string(cmdLine[0]))
	// 处理特殊的事务控制命令和其他不能在事务中执行的命令。
	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return StartMulti(c)
	} else if cmdName == "discard" { // 取消一个事务。
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return DiscardMulti(c)
	} else if cmdName == "exec" { // 执行一个事务。
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return execMulti(db, c)
	} else if cmdName == "watch" { // 监视给定的键，用于事务。
		if !validateArity(-2, cmdLine) {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return Watch(db, c, cmdLine[1:])
	}
	if c != nil && c.InMultiState() { // 如果处于事务中，将命令加入队列
		return EnqueueCmd(c, cmdLine)
	}
	// 执行普通命令。
	return db.execNormalCommand(cmdLine)
}

// execNormalCommand 执行普通命令
func (db *DB) execNormalCommand(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName] //此时的cmd是command类型
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) { //验证命令的 参数是否对应
		return protocol.MakeArgNumErrReply(cmdName)
	}

	prepare := cmd.prepare
	write, read := prepare(cmdLine[1:])
	db.addVersion(write...)
	db.RWLocks(write, read)
	defer db.RWUnLocks(write, read)
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

// execWithLock 使用提供的锁执行命令，通常用于内部调用
func (db *DB) execWithLock(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

// validateArity 验证给定命令的参数数量是否正确。-2表示最少为2个，可变长
func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

/* ---- Data Access ----- */

// GetEntity returns DataEntity bind to given key
// DB把底层的dict包了一层，所以在DB层逻辑写一下
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, ok := db.data.GetWithLock(key)
	if !ok {
		return nil, false
	}
	if db.IsExpired(key) {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

// PutEntity a DataEntity into DB
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	ret := db.data.PutWithLock(key, entity)
	// db.insertCallback may be set as nil, during `if` and actually callback
	// so introduce a local variable `cb`
	if cb := db.insertCallback; ret > 0 && cb != nil {
		cb(db.index, key, entity)
	}
	return ret
}

// PutIfExists edit an existing DataEntity
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExistsWithLock(key, entity)
}

// PutIfAbsent insert an DataEntity only if the key not exists
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	ret := db.data.PutIfAbsentWithLock(key, entity)
	// db.insertCallback may be set as nil, during `if` and actually callback
	// so introduce a local variable `cb`
	if cb := db.insertCallback; ret > 0 && cb != nil {
		cb(db.index, key, entity)
	}
	return ret
}

// Remove the given key from db
func (db *DB) Remove(key string) {
	raw, deleted := db.data.RemoveWithLock(key)
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
	if cb := db.deleteCallback; cb != nil {
		var entity *database.DataEntity
		if deleted > 0 {
			entity = raw.(*database.DataEntity)
		}
		cb(db.index, key, entity)
	}
}

// Removes the given keys from db
func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.GetWithLock(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

// Flush clean database
// deprecated
// for test only
func (db *DB) Flush() {
	db.data.Clear()
	db.ttlMap.Clear()
}

/* ---- Lock Function ----- */

// RWLocks lock keys for writing and reading
func (db *DB) RWLocks(writeKeys []string, readKeys []string) {
	db.data.RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
func (db *DB) RWUnLocks(writeKeys []string, readKeys []string) {
	db.data.RWUnLocks(writeKeys, readKeys)
}

/* ---- TTL Functions ---- */
// genExpireTask 生成一个用于时间轮的任务键
// 该函数接受一个键名，并返回一个与之相关的特定格式的任务键
func genExpireTask(key string) string {
	return "expire:" + key
}

// Expire 为指定的键设置过期时间
// key 为键名，expireTime 为过期时间点
func (db *DB) Expire(key string, expireTime time.Time) {
	db.ttlMap.Put(key, expireTime)             // 将键和过期时间存储到ttlMap中
	taskKey := genExpireTask(key)              // 生成定时任务的键
	timewheel.At(expireTime, taskKey, func() { // 在时间轮上设置任务，当时间达到expireTime时执行
		keys := []string{key}
		db.RWLocks(keys, nil) // 为该键加读写锁，确保在检查和删除过程中键不会被修改
		defer db.RWUnLocks(keys, nil)
		// 检查-锁定-再检查模式，以防在等待锁的过程中键的过期时间被更新
		logger.Info("expire " + key)
		rawExpireTime, ok := db.ttlMap.Get(key)
		if !ok {
			return // 如果键已经不存在于ttlMap中，直接返回
		}
		expireTime, _ := rawExpireTime.(time.Time)
		expired := time.Now().After(expireTime) // 检查当前时间是否超过过期时间
		if expired {
			db.Remove(key) // 如果已过期，从数据库中删除该键
		}
	})
}

// Persist 取消键的过期时间
// key 为需要取消过期时间的键名
func (db *DB) Persist(key string) {
	db.ttlMap.Remove(key)         // 从ttlMap中移除键，意味着该键将不再有过期时间
	taskKey := genExpireTask(key) // 生成对应的定时任务键
	timewheel.Cancel(taskKey)     // 取消该键对应的定时任务，避免它被错误地删除
}

// IsExpired 检查键是否已经过期
// key 为需要检查的键名
func (db *DB) IsExpired(key string) bool {
	rawExpireTime, ok := db.ttlMap.Get(key) // 获取键的过期时间
	if !ok {
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime) // 检查当前时间是否超过过期时间
	if expired {
		db.Remove(key) // 如果已过期，从数据库中删除该键
	}
	return expired // 返回过期状态
}

/* --- add version --- */

func (db *DB) addVersion(keys ...string) {
	for _, key := range keys {
		versionCode := db.GetVersion(key)
		db.versionMap.Put(key, versionCode+1)
	}
}

// GetVersion returns version code for given key
func (db *DB) GetVersion(key string) uint32 {
	entity, ok := db.versionMap.Get(key)
	if !ok {
		return 0
	}
	return entity.(uint32)
}

// ForEach traverses all the keys in the database
func (db *DB) ForEach(cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	db.data.ForEach(func(key string, raw interface{}) bool {
		entity, _ := raw.(*database.DataEntity)
		var expiration *time.Time
		rawExpireTime, ok := db.ttlMap.Get(key)
		if ok {
			expireTime, _ := rawExpireTime.(time.Time)
			expiration = &expireTime
		}

		return cb(key, entity, expiration)
	})
}
