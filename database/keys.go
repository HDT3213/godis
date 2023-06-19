package database

import (
	"github.com/hdt3213/godis/aof"
	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/datastruct/list"
	"github.com/hdt3213/godis/datastruct/set"
	"github.com/hdt3213/godis/datastruct/sortedset"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/lib/wildcard"
	"github.com/hdt3213/godis/redis/protocol"
	"strconv"
	"strings"
	"time"
)

// execDel removes a key from db
func execDel(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}

	deleted := db.Removes(keys...)
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("del", args...))
	}
	return protocol.MakeIntReply(int64(deleted))
}

func undoDel(db *DB, args [][]byte) []CmdLine {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return rollbackGivenKeys(db, keys...)
}

// execExists checks if given key is existed in db
func execExists(db *DB, args [][]byte) redis.Reply {
	result := int64(0)
	for _, arg := range args {
		key := string(arg)
		_, exists := db.GetEntity(key)
		if exists {
			result++
		}
	}
	return protocol.MakeIntReply(result)
}

// execFlushDB removes all data in current db
// deprecated, use Server.flushDB
func execFlushDB(db *DB, args [][]byte) redis.Reply {
	db.Flush()
	db.addAof(utils.ToCmdLine3("flushdb", args...))
	return &protocol.OkReply{}
}

// execType returns the type of entity, including: string, list, hash, set and zset
func execType(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeStatusReply("none")
	}
	switch entity.Data.(type) {
	case []byte:
		return protocol.MakeStatusReply("string")
	case list.List:
		return protocol.MakeStatusReply("list")
	case dict.Dict:
		return protocol.MakeStatusReply("hash")
	case *set.Set:
		return protocol.MakeStatusReply("set")
	case *sortedset.SortedSet:
		return protocol.MakeStatusReply("zset")
	}
	return &protocol.UnknownErrReply{}
}

func prepareRename(args [][]byte) ([]string, []string) {
	src := string(args[0])
	dest := string(args[1])
	return []string{dest}, []string{src}
}

// execRename a key
func execRename(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[0])
	dest := string(args[1])

	entity, ok := db.GetEntity(src)
	if !ok {
		return protocol.MakeErrReply("no such key")
	}
	rawTTL, hasTTL := db.ttlMap.Get(src)
	db.PutEntity(dest, entity)
	db.Remove(src)
	if hasTTL {
		db.Persist(src) // clean src and dest with their ttl
		db.Persist(dest)
		expireTime, _ := rawTTL.(time.Time)
		db.Expire(dest, expireTime)
	}
	db.addAof(utils.ToCmdLine3("rename", args...))
	return &protocol.OkReply{}
}

func undoRename(db *DB, args [][]byte) []CmdLine {
	src := string(args[0])
	dest := string(args[1])
	return rollbackGivenKeys(db, src, dest)
}

// execRenameNx a key, only if the new key does not exist
func execRenameNx(db *DB, args [][]byte) redis.Reply {
	src := string(args[0])
	dest := string(args[1])

	_, ok := db.GetEntity(dest)
	if ok {
		return protocol.MakeIntReply(0)
	}

	entity, ok := db.GetEntity(src)
	if !ok {
		return protocol.MakeErrReply("no such key")
	}
	rawTTL, hasTTL := db.ttlMap.Get(src)
	db.Removes(src, dest) // clean src and dest with their ttl
	db.PutEntity(dest, entity)
	if hasTTL {
		db.Persist(src) // clean src and dest with their ttl
		db.Persist(dest)
		expireTime, _ := rawTTL.(time.Time)
		db.Expire(dest, expireTime)
	}
	db.addAof(utils.ToCmdLine3("renamenx", args...))
	return protocol.MakeIntReply(1)
}

// execExpire sets a key's time to live in seconds
func execExpire(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	ttl := time.Duration(ttlArg) * time.Second

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	expireAt := time.Now().Add(ttl)
	db.Expire(key, expireAt)
	db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

// execExpireAt sets a key's expiration in unix timestamp
func execExpireAt(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	expireAt := time.Unix(raw, 0)

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	db.Expire(key, expireAt)
	db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

// execExpireTime returns the absolute Unix expiration timestamp in seconds at which the given key will expire.
func execExpireTime(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(-2)
	}

	raw, exists := db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}
	rawExpireTime, _ := raw.(time.Time)
	expireTime := rawExpireTime.Unix()
	return protocol.MakeIntReply(expireTime)
}

// execPExpire sets a key's time to live in milliseconds
func execPExpire(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	ttl := time.Duration(ttlArg) * time.Millisecond

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	expireAt := time.Now().Add(ttl)
	db.Expire(key, expireAt)
	db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

// execPExpireAt sets a key's expiration in unix timestamp specified in milliseconds
func execPExpireAt(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	expireAt := time.Unix(0, raw*int64(time.Millisecond))

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	db.Expire(key, expireAt)

	db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

// execPExpireTime returns the absolute Unix expiration timestamp in milliseconds at which the given key will expire.
func execPExpireTime(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(-2)
	}

	raw, exists := db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}
	rawExpireTime, _ := raw.(time.Time)
	expireTime := rawExpireTime.UnixMilli()
	return protocol.MakeIntReply(expireTime)
}

// execTTL returns a key's time to live in seconds
func execTTL(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(-2)
	}

	raw, exists := db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}
	expireTime, _ := raw.(time.Time)
	ttl := expireTime.Sub(time.Now())
	return protocol.MakeIntReply(int64(ttl / time.Second))
}

// execPTTL returns a key's time to live in milliseconds
func execPTTL(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(-2)
	}

	raw, exists := db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}
	expireTime, _ := raw.(time.Time)
	ttl := expireTime.Sub(time.Now())
	return protocol.MakeIntReply(int64(ttl / time.Millisecond))
}

// execPersist removes expiration from a key
func execPersist(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	_, exists = db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	db.Persist(key)
	db.addAof(utils.ToCmdLine3("persist", args...))
	return protocol.MakeIntReply(1)
}

// execKeys returns all keys matching the given pattern
func execKeys(db *DB, args [][]byte) redis.Reply {
	pattern, err := wildcard.CompilePattern(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR illegal wildcard")
	}
	result := make([][]byte, 0)
	db.data.ForEach(func(key string, val interface{}) bool {
		if !pattern.IsMatch(key) {
			return true
		}
		if !db.IsExpired(key) {
			result = append(result, []byte(key))
		}
		return true
	})
	return protocol.MakeMultiBulkReply(result)
}

func toTTLCmd(db *DB, key string) *protocol.MultiBulkReply {
	raw, exists := db.ttlMap.Get(key)
	if !exists {
		// has no TTL
		return protocol.MakeMultiBulkReply(utils.ToCmdLine("PERSIST", key))
	}
	expireTime, _ := raw.(time.Time)
	timestamp := strconv.FormatInt(expireTime.UnixNano()/1000/1000, 10)
	return protocol.MakeMultiBulkReply(utils.ToCmdLine("PEXPIREAT", key, timestamp))
}

func undoExpire(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	return []CmdLine{
		toTTLCmd(db, key).Args,
	}
}

// execCopy usage: COPY source destination [DB destination-db] [REPLACE]
// This command copies the value stored at the source key to the destination key.
func execCopy(mdb *Server, conn redis.Connection, args [][]byte) redis.Reply {
	dbIndex := conn.GetDBIndex()
	db := mdb.mustSelectDB(dbIndex) // Current DB
	replaceFlag := false
	srcKey := string(args[0])
	destKey := string(args[1])

	// Parse options
	if len(args) > 2 {
		for i := 2; i < len(args); i++ {
			arg := strings.ToLower(string(args[i]))
			if arg == "db" {
				if i+1 >= len(args) {
					return &protocol.SyntaxErrReply{}
				}
				idx, err := strconv.Atoi(string(args[i+1]))
				if err != nil {
					return &protocol.SyntaxErrReply{}
				}
				if idx >= len(mdb.dbSet) || idx < 0 {
					return protocol.MakeErrReply("ERR DB index is out of range")
				}
				dbIndex = idx
				i++
			} else if arg == "replace" {
				replaceFlag = true
			} else {
				return &protocol.SyntaxErrReply{}
			}
		}
	}

	if srcKey == destKey && dbIndex == conn.GetDBIndex() {
		return protocol.MakeErrReply("ERR source and destination objects are the same")
	}

	// source key does not exist
	src, exists := db.GetEntity(srcKey)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	destDB := mdb.mustSelectDB(dbIndex)
	if _, exists = destDB.GetEntity(destKey); exists != false {
		// If destKey exists and there is no "replace" option
		if replaceFlag == false {
			return protocol.MakeIntReply(0)
		}
	}

	destDB.PutEntity(destKey, src)
	raw, exists := db.ttlMap.Get(srcKey)
	if exists {
		expire := raw.(time.Time)
		destDB.Expire(destKey, expire)
	}
	mdb.AddAof(conn.GetDBIndex(), utils.ToCmdLine3("copy", args...))
	return protocol.MakeIntReply(1)
}

func init() {
	registerCommand("Del", execDel, writeAllKeys, undoDel, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, -1, 1)
	registerCommand("Expire", execExpire, writeFirstKey, undoExpire, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("ExpireAt", execExpireAt, writeFirstKey, undoExpire, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("ExpireTime", execExpireTime, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("PExpire", execPExpire, writeFirstKey, undoExpire, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("PExpireAt", execPExpireAt, writeFirstKey, undoExpire, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("PExpireTime", execPExpireTime, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("TTL", execTTL, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagRandom, redisFlagFast}, 1, 1, 1)
	registerCommand("PTTL", execPTTL, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagRandom, redisFlagFast}, 1, 1, 1)
	registerCommand("Persist", execPersist, writeFirstKey, undoExpire, 2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("Exists", execExists, readAllKeys, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("Type", execType, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("Rename", execRename, prepareRename, undoRename, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("RenameNx", execRenameNx, prepareRename, undoRename, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("Keys", execKeys, noPrepare, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)
}
