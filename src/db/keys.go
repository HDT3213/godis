package db

import (
	"github.com/HDT3213/godis/src/datastruct/dict"
	"github.com/HDT3213/godis/src/datastruct/list"
	"github.com/HDT3213/godis/src/datastruct/set"
	"github.com/HDT3213/godis/src/datastruct/sortedset"
	"github.com/HDT3213/godis/src/interface/redis"
	"github.com/HDT3213/godis/src/redis/reply"
	"strconv"
	"time"
)

func Del(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) == 0 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'del' command"), nil
	}
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}

	db.Locker.Locks(keys...)
	defer func() {
		db.Locker.UnLocks(keys...)
	}()

	deleted := db.Removes(keys...)
	return reply.MakeIntReply(int64(deleted)), &extra{toPersist: deleted > 0}
}

func Exists(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'exists' command"), nil
	}
	key := string(args[0])
	_, exists := db.Get(key)
	if exists {
		return reply.MakeIntReply(1), nil
	} else {
		return reply.MakeIntReply(0), nil
	}
}

func FlushDB(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) != 0 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'flushdb' command"), nil
	}
	db.Flush()
	return &reply.OkReply{}, &extra{toPersist: true}
}

func FlushAll(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) != 0 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'flushall' command"), nil
	}
	db.Flush()
	return &reply.OkReply{}, &extra{toPersist: true}
}

func Type(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'type' command"), nil
	}
	key := string(args[0])
	entity, exists := db.Get(key)
	if !exists {
		return reply.MakeStatusReply("none"), nil
	}
	switch entity.Data.(type) {
	case []byte:
		return reply.MakeStatusReply("string"), nil
	case *list.LinkedList:
		return reply.MakeStatusReply("list"), nil
	case *dict.Dict:
		return reply.MakeStatusReply("hash"), nil
	case *set.Set:
		return reply.MakeStatusReply("set"), nil
	case *sortedset.SortedSet:
		return reply.MakeStatusReply("zset"), nil
	}
	return &reply.UnknownErrReply{}, nil
}

func Rename(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rename' command"), nil
	}
	src := string(args[0])
	dest := string(args[1])

	db.Locks(src, dest)
	defer db.UnLocks(src, dest)

	entity, ok := db.Get(src)
	if !ok {
		return reply.MakeErrReply("no such key"), nil
	}
	rawTTL, hasTTL := db.TTLMap.Get(src)
	db.Removes(src, dest) // clean src and dest with their ttl
	db.Put(dest, entity)
	if hasTTL {
		expireTime, _ := rawTTL.(time.Time)
		db.Expire(dest, expireTime)
	}
	return &reply.OkReply{}, &extra{toPersist: true}
}

func RenameNx(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'renamenx' command"), nil
	}
	src := string(args[0])
	dest := string(args[1])

	db.Locks(src, dest)
	defer db.UnLocks(src, dest)

	_, ok := db.Get(dest)
	if ok {
		return reply.MakeIntReply(0), nil
	}

	entity, ok := db.Get(src)
	if !ok {
		return reply.MakeErrReply("no such key"), nil
	}
	rawTTL, hasTTL := db.TTLMap.Get(src)
	db.Removes(src, dest) // clean src and dest with their ttl
	db.Put(dest, entity)
	if hasTTL {
		expireTime, _ := rawTTL.(time.Time)
		db.Expire(dest, expireTime)
	}
	return reply.MakeIntReply(1), &extra{toPersist: true}
}

func Expire(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'expire' command"), nil
	}
	key := string(args[0])

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
	}
	ttl := time.Duration(ttlArg) * time.Second

	_, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(0), nil
	}

	expireAt := time.Now().Add(ttl)
	db.Expire(key, expireAt)
	specialAof := []*reply.MultiBulkReply{ // for aof
		makeExpireCmd(key, expireAt),
	}
	return reply.MakeIntReply(1), &extra{toPersist: true, specialAof: specialAof}
}

func ExpireAt(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'expireat' command"), nil
	}
	key := string(args[0])

	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
	}
	expireTime := time.Unix(raw, 0)

	_, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(0), nil
	}

	db.Expire(key, expireTime)
	specialAof := []*reply.MultiBulkReply{ // for aof
		makeExpireCmd(key, expireTime),
	}
	return reply.MakeIntReply(1), &extra{toPersist: true, specialAof: specialAof}
}

func PExpire(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'pexpire' command"), nil
	}
	key := string(args[0])

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
	}
	ttl := time.Duration(ttlArg) * time.Millisecond

	_, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(0), nil
	}

	expireTime := time.Now().Add(ttl)
	db.Expire(key, expireTime)
	specialAof := []*reply.MultiBulkReply{ // for aof
		makeExpireCmd(key, expireTime),
	}
	return reply.MakeIntReply(1), &extra{toPersist: true, specialAof: specialAof}
}

func PExpireAt(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'pexpireat' command"), nil
	}
	key := string(args[0])

	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
	}
	expireTime := time.Unix(0, raw*int64(time.Millisecond))

	_, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(0), nil
	}

	db.Expire(key, expireTime)
	specialAof := []*reply.MultiBulkReply{ // for aof
		makeExpireCmd(key, expireTime),
	}
	return reply.MakeIntReply(1), &extra{toPersist: true, specialAof: specialAof}
}

func TTL(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'ttl' command"), nil
	}
	key := string(args[0])
	_, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(-2), nil
	}

	raw, exists := db.TTLMap.Get(key)
	if !exists {
		return reply.MakeIntReply(-1), nil
	}
	expireTime, _ := raw.(time.Time)
	ttl := expireTime.Sub(time.Now())
	return reply.MakeIntReply(int64(ttl / time.Second)), nil
}

func PTTL(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'ttl' command"), nil
	}
	key := string(args[0])
	_, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(-2), nil
	}

	raw, exists := db.TTLMap.Get(key)
	if !exists {
		return reply.MakeIntReply(-1), nil
	}
	expireTime, _ := raw.(time.Time)
	ttl := expireTime.Sub(time.Now())
	return reply.MakeIntReply(int64(ttl / time.Millisecond)), nil
}

func Persist(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'persist' command"), nil
	}
	key := string(args[0])
	_, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(0), nil
	}

	_, exists = db.TTLMap.Get(key)
	if !exists {
		return reply.MakeIntReply(0), nil
	}

	db.TTLMap.Remove(key)
	return reply.MakeIntReply(1), &extra{toPersist: true}
}

func BGRewriteAOF(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) != 0 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'bgrewriteaof' command"), nil
	}
	go db.aofRewrite()
	return reply.MakeStatusReply("Background append only file rewriting started"), nil
}
