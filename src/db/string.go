package db

import (
	"github.com/hdt3213/godis/src/interface/redis"
	"github.com/hdt3213/godis/src/redis/reply"
	"github.com/shopspring/decimal"
	"strconv"
	"strings"
	"time"
)

func (db *DB) getAsString(key string) ([]byte, reply.ErrorReply) {
	entity, ok := db.Get(key)
	if !ok {
		return nil, nil
	}
	bytes, ok := entity.Data.([]byte)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return bytes, nil
}

func Get(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'get' command")
	}
	key := string(args[0])
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		return &reply.NullBulkReply{}
	}
	return reply.MakeBulkReply(bytes)
}

const (
	upsertPolicy = iota // default
	insertPolicy        // set nx
	updatePolicy        // set ex
)

const unlimitedTTL int64 = 0

// SET key value [EX seconds] [PX milliseconds] [NX|XX]
func Set(db *DB, args [][]byte) redis.Reply {
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
					// ttl has been set
					return &reply.SyntaxErrReply{}
				}
				if i+1 >= len(args) {
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
				if i+1 >= len(args) {
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
		Data: value,
	}

	db.Persist(key) // clean ttl
	var result int
	switch policy {
	case upsertPolicy:
		result = db.Put(key, entity)
	case insertPolicy:
		result = db.PutIfAbsent(key, entity)
	case updatePolicy:
		result = db.PutIfExists(key, entity)
	}
	/*
	 *   如果设置了ttl 则以最新的ttl为准
	 *   如果没有设置ttl 是新增key的情况，不设置ttl。
	 *   如果没有设置ttl 且已存在key的 不修改ttl 但需要增加aof
	 */
	if ttl != unlimitedTTL {
		expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
		db.Expire(key, expireTime)
		db.AddAof(reply.MakeMultiBulkReply([][]byte{
			[]byte("SET"),
			args[0],
			args[1],
		}))
		db.AddAof(makeExpireCmd(key, expireTime))
	} else if result > 0 {
		db.Persist(key) // override ttl
		db.AddAof(makeAofCmd("set", args))
	} else {
		db.AddAof(makeAofCmd("set", args))
	}

	if policy == upsertPolicy || result > 0 {
		return &reply.OkReply{}
	} else {
		return &reply.NullBulkReply{}
	}
}

func SetNX(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'setnx' command")
	}
	key := string(args[0])
	value := args[1]
	entity := &DataEntity{
		Data: value,
	}
	result := db.PutIfAbsent(key, entity)
	db.AddAof(makeAofCmd("setnx", args))
	return reply.MakeIntReply(int64(result))
}

func SetEX(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'setex' command")
	}
	key := string(args[0])
	value := args[2]

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &reply.SyntaxErrReply{}
	}
	if ttlArg <= 0 {
		return reply.MakeErrReply("ERR invalid expire time in setex")
	}
	ttl := ttlArg * 1000

	entity := &DataEntity{
		Data: value,
	}

	db.Lock(key)
	defer db.UnLock(key)

	db.Put(key, entity)
	expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
	db.Expire(key, expireTime)
	db.AddAof(makeAofCmd("setex", args))
	db.AddAof(makeExpireCmd(key, expireTime))
	return &reply.OkReply{}
}

func PSetEX(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'setex' command")
	}
	key := string(args[0])
	value := args[2]

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &reply.SyntaxErrReply{}
	}
	if ttlArg <= 0 {
		return reply.MakeErrReply("ERR invalid expire time in setex")
	}

	entity := &DataEntity{
		Data: value,
	}

	db.Lock(key)
	defer db.UnLock(key)

	db.Put(key, entity)
	expireTime := time.Now().Add(time.Duration(ttlArg) * time.Millisecond)
	db.Expire(key, expireTime)
	db.AddAof(makeAofCmd("setex", args))
	db.AddAof(makeExpireCmd(key, expireTime))

	return &reply.OkReply{}
}

func MSet(db *DB, args [][]byte) redis.Reply {
	if len(args)%2 != 0 || len(args) == 0 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'mset' command")
	}

	size := len(args) / 2
	keys := make([]string, size)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		keys[i] = string(args[2*i])
		values[i] = args[2*i+1]
	}

	db.Locks(keys...)
	defer db.UnLocks(keys...)

	for i, key := range keys {
		value := values[i]
		db.Put(key, &DataEntity{Data: value})
	}
	db.AddAof(makeAofCmd("mset", args))
	return &reply.OkReply{}
}

func MGet(db *DB, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'mget' command")
	}
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}

	result := make([][]byte, len(args))
	for i, key := range keys {
		bytes, err := db.getAsString(key)
		if err != nil {
			_, isWrongType := err.(*reply.WrongTypeErrReply)
			if isWrongType {
				result[i] = nil
				continue
			} else {
				return err
			}
		}
		result[i] = bytes // nil or []byte
	}

	return reply.MakeMultiBulkReply(result)
}

func MSetNX(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args)%2 != 0 || len(args) == 0 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'msetnx' command")
	}
	size := len(args) / 2
	values := make([][]byte, size)
	keys := make([]string, size)
	for i := 0; i < size; i++ {
		keys[i] = string(args[2*i])
		values[i] = args[2*i+1]
	}

	// lock keys
	db.Locks(keys...)
	defer db.UnLocks(keys...)

	for _, key := range keys {
		_, exists := db.Get(key)
		if exists {
			return reply.MakeIntReply(0)
		}
	}

	for i, key := range keys {
		value := values[i]
		db.Put(key, &DataEntity{Data: value})
	}
	db.AddAof(makeAofCmd("msetnx", args))
	return reply.MakeIntReply(1)
}

func GetSet(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'getset' command")
	}
	key := string(args[0])
	value := args[1]

	old, err := db.getAsString(key)
	if err != nil {
		return err
	}

	db.Put(key, &DataEntity{Data: value})
	db.Persist(key) // override ttl
	db.AddAof(makeAofCmd("getset", args))
	if old == nil {
		return new(reply.NullBulkReply)
	}
	return reply.MakeBulkReply(old)
}

func Incr(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'incr' command")
	}
	key := string(args[0])

	db.Lock(key)
	defer db.UnLock(key)

	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes != nil {
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.Put(key, &DataEntity{
			Data: []byte(strconv.FormatInt(val+1, 10)),
		})
		db.AddAof(makeAofCmd("incr", args))
		return reply.MakeIntReply(val + 1)
	} else {
		db.Put(key, &DataEntity{
			Data: []byte("1"),
		})
		db.AddAof(makeAofCmd("incr", args))
		return reply.MakeIntReply(1)
	}
}

func IncrBy(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'incrby' command")
	}
	key := string(args[0])
	rawDelta := string(args[1])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}

	db.Lock(key)
	defer db.UnLock(key)

	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.Put(key, &DataEntity{
			Data: []byte(strconv.FormatInt(val+delta, 10)),
		})
		db.AddAof(makeAofCmd("incrby", args))
		return reply.MakeIntReply(val + delta)
	} else {
		db.Put(key, &DataEntity{
			Data: args[1],
		})
		db.AddAof(makeAofCmd("incrby", args))
		return reply.MakeIntReply(delta)
	}
}

func IncrByFloat(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'incrbyfloat' command")
	}
	key := string(args[0])
	rawDelta := string(args[1])
	delta, err := decimal.NewFromString(rawDelta)
	if err != nil {
		return reply.MakeErrReply("ERR value is not a valid float")
	}

	db.Lock(key)
	defer db.UnLock(key)

	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		val, err := decimal.NewFromString(string(bytes))
		if err != nil {
			return reply.MakeErrReply("ERR value is not a valid float")
		}
		resultBytes := []byte(val.Add(delta).String())
		db.Put(key, &DataEntity{
			Data: resultBytes,
		})
		db.AddAof(makeAofCmd("incrbyfloat", args))
		return reply.MakeBulkReply(resultBytes)
	} else {
		db.Put(key, &DataEntity{
			Data: args[1],
		})
		db.AddAof(makeAofCmd("incrbyfloat", args))
		return reply.MakeBulkReply(args[1])
	}
}

func Decr(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'decr' command")
	}
	key := string(args[0])

	db.Lock(key)
	defer db.UnLock(key)

	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.Put(key, &DataEntity{
			Data: []byte(strconv.FormatInt(val-1, 10)),
		})
		db.AddAof(makeAofCmd("decr", args))
		return reply.MakeIntReply(val - 1)
	} else {
		entity := &DataEntity{
			Data: []byte("-1"),
		}
		db.Put(key, entity)
		db.AddAof(makeAofCmd("decr", args))
		return reply.MakeIntReply(-1)
	}
}

func DecrBy(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'decrby' command")
	}
	key := string(args[0])
	rawDelta := string(args[1])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}

	db.Lock(key)
	defer db.UnLock(key)

	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.Put(key, &DataEntity{
			Data: []byte(strconv.FormatInt(val-delta, 10)),
		})
		db.AddAof(makeAofCmd("decrby", args))
		return reply.MakeIntReply(val - delta)
	} else {
		valueStr := strconv.FormatInt(-delta, 10)
		db.Put(key, &DataEntity{
			Data: []byte(valueStr),
		})
		db.AddAof(makeAofCmd("decrby", args))
		return reply.MakeIntReply(-delta)
	}
}
