package database

import (
	"github.com/hdt3213/godis/aof"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/reply"
	"github.com/shopspring/decimal"
	"strconv"
	"strings"
	"time"
)

func (db *DB) getAsString(key string) ([]byte, reply.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	bytes, ok := entity.Data.([]byte)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return bytes, nil
}

// execGet returns string value bound to the given key
func execGet(db *DB, args [][]byte) redis.Reply {
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

// execSet sets string value and time to live to the given key
func execSet(db *DB, args [][]byte) redis.Reply {
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

	entity := &database.DataEntity{
		Data: value,
	}

	var result int
	switch policy {
	case upsertPolicy:
		db.PutEntity(key, entity)
		result = 1
	case insertPolicy:
		result = db.PutIfAbsent(key, entity)
	case updatePolicy:
		result = db.PutIfExists(key, entity)
	}
	if result > 0 {
		if ttl != unlimitedTTL {
			expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
			db.Expire(key, expireTime)
			db.addAof(CmdLine{
				[]byte("SET"),
				args[0],
				args[1],
			})
			db.addAof(aof.MakeExpireCmd(key, expireTime).Args)
		} else {
			db.Persist(key) // override ttl
			db.addAof(utils.ToCmdLine3("set", args...))
		}
	}

	if result > 0 {
		return &reply.OkReply{}
	}
	return &reply.NullBulkReply{}
}

// execSetNX sets string if not exists
func execSetNX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	value := args[1]
	entity := &database.DataEntity{
		Data: value,
	}
	result := db.PutIfAbsent(key, entity)
	db.addAof(utils.ToCmdLine3("setnx", args...))
	return reply.MakeIntReply(int64(result))
}

// execSetEX sets string and its ttl
func execSetEX(db *DB, args [][]byte) redis.Reply {
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

	entity := &database.DataEntity{
		Data: value,
	}

	db.PutEntity(key, entity)
	expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
	db.Expire(key, expireTime)
	db.addAof(utils.ToCmdLine3("setex", args...))
	db.addAof(aof.MakeExpireCmd(key, expireTime).Args)
	return &reply.OkReply{}
}

// execPSetEX set a key's time to live in  milliseconds
func execPSetEX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	value := args[2]

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &reply.SyntaxErrReply{}
	}
	if ttlArg <= 0 {
		return reply.MakeErrReply("ERR invalid expire time in setex")
	}

	entity := &database.DataEntity{
		Data: value,
	}

	db.PutEntity(key, entity)
	expireTime := time.Now().Add(time.Duration(ttlArg) * time.Millisecond)
	db.Expire(key, expireTime)
	db.addAof(utils.ToCmdLine3("setex", args...))
	db.addAof(aof.MakeExpireCmd(key, expireTime).Args)

	return &reply.OkReply{}
}

func prepareMSet(args [][]byte) ([]string, []string) {
	size := len(args) / 2
	keys := make([]string, size)
	for i := 0; i < size; i++ {
		keys[i] = string(args[2*i])
	}
	return keys, nil
}

func undoMSet(db *DB, args [][]byte) []CmdLine {
	writeKeys, _ := prepareMSet(args)
	return rollbackGivenKeys(db, writeKeys...)
}

// execMSet sets multi key-value in database
func execMSet(db *DB, args [][]byte) redis.Reply {
	if len(args)%2 != 0 {
		return reply.MakeSyntaxErrReply()
	}

	size := len(args) / 2
	keys := make([]string, size)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		keys[i] = string(args[2*i])
		values[i] = args[2*i+1]
	}

	for i, key := range keys {
		value := values[i]
		db.PutEntity(key, &database.DataEntity{Data: value})
	}
	db.addAof(utils.ToCmdLine3("mset", args...))
	return &reply.OkReply{}
}

func prepareMGet(args [][]byte) ([]string, []string) {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return nil, keys
}

// execMGet get multi key-value from database
func execMGet(db *DB, args [][]byte) redis.Reply {
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

// execMSetNX sets multi key-value in database, only if none of the given keys exist
func execMSetNX(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args)%2 != 0 {
		return reply.MakeSyntaxErrReply()
	}
	size := len(args) / 2
	values := make([][]byte, size)
	keys := make([]string, size)
	for i := 0; i < size; i++ {
		keys[i] = string(args[2*i])
		values[i] = args[2*i+1]
	}

	for _, key := range keys {
		_, exists := db.GetEntity(key)
		if exists {
			return reply.MakeIntReply(0)
		}
	}

	for i, key := range keys {
		value := values[i]
		db.PutEntity(key, &database.DataEntity{Data: value})
	}
	db.addAof(utils.ToCmdLine3("msetnx", args...))
	return reply.MakeIntReply(1)
}

// execGetSet sets value of a string-type key and returns its old value
func execGetSet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	value := args[1]

	old, err := db.getAsString(key)
	if err != nil {
		return err
	}

	db.PutEntity(key, &database.DataEntity{Data: value})
	db.Persist(key) // override ttl
	db.addAof(utils.ToCmdLine3("getset", args...))
	if old == nil {
		return new(reply.NullBulkReply)
	}
	return reply.MakeBulkReply(old)
}

// execIncr increments the integer value of a key by one
func execIncr(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes != nil {
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val+1, 10)),
		})
		db.addAof(utils.ToCmdLine3("incr", args...))
		return reply.MakeIntReply(val + 1)
	}
	db.PutEntity(key, &database.DataEntity{
		Data: []byte("1"),
	})
	db.addAof(utils.ToCmdLine3("incr", args...))
	return reply.MakeIntReply(1)
}

// execIncrBy increments the integer value of a key by given value
func execIncrBy(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	rawDelta := string(args[1])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}

	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		// existed value
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val+delta, 10)),
		})
		db.addAof(utils.ToCmdLine3("incrby", args...))
		return reply.MakeIntReply(val + delta)
	}
	db.PutEntity(key, &database.DataEntity{
		Data: args[1],
	})
	db.addAof(utils.ToCmdLine3("incrby", args...))
	return reply.MakeIntReply(delta)
}

// execIncrByFloat increments the float value of a key by given value
func execIncrByFloat(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	rawDelta := string(args[1])
	delta, err := decimal.NewFromString(rawDelta)
	if err != nil {
		return reply.MakeErrReply("ERR value is not a valid float")
	}

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
		db.PutEntity(key, &database.DataEntity{
			Data: resultBytes,
		})
		db.addAof(utils.ToCmdLine3("incrbyfloat", args...))
		return reply.MakeBulkReply(resultBytes)
	}
	db.PutEntity(key, &database.DataEntity{
		Data: args[1],
	})
	db.addAof(utils.ToCmdLine3("incrbyfloat", args...))
	return reply.MakeBulkReply(args[1])
}

// execDecr decrements the integer value of a key by one
func execDecr(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val-1, 10)),
		})
		db.addAof(utils.ToCmdLine3("decr", args...))
		return reply.MakeIntReply(val - 1)
	}
	entity := &database.DataEntity{
		Data: []byte("-1"),
	}
	db.PutEntity(key, entity)
	db.addAof(utils.ToCmdLine3("decr", args...))
	return reply.MakeIntReply(-1)
}

// execDecrBy decrements the integer value of a key by onedecrement
func execDecrBy(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	rawDelta := string(args[1])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}

	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val-delta, 10)),
		})
		db.addAof(utils.ToCmdLine3("decrby", args...))
		return reply.MakeIntReply(val - delta)
	}
	valueStr := strconv.FormatInt(-delta, 10)
	db.PutEntity(key, &database.DataEntity{
		Data: []byte(valueStr),
	})
	db.addAof(utils.ToCmdLine3("decrby", args...))
	return reply.MakeIntReply(-delta)
}

// execStrLen returns len of string value bound to the given key
func execStrLen(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		return reply.MakeIntReply(0)
	}
	return reply.MakeIntReply(int64(len(bytes)))
}

// execAppend sets string value to the given key
func execAppend(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	bytes = append(bytes, args[1]...)
	db.PutEntity(key, &database.DataEntity{
		Data: bytes,
	})
	db.addAof(utils.ToCmdLine3("append", args...))
	return reply.MakeIntReply(int64(len(bytes)))
}

// execSetRange overwrites part of the string stored at key, starting at the specified offset.
// If the offset is larger than the current length of the string at key, the string is padded with zero-bytes.
func execSetRange(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	offset, errNative := strconv.ParseInt(string(args[1]), 10, 64)
	if errNative != nil {
		return reply.MakeErrReply(errNative.Error())
	}
	value := args[2]
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	bytesLen := int64(len(bytes))
	if bytesLen < offset {
		diff := offset - bytesLen
		diffArray := make([]byte, diff)
		bytes = append(bytes, diffArray...)
		bytesLen = int64(len(bytes))
	}
	for i := 0; i < len(value); i++ {
		idx := offset + int64(i)
		if idx >= bytesLen {
			bytes = append(bytes, value[i])
		} else {
			bytes[idx] = value[i]
		}
	}
	db.PutEntity(key, &database.DataEntity{
		Data: bytes,
	})
	db.addAof(utils.ToCmdLine3("setRange", args...))
	return reply.MakeIntReply(int64(len(bytes)))
}

func execGetRange(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	startIdx, errNative := strconv.ParseInt(string(args[1]), 10, 64)
	if errNative != nil {
		return reply.MakeErrReply(errNative.Error())
	}
	endIdx, errNative := strconv.ParseInt(string(args[2]), 10, 64)
	if errNative != nil {
		return reply.MakeErrReply(errNative.Error())
	}

	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}

	if bytes == nil {
		return reply.MakeNullBulkReply()
	}

	bytesLen := int64(len(bytes))
	if startIdx < -1*bytesLen {
		return &reply.NullBulkReply{}
	} else if startIdx < 0 {
		startIdx = bytesLen + startIdx
	} else if startIdx >= bytesLen {
		return &reply.NullBulkReply{}
	}
	if endIdx < -1*bytesLen {
		return &reply.NullBulkReply{}
	} else if endIdx < 0 {
		endIdx = bytesLen + endIdx + 1
	} else if endIdx < bytesLen {
		endIdx = endIdx + 1
	} else {
		endIdx = bytesLen
	}
	if startIdx > endIdx {
		return reply.MakeNullBulkReply()
	}

	return reply.MakeBulkReply(bytes[startIdx:endIdx])
}

func init() {
	RegisterCommand("Set", execSet, writeFirstKey, rollbackFirstKey, -3)
	RegisterCommand("SetNx", execSetNX, writeFirstKey, rollbackFirstKey, 3)
	RegisterCommand("SetEX", execSetEX, writeFirstKey, rollbackFirstKey, 4)
	RegisterCommand("PSetEX", execPSetEX, writeFirstKey, rollbackFirstKey, 4)
	RegisterCommand("MSet", execMSet, prepareMSet, undoMSet, -3)
	RegisterCommand("MGet", execMGet, prepareMGet, nil, -2)
	RegisterCommand("MSetNX", execMSetNX, prepareMSet, undoMSet, -3)
	RegisterCommand("Get", execGet, readFirstKey, nil, 2)
	RegisterCommand("GetSet", execGetSet, writeFirstKey, rollbackFirstKey, 3)
	RegisterCommand("Incr", execIncr, writeFirstKey, rollbackFirstKey, 2)
	RegisterCommand("IncrBy", execIncrBy, writeFirstKey, rollbackFirstKey, 3)
	RegisterCommand("IncrByFloat", execIncrByFloat, writeFirstKey, rollbackFirstKey, 3)
	RegisterCommand("Decr", execDecr, writeFirstKey, rollbackFirstKey, 2)
	RegisterCommand("DecrBy", execDecrBy, writeFirstKey, rollbackFirstKey, 3)
	RegisterCommand("StrLen", execStrLen, readFirstKey, nil, 2)
	RegisterCommand("Append", execAppend, writeFirstKey, rollbackFirstKey, 3)
	RegisterCommand("SetRange", execSetRange, writeFirstKey, rollbackFirstKey, 4)
	RegisterCommand("GetRange", execGetRange, readFirstKey, nil, 4)
}
