package database

import (
	"math/bits"
	"strconv"
	"strings"
	"time"

	"github.com/hdt3213/godis/aof"
	"github.com/hdt3213/godis/datastruct/bitmap"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

func (db *DB) getAsString(key string) ([]byte, protocol.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	bytes, ok := entity.Data.([]byte)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
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
		return &protocol.NullBulkReply{}
	}
	return protocol.MakeBulkReply(bytes)
}

const (
	upsertPolicy = iota // default
	insertPolicy        // set nx
	updatePolicy        // set ex
)

const unlimitedTTL int64 = 0

// execGetEX Get the value of key and optionally set its expiration
func execGetEX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key)
	ttl := unlimitedTTL
	if err != nil {
		return err
	}
	if bytes == nil {
		return &protocol.NullBulkReply{}
	}

	for i := 1; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))
		if arg == "EX" { // ttl in seconds
			if ttl != unlimitedTTL {
				// ttl has been set
				return &protocol.SyntaxErrReply{}
			}
			if i+1 >= len(args) {
				return &protocol.SyntaxErrReply{}
			}
			ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return &protocol.SyntaxErrReply{}
			}
			if ttlArg <= 0 {
				return protocol.MakeErrReply("ERR invalid expire time in getex")
			}
			ttl = ttlArg * 1000
			i++ // skip next arg
		} else if arg == "PX" { // ttl in milliseconds
			if ttl != unlimitedTTL {
				return &protocol.SyntaxErrReply{}
			}
			if i+1 >= len(args) {
				return &protocol.SyntaxErrReply{}
			}
			ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return &protocol.SyntaxErrReply{}
			}
			if ttlArg <= 0 {
				return protocol.MakeErrReply("ERR invalid expire time in getex")
			}
			ttl = ttlArg
			i++ // skip next arg
		} else if arg == "PERSIST" {
			if ttl != unlimitedTTL { // PERSIST Cannot be used with EX | PX
				return &protocol.SyntaxErrReply{}
			}
			if i+1 > len(args) {
				return &protocol.SyntaxErrReply{}
			}
			db.Persist(key)
		}
	}

	if len(args) > 1 {
		if ttl != unlimitedTTL { // EX | PX
			expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
			db.Expire(key, expireTime)
			db.addAof(aof.MakeExpireCmd(key, expireTime).Args)
		} else { // PERSIST
			db.Persist(key) // override ttl
			// we convert to persist command to write aof
			db.addAof(utils.ToCmdLine3("persist", args[0]))
		}
	}
	return protocol.MakeBulkReply(bytes)
}

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
					return &protocol.SyntaxErrReply{}
				}
				policy = insertPolicy
			} else if arg == "XX" { // update policy
				if policy == insertPolicy {
					return &protocol.SyntaxErrReply{}
				}
				policy = updatePolicy
			} else if arg == "EX" { // ttl in seconds
				if ttl != unlimitedTTL {
					// ttl has been set
					return &protocol.SyntaxErrReply{}
				}
				if i+1 >= len(args) {
					return &protocol.SyntaxErrReply{}
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return &protocol.SyntaxErrReply{}
				}
				if ttlArg <= 0 {
					return protocol.MakeErrReply("ERR invalid expire time in set")
				}
				ttl = ttlArg * 1000
				i++ // skip next arg
			} else if arg == "PX" { // ttl in milliseconds
				if ttl != unlimitedTTL {
					return &protocol.SyntaxErrReply{}
				}
				if i+1 >= len(args) {
					return &protocol.SyntaxErrReply{}
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return &protocol.SyntaxErrReply{}
				}
				if ttlArg <= 0 {
					return protocol.MakeErrReply("ERR invalid expire time in set")
				}
				ttl = ttlArg
				i++ // skip next arg
			} else {
				return &protocol.SyntaxErrReply{}
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
		return &protocol.OkReply{}
	}
	return &protocol.NullBulkReply{}
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
	return protocol.MakeIntReply(int64(result))
}

// execSetEX sets string and its ttl
func execSetEX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	value := args[2]

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &protocol.SyntaxErrReply{}
	}
	if ttlArg <= 0 {
		return protocol.MakeErrReply("ERR invalid expire time in setex")
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
	return &protocol.OkReply{}
}

// execPSetEX set a key's time to live in  milliseconds
func execPSetEX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	value := args[2]

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &protocol.SyntaxErrReply{}
	}
	if ttlArg <= 0 {
		return protocol.MakeErrReply("ERR invalid expire time in setex")
	}

	entity := &database.DataEntity{
		Data: value,
	}

	db.PutEntity(key, entity)
	expireTime := time.Now().Add(time.Duration(ttlArg) * time.Millisecond)
	db.Expire(key, expireTime)
	db.addAof(utils.ToCmdLine3("setex", args...))
	db.addAof(aof.MakeExpireCmd(key, expireTime).Args)

	return &protocol.OkReply{}
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
		return protocol.MakeSyntaxErrReply()
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
	return &protocol.OkReply{}
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
			_, isWrongType := err.(*protocol.WrongTypeErrReply)
			if isWrongType {
				result[i] = nil
				continue
			} else {
				return err
			}
		}
		result[i] = bytes // nil or []byte
	}

	return protocol.MakeMultiBulkReply(result)
}

// execMSetNX sets multi key-value in database, only if none of the given keys exist
func execMSetNX(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args)%2 != 0 {
		return protocol.MakeSyntaxErrReply()
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
			return protocol.MakeIntReply(0)
		}
	}

	for i, key := range keys {
		value := values[i]
		db.PutEntity(key, &database.DataEntity{Data: value})
	}
	db.addAof(utils.ToCmdLine3("msetnx", args...))
	return protocol.MakeIntReply(1)
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
	db.addAof(utils.ToCmdLine3("set", args...))
	if old == nil {
		return new(protocol.NullBulkReply)
	}
	return protocol.MakeBulkReply(old)
}

// execGetDel Get the value of key and delete the key.
func execGetDel(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	old, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if old == nil {
		return new(protocol.NullBulkReply)
	}
	db.Remove(key)

	// We convert to del command to write aof
	db.addAof(utils.ToCmdLine3("del", args...))
	return protocol.MakeBulkReply(old)
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
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val+1, 10)),
		})
		db.addAof(utils.ToCmdLine3("incr", args...))
		return protocol.MakeIntReply(val + 1)
	}
	db.PutEntity(key, &database.DataEntity{
		Data: []byte("1"),
	})
	db.addAof(utils.ToCmdLine3("incr", args...))
	return protocol.MakeIntReply(1)
}

// execIncrBy increments the integer value of a key by given value
func execIncrBy(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	rawDelta := string(args[1])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		// existed value
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val+delta, 10)),
		})
		db.addAof(utils.ToCmdLine3("incrby", args...))
		return protocol.MakeIntReply(val + delta)
	}
	db.PutEntity(key, &database.DataEntity{
		Data: args[1],
	})
	db.addAof(utils.ToCmdLine3("incrby", args...))
	return protocol.MakeIntReply(delta)
}

// execIncrByFloat increments the float value of a key by given value
func execIncrByFloat(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	rawDelta := string(args[1])
	delta, err := strconv.ParseFloat(rawDelta, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not a valid float")
	}

	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		val, err := strconv.ParseFloat(string(bytes), 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not a valid float")
		}
		resultBytes := []byte(strconv.FormatFloat(val+delta, 'f', -1, 64))
		db.PutEntity(key, &database.DataEntity{
			Data: resultBytes,
		})
		db.addAof(utils.ToCmdLine3("incrbyfloat", args...))
		return protocol.MakeBulkReply(resultBytes)
	}
	db.PutEntity(key, &database.DataEntity{
		Data: args[1],
	})
	db.addAof(utils.ToCmdLine3("incrbyfloat", args...))
	return protocol.MakeBulkReply(args[1])
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
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val-1, 10)),
		})
		db.addAof(utils.ToCmdLine3("decr", args...))
		return protocol.MakeIntReply(val - 1)
	}
	entity := &database.DataEntity{
		Data: []byte("-1"),
	}
	db.PutEntity(key, entity)
	db.addAof(utils.ToCmdLine3("decr", args...))
	return protocol.MakeIntReply(-1)
}

// execDecrBy decrements the integer value of a key by onedecrement
func execDecrBy(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	rawDelta := string(args[1])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val-delta, 10)),
		})
		db.addAof(utils.ToCmdLine3("decrby", args...))
		return protocol.MakeIntReply(val - delta)
	}
	valueStr := strconv.FormatInt(-delta, 10)
	db.PutEntity(key, &database.DataEntity{
		Data: []byte(valueStr),
	})
	db.addAof(utils.ToCmdLine3("decrby", args...))
	return protocol.MakeIntReply(-delta)
}

// execStrLen returns len of string value bound to the given key
func execStrLen(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(len(bytes)))
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
	return protocol.MakeIntReply(int64(len(bytes)))
}

// execSetRange overwrites part of the string stored at key, starting at the specified offset.
// If the offset is larger than the current length of the string at key, the string is padded with zero-bytes.
func execSetRange(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	offset, errNative := strconv.ParseInt(string(args[1]), 10, 64)
	if errNative != nil {
		return protocol.MakeErrReply(errNative.Error())
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
	return protocol.MakeIntReply(int64(len(bytes)))
}

func execGetRange(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	startIdx, err2 := strconv.ParseInt(string(args[1]), 10, 64)
	if err2 != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	endIdx, err2 := strconv.ParseInt(string(args[2]), 10, 64)
	if err2 != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	bs, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bs == nil {
		return protocol.MakeNullBulkReply()
	}
	bytesLen := int64(len(bs))
	beg, end := utils.ConvertRange(startIdx, endIdx, bytesLen)
	if beg < 0 {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeBulkReply(bs[beg:end])
}

func execSetBit(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR bit offset is not an integer or out of range")
	}
	valStr := string(args[2])
	var v byte
	if valStr == "1" {
		v = 1
	} else if valStr == "0" {
		v = 0
	} else {
		return protocol.MakeErrReply("ERR bit is not an integer or out of range")
	}
	bs, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	bm := bitmap.FromBytes(bs)
	former := bm.GetBit(offset)
	bm.SetBit(offset, v)
	db.PutEntity(key, &database.DataEntity{Data: bm.ToBytes()})
	db.addAof(utils.ToCmdLine3("setBit", args...))
	return protocol.MakeIntReply(int64(former))
}

func execGetBit(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR bit offset is not an integer or out of range")
	}
	bs, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bs == nil {
		return protocol.MakeIntReply(0)
	}
	bm := bitmap.FromBytes(bs)
	return protocol.MakeIntReply(int64(bm.GetBit(offset)))
}

func execBitCount(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bs, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bs == nil {
		return protocol.MakeIntReply(0)
	}
	byteMode := true
	if len(args) > 3 {
		mode := strings.ToLower(string(args[3]))
		if mode == "bit" {
			byteMode = false
		} else if mode == "byte" {
			byteMode = true
		} else {
			return protocol.MakeErrReply("ERR syntax error")
		}
	}
	var size int64
	bm := bitmap.FromBytes(bs)
	if byteMode {
		size = int64(len(*bm))
	} else {
		size = int64(bm.BitSize())
	}
	var beg, end int
	if len(args) > 1 {
		var err2 error
		var startIdx, endIdx int64
		startIdx, err2 = strconv.ParseInt(string(args[1]), 10, 64)
		if err2 != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		endIdx, err2 = strconv.ParseInt(string(args[2]), 10, 64)
		if err2 != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		beg, end = utils.ConvertRange(startIdx, endIdx, size)
		if beg < 0 {
			return protocol.MakeIntReply(0)
		}
	}
	var count int64
	if byteMode {
		bm.ForEachByte(beg, end, func(offset int64, val byte) bool {
			count += int64(bits.OnesCount8(val))
			return true
		})
	} else {
		bm.ForEachBit(int64(beg), int64(end), func(offset int64, val byte) bool {
			if val > 0 {
				count++
			}
			return true
		})
	}
	return protocol.MakeIntReply(count)
}

func execBitPos(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bs, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bs == nil {
		return protocol.MakeIntReply(-1)
	}
	valStr := string(args[1])
	var v byte
	if valStr == "1" {
		v = 1
	} else if valStr == "0" {
		v = 0
	} else {
		return protocol.MakeErrReply("ERR bit is not an integer or out of range")
	}
	byteMode := true
	if len(args) > 4 {
		mode := strings.ToLower(string(args[4]))
		if mode == "bit" {
			byteMode = false
		} else if mode == "byte" {
			byteMode = true
		} else {
			return protocol.MakeErrReply("ERR syntax error")
		}
	}
	var size int64
	bm := bitmap.FromBytes(bs)
	if byteMode {
		size = int64(len(*bm))
	} else {
		size = int64(bm.BitSize())
	}
	var beg, end int
	if len(args) > 2 {
		var err2 error
		var startIdx, endIdx int64
		startIdx, err2 = strconv.ParseInt(string(args[2]), 10, 64)
		if err2 != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		endIdx, err2 = strconv.ParseInt(string(args[3]), 10, 64)
		if err2 != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		beg, end = utils.ConvertRange(startIdx, endIdx, size)
		if beg < 0 {
			return protocol.MakeIntReply(0)
		}
	}
	if byteMode {
		beg *= 8
		end *= 8
	}
	var offset = int64(-1)
	bm.ForEachBit(int64(beg), int64(end), func(o int64, val byte) bool {
		if val == v {
			offset = o
			return false
		}
		return true
	})
	return protocol.MakeIntReply(offset)
}

// GetRandomKey Randomly return (do not delete) a key from the godis
func getRandomKey(db *DB, args [][]byte) redis.Reply {
	k := db.data.RandomKeys(1)
	if len(k) == 0 {
		return &protocol.NullBulkReply{}
	}
	var key []byte
	return protocol.MakeBulkReply(strconv.AppendQuote(key, k[0]))
}

func init() {
	registerCommand("Set", execSet, writeFirstKey, rollbackFirstKey, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("SetNx", execSetNX, writeFirstKey, rollbackFirstKey, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("SetEX", execSetEX, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("PSetEX", execPSetEX, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("MSet", execMSet, prepareMSet, undoMSet, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, -1, 2)
	registerCommand("MGet", execMGet, prepareMGet, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("MSetNX", execMSetNX, prepareMSet, undoMSet, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("Get", execGet, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("GetEX", execGetEX, writeFirstKey, rollbackFirstKey, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("GetSet", execGetSet, writeFirstKey, rollbackFirstKey, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("GetDel", execGetDel, writeFirstKey, rollbackFirstKey, 2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("Incr", execIncr, writeFirstKey, rollbackFirstKey, 2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("IncrBy", execIncrBy, writeFirstKey, rollbackFirstKey, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("IncrByFloat", execIncrByFloat, writeFirstKey, rollbackFirstKey, 3, flagWrite).attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("Decr", execDecr, writeFirstKey, rollbackFirstKey, 2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("DecrBy", execDecrBy, writeFirstKey, rollbackFirstKey, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("StrLen", execStrLen, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("Append", execAppend, writeFirstKey, rollbackFirstKey, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("SetRange", execSetRange, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("GetRange", execGetRange, readFirstKey, nil, 4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("SetBit", execSetBit, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("GetBit", execGetBit, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("BitCount", execBitCount, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("BitPos", execBitPos, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("Randomkey", getRandomKey, readAllKeys, nil, 1, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagRandom}, 1, 1, 1)
}
