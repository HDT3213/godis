package database

import (
	Dict "github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
	"strconv"
	"strings"
)

func (db *DB) getAsDict(key string) (Dict.Dict, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	dict, ok := entity.Data.(Dict.Dict)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return dict, nil
}

func (db *DB) getOrInitDict(key string) (dict Dict.Dict, inited bool, errReply protocol.ErrorReply) {
	dict, errReply = db.getAsDict(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if dict == nil {
		dict = Dict.MakeSimple()
		db.PutEntity(key, &database.DataEntity{
			Data: dict,
		})
		inited = true
	}
	return dict, inited, nil
}

// execHSet sets field in hash table
func execHSet(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	field := string(args[1])
	value := args[2]

	// get or init entity
	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	result := dict.Put(field, value)
	db.addAof(utils.ToCmdLine3("hset", args...))
	return protocol.MakeIntReply(int64(result))
}

func undoHSet(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	field := string(args[1])
	return rollbackHashFields(db, key, field)
}

// execHSetNX sets field in hash table only if field not exists
func execHSetNX(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	field := string(args[1])
	value := args[2]

	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	result := dict.PutIfAbsent(field, value)
	if result > 0 {
		db.addAof(utils.ToCmdLine3("hsetnx", args...))

	}
	return protocol.MakeIntReply(int64(result))
}

// execHGet gets field value of hash table
func execHGet(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	field := string(args[1])

	// get entity
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return &protocol.NullBulkReply{}
	}

	raw, exists := dict.Get(field)
	if !exists {
		return &protocol.NullBulkReply{}
	}
	value, _ := raw.([]byte)
	return protocol.MakeBulkReply(value)
}

// execHExists checks if a hash field exists
func execHExists(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	field := string(args[1])

	// get entity
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeIntReply(0)
	}

	_, exists := dict.Get(field)
	if exists {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// execHDel deletes a hash field
func execHDel(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}

	// get entity
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeIntReply(0)
	}

	deleted := 0
	for _, field := range fields {
		_, result := dict.Remove(field)
		deleted += result
	}
	if dict.Len() == 0 {
		db.Remove(key)
	}
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("hdel", args...))
	}

	return protocol.MakeIntReply(int64(deleted))
}

func undoHDel(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}
	return rollbackHashFields(db, key, fields...)
}

// execHLen gets number of fields in hash table
func execHLen(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(dict.Len()))
}

// execHStrlen Returns the string length of the value associated with field in the hash stored at key.
// If the key or the field do not exist, 0 is returned.
func execHStrlen(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeIntReply(0)
	}

	raw, exists := dict.Get(field)
	if exists {
		value, _ := raw.([]byte)
		return protocol.MakeIntReply(int64(len(value)))
	}
	return protocol.MakeIntReply(0)
}

// execHMSet sets multi fields in hash table
func execHMSet(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args)%2 != 1 {
		return protocol.MakeSyntaxErrReply()
	}
	key := string(args[0])
	size := (len(args) - 1) / 2
	fields := make([]string, size)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[2*i+1])
		values[i] = args[2*i+2]
	}

	// get or init entity
	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	// put data
	for i, field := range fields {
		value := values[i]
		dict.Put(field, value)
	}
	db.addAof(utils.ToCmdLine3("hmset", args...))
	return &protocol.OkReply{}
}

func undoHMSet(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	size := (len(args) - 1) / 2
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[2*i+1])
	}
	return rollbackHashFields(db, key, fields...)
}

// execHMGet gets multi fields in hash table
func execHMGet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	size := len(args) - 1
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[i+1])
	}

	// get entity
	result := make([][]byte, size)
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeMultiBulkReply(result)
	}

	for i, field := range fields {
		value, ok := dict.Get(field)
		if !ok {
			result[i] = nil
		} else {
			bytes, _ := value.([]byte)
			result[i] = bytes
		}
	}
	return protocol.MakeMultiBulkReply(result)
}

// execHKeys gets all field names in hash table
func execHKeys(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	fields := make([][]byte, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		fields[i] = []byte(key)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(fields[:i])
}

// execHVals gets all field value in hash table
func execHVals(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	// get entity
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	values := make([][]byte, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		values[i], _ = val.([]byte)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(values[:i])
}

// execHGetAll gets all key-value entries in hash table
func execHGetAll(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	// get entity
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	size := dict.Len()
	result := make([][]byte, size*2)
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		result[i] = []byte(key)
		i++
		result[i], _ = val.([]byte)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(result[:i])
}

// execHIncrBy increments the integer value of a hash field by the given number
func execHIncrBy(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	rawDelta := string(args[2])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	value, exists := dict.Get(field)
	if !exists {
		dict.Put(field, args[2])
		db.addAof(utils.ToCmdLine3("hincrby", args...))
		return protocol.MakeBulkReply(args[2])
	}
	val, err := strconv.ParseInt(string(value.([]byte)), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR hash value is not an integer")
	}
	val += delta
	bytes := []byte(strconv.FormatInt(val, 10))
	dict.Put(field, bytes)
	db.addAof(utils.ToCmdLine3("hincrby", args...))
	return protocol.MakeBulkReply(bytes)
}

func undoHIncr(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	field := string(args[1])
	return rollbackHashFields(db, key, field)
}

// execHIncrByFloat increments the float value of a hash field by the given number
func execHIncrByFloat(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	rawDelta := string(args[2])
	delta, err := strconv.ParseFloat(rawDelta, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not a valid float")
	}

	// get or init entity
	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	value, exists := dict.Get(field)
	if !exists {
		dict.Put(field, args[2])
		return protocol.MakeBulkReply(args[2])
	}
	val, err := strconv.ParseFloat(string(value.([]byte)), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR hash value is not a float")
	}
	result := val + delta
	resultBytes := []byte(strconv.FormatFloat(result, 'f', -1, 64))
	dict.Put(field, resultBytes)
	db.addAof(utils.ToCmdLine3("hincrbyfloat", args...))
	return protocol.MakeBulkReply(resultBytes)
}

// execHRandField return a random field(or field-value) from the hash value stored at key.
func execHRandField(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	count := 1
	withvalues := 0

	if len(args) > 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hrandfield' command")
	}

	if len(args) == 3 {
		if strings.ToLower(string(args[2])) == "withvalues" {
			withvalues = 1
		} else {
			return protocol.MakeSyntaxErrReply()
		}
	}

	if len(args) >= 2 {
		count64, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		count = int(count64)
	}

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	if count > 0 {
		fields := dict.RandomDistinctKeys(count)
		Numfield := len(fields)
		if withvalues == 0 {
			result := make([][]byte, Numfield)
			for i, v := range fields {
				result[i] = []byte(v)
			}
			return protocol.MakeMultiBulkReply(result)
		} else {
			result := make([][]byte, 2*Numfield)
			for i, v := range fields {
				result[2*i] = []byte(v)
				raw, _ := dict.Get(v)
				result[2*i+1] = raw.([]byte)
			}
			return protocol.MakeMultiBulkReply(result)
		}
	} else if count < 0 {
		fields := dict.RandomKeys(-count)
		Numfield := len(fields)
		if withvalues == 0 {
			result := make([][]byte, Numfield)
			for i, v := range fields {
				result[i] = []byte(v)
			}
			return protocol.MakeMultiBulkReply(result)
		} else {
			result := make([][]byte, 2*Numfield)
			for i, v := range fields {
				result[2*i] = []byte(v)
				raw, _ := dict.Get(v)
				result[2*i+1] = raw.([]byte)
			}
			return protocol.MakeMultiBulkReply(result)
		}
	}

	// 'count' is 0 will reach.
	return &protocol.EmptyMultiBulkReply{}
}

func init() {
	registerCommand("HSet", execHSet, writeFirstKey, undoHSet, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HSetNX", execHSetNX, writeFirstKey, undoHSet, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HGet", execHGet, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HExists", execHExists, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HDel", execHDel, writeFirstKey, undoHDel, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("HLen", execHLen, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HStrlen", execHStrlen, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HMSet", execHMSet, writeFirstKey, undoHMSet, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HMGet", execHMGet, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HGet", execHGet, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HKeys", execHKeys, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)
	registerCommand("HVals", execHVals, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)
	registerCommand("HGetAll", execHGetAll, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagRandom}, 1, 1, 1)
	registerCommand("HIncrBy", execHIncrBy, writeFirstKey, undoHIncr, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HIncrByFloat", execHIncrByFloat, writeFirstKey, undoHIncr, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HRandField", execHRandField, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagRandom, redisFlagReadonly}, 1, 1, 1)
}
