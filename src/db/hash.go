package db

import (
	Dict "github.com/hdt3213/godis/src/datastruct/dict"
	"github.com/hdt3213/godis/src/interface/redis"
	"github.com/hdt3213/godis/src/redis/reply"
	"github.com/shopspring/decimal"
	"strconv"
)

func (db *DB) getAsDict(key string) (Dict.Dict, reply.ErrorReply) {
	entity, exists := db.Get(key)
	if !exists {
		return nil, nil
	}
	dict, ok := entity.Data.(Dict.Dict)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return dict, nil
}

func (db *DB) getOrInitDict(key string) (dict Dict.Dict, inited bool, errReply reply.ErrorReply) {
	dict, errReply = db.getAsDict(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if dict == nil {
		dict = Dict.MakeSimple()
		db.Put(key, &DataEntity{
			Data: dict,
		})
		inited = true
	}
	return dict, inited, nil
}

// HSet sets field in hash table
func HSet(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'hset' command")
	}
	key := string(args[0])
	field := string(args[1])
	value := args[2]

	// lock
	db.Lock(key)
	defer db.UnLock(key)

	// get or init entity
	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	result := dict.Put(field, value)
	db.AddAof(makeAofCmd("hset", args))
	return reply.MakeIntReply(int64(result))
}

// HSetNX sets field in hash table only if field not exists
func HSetNX(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'hsetnx' command")
	}
	key := string(args[0])
	field := string(args[1])
	value := args[2]

	db.Lock(key)
	defer db.UnLock(key)

	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	result := dict.PutIfAbsent(field, value)
	if result > 0 {
		db.AddAof(makeAofCmd("hsetnx", args))

	}
	return reply.MakeIntReply(int64(result))
}

// HGet gets field value of hash table
func HGet(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'hget' command")
	}
	key := string(args[0])
	field := string(args[1])

	// get entity
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return &reply.NullBulkReply{}
	}

	raw, exists := dict.Get(field)
	if !exists {
		return &reply.NullBulkReply{}
	}
	value, _ := raw.([]byte)
	return reply.MakeBulkReply(value)
}

// HExists checks if a hash field exists
func HExists(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'hexists' command")
	}
	key := string(args[0])
	field := string(args[1])

	// get entity
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return reply.MakeIntReply(0)
	}

	_, exists := dict.Get(field)
	if exists {
		return reply.MakeIntReply(1)
	}
	return reply.MakeIntReply(0)
}

// HDel deletes a hash field
func HDel(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'hdel' command")
	}
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}

	db.Lock(key)
	defer db.UnLock(key)

	// get entity
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return reply.MakeIntReply(0)
	}

	deleted := 0
	for _, field := range fields {
		result := dict.Remove(field)
		deleted += result
	}
	if dict.Len() == 0 {
		db.Remove(key)
	}
	if deleted > 0 {
		db.AddAof(makeAofCmd("hdel", args))
	}

	return reply.MakeIntReply(int64(deleted))
}

// HLen gets number of fields in hash table
func HLen(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'hlen' command")
	}
	key := string(args[0])

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return reply.MakeIntReply(0)
	}
	return reply.MakeIntReply(int64(dict.Len()))
}

// HMSet sets multi fields in hash table
func HMSet(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) < 3 || len(args)%2 != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'hmset' command")
	}
	key := string(args[0])
	size := (len(args) - 1) / 2
	fields := make([]string, size)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[2*i+1])
		values[i] = args[2*i+2]
	}

	// lock key
	db.locker.Lock(key)
	defer db.locker.UnLock(key)

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
	db.AddAof(makeAofCmd("hmset", args))
	return &reply.OkReply{}
}

// HMGet gets multi fields in hash table
func HMGet(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'hmget' command")
	}
	key := string(args[0])
	size := len(args) - 1
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[i+1])
	}

	db.RLock(key)
	defer db.RUnLock(key)

	// get entity
	result := make([][]byte, size)
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return reply.MakeMultiBulkReply(result)
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
	return reply.MakeMultiBulkReply(result)
}

// HKeys gets all field names in hash table
func HKeys(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'hkeys' command")
	}
	key := string(args[0])

	db.RLock(key)
	defer db.RUnLock(key)

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return &reply.EmptyMultiBulkReply{}
	}

	fields := make([][]byte, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		fields[i] = []byte(key)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(fields[:i])
}

// HVals gets all field value in hash table
func HVals(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'hvals' command")
	}
	key := string(args[0])

	db.RLock(key)
	defer db.RUnLock(key)

	// get entity
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return &reply.EmptyMultiBulkReply{}
	}

	values := make([][]byte, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		values[i], _ = val.([]byte)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(values[:i])
}

// HGetAll gets all key-value entries in hash table
func HGetAll(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'hgetAll' command")
	}
	key := string(args[0])

	db.RLock(key)
	defer db.RUnLock(key)

	// get entity
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return &reply.EmptyMultiBulkReply{}
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
	return reply.MakeMultiBulkReply(result[:i])
}

// HIncrBy increments the integer value of a hash field by the given number
func HIncrBy(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'hincrby' command")
	}
	key := string(args[0])
	field := string(args[1])
	rawDelta := string(args[2])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}

	db.locker.Lock(key)
	defer db.locker.UnLock(key)

	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	value, exists := dict.Get(field)
	if !exists {
		dict.Put(field, args[2])
		db.AddAof(makeAofCmd("hincrby", args))
		return reply.MakeBulkReply(args[2])
	}
	val, err := strconv.ParseInt(string(value.([]byte)), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR hash value is not an integer")
	}
	val += delta
	bytes := []byte(strconv.FormatInt(val, 10))
	dict.Put(field, bytes)
	db.AddAof(makeAofCmd("hincrby", args))
	return reply.MakeBulkReply(bytes)
}

// HIncrByFloat increments the float value of a hash field by the given number
func HIncrByFloat(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'hincrbyfloat' command")
	}
	key := string(args[0])
	field := string(args[1])
	rawDelta := string(args[2])
	delta, err := decimal.NewFromString(rawDelta)
	if err != nil {
		return reply.MakeErrReply("ERR value is not a valid float")
	}

	db.Lock(key)
	defer db.UnLock(key)

	// get or init entity
	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	value, exists := dict.Get(field)
	if !exists {
		dict.Put(field, args[2])
		return reply.MakeBulkReply(args[2])
	}
	val, err := decimal.NewFromString(string(value.([]byte)))
	if err != nil {
		return reply.MakeErrReply("ERR hash value is not a float")
	}
	result := val.Add(delta)
	resultBytes := []byte(result.String())
	dict.Put(field, resultBytes)
	db.AddAof(makeAofCmd("hincrbyfloat", args))
	return reply.MakeBulkReply(resultBytes)
}
