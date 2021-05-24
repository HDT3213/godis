package godis

import (
	Dict "github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/reply"
	"github.com/shopspring/decimal"
	"strconv"
)

func (db *DB) getAsDict(key string) (Dict.Dict, reply.ErrorReply) {
	entity, exists := db.GetEntity(key)
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
		db.PutEntity(key, &DataEntity{
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

// execHSetNX sets field in hash table only if field not exists
func execHSetNX(db *DB, args [][]byte) redis.Reply {
	// parse args
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

// execHGet gets field value of hash table
func execHGet(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	field := string(args[1])

	db.RLock(key)
	defer db.RUnLock(key)

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

// execHExists checks if a hash field exists
func execHExists(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	field := string(args[1])

	db.RLock(key)
	defer db.RUnLock(key)

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

// execHDel deletes a hash field
func execHDel(db *DB, args [][]byte) redis.Reply {
	// parse args
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

// execHLen gets number of fields in hash table
func execHLen(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])

	db.RLock(key)
	defer db.RUnLock(key)

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return reply.MakeIntReply(0)
	}
	return reply.MakeIntReply(int64(dict.Len()))
}

// execHMSet sets multi fields in hash table
func execHMSet(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args)%2 != 1 {
		return reply.MakeSyntaxErrReply()
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
	db.Lock(key)
	defer db.UnLock(key)

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

// execHKeys gets all field names in hash table
func execHKeys(db *DB, args [][]byte) redis.Reply {
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

// execHVals gets all field value in hash table
func execHVals(db *DB, args [][]byte) redis.Reply {
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

// execHGetAll gets all key-value entries in hash table
func execHGetAll(db *DB, args [][]byte) redis.Reply {
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

// execHIncrBy increments the integer value of a hash field by the given number
func execHIncrBy(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	rawDelta := string(args[2])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}

	db.Lock(key)
	defer db.UnLock(key)

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

// execHIncrByFloat increments the float value of a hash field by the given number
func execHIncrByFloat(db *DB, args [][]byte) redis.Reply {
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

func init() {
	RegisterCommand("HSet", execHSet, nil, 4)
	RegisterCommand("HSetNX", execHSetNX, nil, 4)
	RegisterCommand("HGet", execHGet, nil, 3)
	RegisterCommand("HExists", execHExists, nil, 3)
	RegisterCommand("HDel", execHDel, nil, -3)
	RegisterCommand("HLen", execHLen, nil, 2)
	RegisterCommand("HMSet", execHMSet, nil, -4)
	RegisterCommand("HGet", execHGet, nil, -3)
	RegisterCommand("HKeys", execHKeys, nil, 2)
	RegisterCommand("HVals", execHVals, nil, 2)
	RegisterCommand("HGetAll", execHGetAll, nil, 2)
	RegisterCommand("HIncrBy", execHIncrBy, nil, 4)
	RegisterCommand("HIncrByFloat", execHIncrByFloat, nil, 4)
}
