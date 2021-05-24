package godis

import (
	List "github.com/hdt3213/godis/datastruct/list"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/reply"
	"strconv"
)

func (db *DB) getAsList(key string) (*List.LinkedList, reply.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	bytes, ok := entity.Data.(*List.LinkedList)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return bytes, nil
}

func (db *DB) getOrInitList(key string) (list *List.LinkedList, isNew bool, errReply reply.ErrorReply) {
	list, errReply = db.getAsList(key)
	if errReply != nil {
		return nil, false, errReply
	}
	isNew = false
	if list == nil {
		list = &List.LinkedList{}
		db.PutEntity(key, &DataEntity{
			Data: list,
		})
		isNew = true
	}
	return list, isNew, nil
}

// execLIndex gets element of list at given list
func execLIndex(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	index := int(index64)

	db.RLock(key)
	defer db.RUnLock(key)

	// get entity
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return &reply.NullBulkReply{}
	}

	size := list.Len() // assert: size > 0
	if index < -1*size {
		return &reply.NullBulkReply{}
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return &reply.NullBulkReply{}
	}

	val, _ := list.Get(index).([]byte)
	return reply.MakeBulkReply(val)
}

// execLLen gets length of list
func execLLen(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])

	db.RLock(key)
	defer db.RUnLock(key)

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.MakeIntReply(0)
	}

	size := int64(list.Len())
	return reply.MakeIntReply(size)
}

// execLPop removes the first element of list, and return it
func execLPop(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])

	// lock
	db.Lock(key)
	defer db.UnLock(key)

	// get data
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return &reply.NullBulkReply{}
	}

	val, _ := list.Remove(0).([]byte)
	if list.Len() == 0 {
		db.Remove(key)
	}
	db.AddAof(makeAofCmd("lpop", args))
	return reply.MakeBulkReply(val)
}

// execLPush inserts element at head of list
func execLPush(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := args[1:]

	// lock
	db.Lock(key)
	defer db.UnLock(key)

	// get or init entity
	list, _, errReply := db.getOrInitList(key)
	if errReply != nil {
		return errReply
	}

	// insert
	for _, value := range values {
		list.Insert(0, value)
	}

	db.AddAof(makeAofCmd("lpush", args))
	return reply.MakeIntReply(int64(list.Len()))
}

// execLPushX inserts element at head of list, only if list exists
func execLPushX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := args[1:]

	// lock
	db.Lock(key)
	defer db.UnLock(key)

	// get or init entity
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.MakeIntReply(0)
	}

	// insert
	for _, value := range values {
		list.Insert(0, value)
	}
	db.AddAof(makeAofCmd("lpushx", args))
	return reply.MakeIntReply(int64(list.Len()))
}

// execLRange gets elements of list in given range
func execLRange(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	start64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	start := int(start64)
	stop64, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	stop := int(stop64)

	// lock key
	db.RLock(key)
	defer db.RUnLock(key)

	// get data
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return &reply.EmptyMultiBulkReply{}
	}

	// compute index
	size := list.Len() // assert: size > 0
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return &reply.EmptyMultiBulkReply{}
	}
	if stop < -1*size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop + 1
	} else if stop < size {
		stop = stop + 1
	} else {
		stop = size
	}
	if stop < start {
		stop = start
	}

	// assert: start in [0, size - 1], stop in [start, size]
	slice := list.Range(start, stop)
	result := make([][]byte, len(slice))
	for i, raw := range slice {
		bytes, _ := raw.([]byte)
		result[i] = bytes
	}
	return reply.MakeMultiBulkReply(result)
}

// execLRem removes element of list at specified index
func execLRem(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	count64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	count := int(count64)
	value := args[2]

	// lock
	db.Lock(key)
	defer db.UnLock(key)

	// get data entity
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.MakeIntReply(0)
	}

	var removed int
	if count == 0 {
		removed = list.RemoveAllByVal(value)
	} else if count > 0 {
		removed = list.RemoveByVal(value, count)
	} else {
		removed = list.ReverseRemoveByVal(value, -count)
	}

	if list.Len() == 0 {
		db.Remove(key)
	}
	if removed > 0 {
		db.AddAof(makeAofCmd("lrem", args))
	}

	return reply.MakeIntReply(int64(removed))
}

// execLSet puts element at specified index of list
func execLSet(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	index := int(index64)
	value := args[2]

	// lock
	db.Lock(key)
	defer db.UnLock(key)

	// get data
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.MakeErrReply("ERR no such key")
	}

	size := list.Len() // assert: size > 0
	if index < -1*size {
		return reply.MakeErrReply("ERR index out of range")
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return reply.MakeErrReply("ERR index out of range")
	}

	list.Set(index, value)
	db.AddAof(makeAofCmd("lset", args))
	return &reply.OkReply{}
}

// execRPop removes last element of list then return it
func execRPop(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])

	// lock
	db.Lock(key)
	defer db.UnLock(key)

	// get data
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return &reply.NullBulkReply{}
	}

	val, _ := list.RemoveLast().([]byte)
	if list.Len() == 0 {
		db.Remove(key)
	}
	db.AddAof(makeAofCmd("rpop", args))
	return reply.MakeBulkReply(val)
}

// execRPopLPush pops last element of list-A then insert it to the head of list-B
func execRPopLPush(db *DB, args [][]byte) redis.Reply {
	sourceKey := string(args[0])
	destKey := string(args[1])

	// lock
	db.Locks(sourceKey, destKey)
	defer db.UnLocks(sourceKey, destKey)

	// get source entity
	sourceList, errReply := db.getAsList(sourceKey)
	if errReply != nil {
		return errReply
	}
	if sourceList == nil {
		return &reply.NullBulkReply{}
	}

	// get dest entity
	destList, _, errReply := db.getOrInitList(destKey)
	if errReply != nil {
		return errReply
	}

	// pop and push
	val, _ := sourceList.RemoveLast().([]byte)
	destList.Insert(0, val)

	if sourceList.Len() == 0 {
		db.Remove(sourceKey)
	}

	db.AddAof(makeAofCmd("rpoplpush", args))
	return reply.MakeBulkReply(val)
}

// execRPush inserts element at last of list
func execRPush(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	values := args[1:]

	// lock
	db.Lock(key)
	defer db.UnLock(key)

	// get or init entity
	list, _, errReply := db.getOrInitList(key)
	if errReply != nil {
		return errReply
	}

	// put list
	for _, value := range values {
		list.Add(value)
	}
	db.AddAof(makeAofCmd("rpush", args))
	return reply.MakeIntReply(int64(list.Len()))
}

// execRPushX inserts element at last of list only if list exists
func execRPushX(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rpush' command")
	}
	key := string(args[0])
	values := args[1:]

	// lock
	db.Lock(key)
	defer db.UnLock(key)

	// get or init entity
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.MakeIntReply(0)
	}

	// put list
	for _, value := range values {
		list.Add(value)
	}
	db.AddAof(makeAofCmd("rpushx", args))

	return reply.MakeIntReply(int64(list.Len()))
}

func init() {
	RegisterCommand("LPush", execLPush, nil, -3)
	RegisterCommand("LPushX", execLPushX, nil, -3)
	RegisterCommand("RPush", execRPush, nil, -3)
	RegisterCommand("RPushX", execRPushX, nil, -3)
	RegisterCommand("LPop", execLPop, nil, 2)
	RegisterCommand("RPop", execRPop, nil, 2)
	RegisterCommand("RPopLPush", execRPopLPush, nil, 4)
	RegisterCommand("LRem", execLRem, nil, 4)
	RegisterCommand("LLen", execLLen, nil, 2)
	RegisterCommand("LIndex", execLIndex, nil, 3)
	RegisterCommand("LSet", execLSet, nil, 4)
	RegisterCommand("LRange", execLRange, nil, 4)
}
