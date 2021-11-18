package database

import (
	List "github.com/hdt3213/godis/datastruct/list"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
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
		db.PutEntity(key, &database.DataEntity{
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
	db.addAof(utils.ToCmdLine3("lpop", args...))
	return reply.MakeBulkReply(val)
}

var lPushCmd = []byte("LPUSH")

func undoLPop(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}
	if list == nil || list.Len() == 0 {
		return nil
	}
	element, _ := list.Get(0).([]byte)
	return []CmdLine{
		{
			lPushCmd,
			args[0],
			element,
		},
	}
}

// execLPush inserts element at head of list
func execLPush(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := args[1:]

	// get or init entity
	list, _, errReply := db.getOrInitList(key)
	if errReply != nil {
		return errReply
	}

	// insert
	for _, value := range values {
		list.Insert(0, value)
	}

	db.addAof(utils.ToCmdLine3("lpush", args...))
	return reply.MakeIntReply(int64(list.Len()))
}

func undoLPush(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	count := len(args) - 1
	cmdLines := make([]CmdLine, 0, count)
	for i := 0; i < count; i++ {
		cmdLines = append(cmdLines, utils.ToCmdLine("LPOP", key))
	}
	return cmdLines
}

// execLPushX inserts element at head of list, only if list exists
func execLPushX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := args[1:]

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
	db.addAof(utils.ToCmdLine3("lpushx", args...))
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
		db.addAof(utils.ToCmdLine3("lrem", args...))
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
	db.addAof(utils.ToCmdLine3("lset", args...))
	return &reply.OkReply{}
}

func undoLSet(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return nil
	}
	index := int(index64)
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}
	if list == nil {
		return nil
	}
	size := list.Len() // assert: size > 0
	if index < -1*size {
		return nil
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return nil
	}
	value, _ := list.Get(index).([]byte)
	return []CmdLine{
		{
			[]byte("LSET"),
			args[0],
			args[1],
			value,
		},
	}
}

// execRPop removes last element of list then return it
func execRPop(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])

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
	db.addAof(utils.ToCmdLine3("rpop", args...))
	return reply.MakeBulkReply(val)
}

var rPushCmd = []byte("RPUSH")

func undoRPop(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}
	if list == nil || list.Len() == 0 {
		return nil
	}
	element, _ := list.Get(list.Len() - 1).([]byte)
	return []CmdLine{
		{
			rPushCmd,
			args[0],
			element,
		},
	}
}

func prepareRPopLPush(args [][]byte) ([]string, []string) {
	return []string{
		string(args[0]),
		string(args[1]),
	}, nil
}

// execRPopLPush pops last element of list-A then insert it to the head of list-B
func execRPopLPush(db *DB, args [][]byte) redis.Reply {
	sourceKey := string(args[0])
	destKey := string(args[1])

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

	db.addAof(utils.ToCmdLine3("rpoplpush", args...))
	return reply.MakeBulkReply(val)
}

func undoRPopLPush(db *DB, args [][]byte) []CmdLine {
	sourceKey := string(args[0])
	list, errReply := db.getAsList(sourceKey)
	if errReply != nil {
		return nil
	}
	if list == nil || list.Len() == 0 {
		return nil
	}
	element, _ := list.Get(list.Len() - 1).([]byte)
	return []CmdLine{
		{
			rPushCmd,
			args[0],
			element,
		},
		{
			[]byte("LPOP"),
			args[1],
		},
	}
}

// execRPush inserts element at last of list
func execRPush(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	values := args[1:]

	// get or init entity
	list, _, errReply := db.getOrInitList(key)
	if errReply != nil {
		return errReply
	}

	// put list
	for _, value := range values {
		list.Add(value)
	}
	db.addAof(utils.ToCmdLine3("rpush", args...))
	return reply.MakeIntReply(int64(list.Len()))
}

func undoRPush(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	count := len(args) - 1
	cmdLines := make([]CmdLine, 0, count)
	for i := 0; i < count; i++ {
		cmdLines = append(cmdLines, utils.ToCmdLine("RPOP", key))
	}
	return cmdLines
}

// execRPushX inserts element at last of list only if list exists
func execRPushX(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rpush' command")
	}
	key := string(args[0])
	values := args[1:]

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
	db.addAof(utils.ToCmdLine3("rpushx", args...))

	return reply.MakeIntReply(int64(list.Len()))
}

func init() {
	RegisterCommand("LPush", execLPush, writeFirstKey, undoLPush, -3)
	RegisterCommand("LPushX", execLPushX, writeFirstKey, undoLPush, -3)
	RegisterCommand("RPush", execRPush, writeFirstKey, undoRPush, -3)
	RegisterCommand("RPushX", execRPushX, writeFirstKey, undoRPush, -3)
	RegisterCommand("LPop", execLPop, writeFirstKey, undoLPop, 2)
	RegisterCommand("RPop", execRPop, writeFirstKey, undoRPop, 2)
	RegisterCommand("RPopLPush", execRPopLPush, prepareRPopLPush, undoRPopLPush, 3)
	RegisterCommand("LRem", execLRem, writeFirstKey, rollbackFirstKey, 4)
	RegisterCommand("LLen", execLLen, readFirstKey, nil, 2)
	RegisterCommand("LIndex", execLIndex, readFirstKey, nil, 3)
	RegisterCommand("LSet", execLSet, writeFirstKey, undoLSet, 4)
	RegisterCommand("LRange", execLRange, readFirstKey, nil, 4)
}
