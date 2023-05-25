package database

import (
	"fmt"
	"strconv"
	"strings"

	List "github.com/hdt3213/godis/datastruct/list"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

func (db *DB) getAsList(key string) (List.List, protocol.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	list, ok := entity.Data.(List.List)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return list, nil
}

func (db *DB) getOrInitList(key string) (list List.List, isNew bool, errReply protocol.ErrorReply) {
	list, errReply = db.getAsList(key)
	if errReply != nil {
		return nil, false, errReply
	}
	isNew = false
	if list == nil {
		list = List.NewQuickList()
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
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	index := int(index64)

	// get entity
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return &protocol.NullBulkReply{}
	}

	size := list.Len() // assert: size > 0
	if index < -1*size {
		return &protocol.NullBulkReply{}
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return &protocol.NullBulkReply{}
	}

	val, _ := list.Get(index).([]byte)
	return protocol.MakeBulkReply(val)
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
		return protocol.MakeIntReply(0)
	}

	size := int64(list.Len())
	return protocol.MakeIntReply(size)
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
		return &protocol.NullBulkReply{}
	}

	val, _ := list.Remove(0).([]byte)
	if list.Len() == 0 {
		db.Remove(key)
	}
	db.addAof(utils.ToCmdLine3("lpop", args...))
	return protocol.MakeBulkReply(val)
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
	return protocol.MakeIntReply(int64(list.Len()))
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
		return protocol.MakeIntReply(0)
	}

	// insert
	for _, value := range values {
		list.Insert(0, value)
	}
	db.addAof(utils.ToCmdLine3("lpushx", args...))
	return protocol.MakeIntReply(int64(list.Len()))
}

// execLRange gets elements of list in given range
func execLRange(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	start64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	start := int(start64)
	stop64, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	stop := int(stop64)

	// get data
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	// compute index
	size := list.Len() // assert: size > 0
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return &protocol.EmptyMultiBulkReply{}
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
	return protocol.MakeMultiBulkReply(result)
}

// execLRem removes element of list at specified index
func execLRem(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	count64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	count := int(count64)
	value := args[2]

	// get data entity
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeIntReply(0)
	}

	var removed int
	if count == 0 {
		removed = list.RemoveAllByVal(func(a interface{}) bool {
			return utils.Equals(a, value)
		})
	} else if count > 0 {
		removed = list.RemoveByVal(func(a interface{}) bool {
			return utils.Equals(a, value)
		}, count)
	} else {
		removed = list.ReverseRemoveByVal(func(a interface{}) bool {
			return utils.Equals(a, value)
		}, -count)
	}

	if list.Len() == 0 {
		db.Remove(key)
	}
	if removed > 0 {
		db.addAof(utils.ToCmdLine3("lrem", args...))
	}

	return protocol.MakeIntReply(int64(removed))
}

// execLSet puts element at specified index of list
func execLSet(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	index := int(index64)
	value := args[2]

	// get data
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeErrReply("ERR no such key")
	}

	size := list.Len() // assert: size > 0
	if index < -1*size {
		return protocol.MakeErrReply("ERR index out of range")
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return protocol.MakeErrReply("ERR index out of range")
	}

	list.Set(index, value)
	db.addAof(utils.ToCmdLine3("lset", args...))
	return &protocol.OkReply{}
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
		return &protocol.NullBulkReply{}
	}

	val, _ := list.RemoveLast().([]byte)
	if list.Len() == 0 {
		db.Remove(key)
	}
	db.addAof(utils.ToCmdLine3("rpop", args...))
	return protocol.MakeBulkReply(val)
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
		return &protocol.NullBulkReply{}
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
	return protocol.MakeBulkReply(val)
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
	return protocol.MakeIntReply(int64(list.Len()))
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
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rpush' command")
	}
	key := string(args[0])
	values := args[1:]

	// get or init entity
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeIntReply(0)
	}

	// put list
	for _, value := range values {
		list.Add(value)
	}
	db.addAof(utils.ToCmdLine3("rpushx", args...))

	return protocol.MakeIntReply(int64(list.Len()))
}

// execLTrim removes elements from both ends a list. delete the list if all elements were trimmmed.
func execLTrim(db *DB, args [][]byte) redis.Reply {
	n := len(args)
	if n != 3 {
		return protocol.MakeErrReply(fmt.Sprintf("ERR wrong number of arguments (given %d, expected 3)", n))
	}
	key := string(args[0])
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	end, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	// get or init entity
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeOkReply()
	}

	length := list.Len()
	if start < 0 {
		start += length
	}
	if end < 0 {
		end += length
	}

	leftCount := start
	rightCount := length - end - 1

	for i := 0; i < leftCount && list.Len() > 0; i++ {
		list.Remove(0)
	}
	for i := 0; i < rightCount && list.Len() > 0; i++ {
		list.RemoveLast()
	}

	db.addAof(utils.ToCmdLine3("ltrim", args...))

	return protocol.MakeOkReply()
}

func execLInsert(db *DB, args [][]byte) redis.Reply {
	n := len(args)
	if n != 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'linsert' command")
	}
	key := string(args[0])
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeIntReply(0)
	}

	dir := strings.ToLower(string(args[1]))
	if dir != "before" && dir != "after" {
		return protocol.MakeErrReply("ERR syntax error")
	}

	pivot := string(args[2])
	index := -1
	list.ForEach(func(i int, v interface{}) bool {
		if string(v.([]byte)) == pivot {
			index = i
			return false
		}
		return true
	})
	if index == -1 {
		return protocol.MakeIntReply(-1)
	}

	val := args[3]
	if dir == "before" {
		list.Insert(index, val)
	} else {
		list.Insert(index+1, val)
	}

	db.addAof(utils.ToCmdLine3("linsert", args...))

	return protocol.MakeIntReply(int64(list.Len()))
}

func init() {
	registerCommand("LPush", execLPush, writeFirstKey, undoLPush, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("LPushX", execLPushX, writeFirstKey, undoLPush, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("RPush", execRPush, writeFirstKey, undoRPush, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("RPushX", execRPushX, writeFirstKey, undoRPush, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("LPop", execLPop, writeFirstKey, undoLPop, 2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("RPop", execRPop, writeFirstKey, undoRPop, 2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("RPopLPush", execRPopLPush, prepareRPopLPush, undoRPopLPush, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("LRem", execLRem, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("LLen", execLLen, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("LIndex", execLIndex, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("LSet", execLSet, writeFirstKey, undoLSet, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("LRange", execLRange, readFirstKey, nil, 4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("LTrim", execLTrim, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("LInsert", execLInsert, writeFirstKey, rollbackFirstKey, 5, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
}
