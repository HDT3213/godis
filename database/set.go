package database

import (
	HashSet "github.com/hdt3213/godis/datastruct/set"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
	"strconv"
)

func (db *DB) getAsSet(key string) (*HashSet.Set, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	set, ok := entity.Data.(*HashSet.Set)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return set, nil
}

func (db *DB) getOrInitSet(key string) (set *HashSet.Set, inited bool, errReply protocol.ErrorReply) {
	set, errReply = db.getAsSet(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if set == nil {
		set = HashSet.Make()
		db.PutEntity(key, &database.DataEntity{
			Data: set,
		})
		inited = true
	}
	return set, inited, nil
}

// execSAdd adds members into set
func execSAdd(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	members := args[1:]

	// get or init entity
	set, _, errReply := db.getOrInitSet(key)
	if errReply != nil {
		return errReply
	}
	counter := 0
	for _, member := range members {
		counter += set.Add(string(member))
	}
	db.addAof(utils.ToCmdLine3("sadd", args...))
	return protocol.MakeIntReply(int64(counter))
}

// execSIsMember checks if the given value is member of set
func execSIsMember(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	member := string(args[1])

	// get set
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.MakeIntReply(0)
	}

	has := set.Has(member)
	if has {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// execSRem removes a member from set
func execSRem(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	members := args[1:]

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.MakeIntReply(0)
	}
	counter := 0
	for _, member := range members {
		counter += set.Remove(string(member))
	}
	if set.Len() == 0 {
		db.Remove(key)
	}
	if counter > 0 {
		db.addAof(utils.ToCmdLine3("srem", args...))
	}
	return protocol.MakeIntReply(int64(counter))
}

// execSPop removes one or more random members from set
func execSPop(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 && len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'spop' command")
	}
	key := string(args[0])

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return &protocol.NullBulkReply{}
	}

	count := 1
	if len(args) == 2 {
		count64, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil || count64 <= 0 {
			return protocol.MakeErrReply("ERR value is out of range, must be positive")
		}
		count = int(count64)
	}
	if count > set.Len() {
		count = set.Len()
	}

	members := set.RandomDistinctMembers(count)
	result := make([][]byte, len(members))
	for i, v := range members {
		set.Remove(v)
		result[i] = []byte(v)
	}

	if count > 0 {
		db.addAof(utils.ToCmdLine3("spop", args...))
	}
	return protocol.MakeMultiBulkReply(result)
}

// execSCard gets the number of members in a set
func execSCard(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	// get or init entity
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(set.Len()))
}

// execSMembers gets all members in a set
func execSMembers(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	// get or init entity
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	arr := make([][]byte, set.Len())
	i := 0
	set.ForEach(func(member string) bool {
		arr[i] = []byte(member)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(arr)
}

func set2reply(set *HashSet.Set) redis.Reply {
	arr := make([][]byte, set.Len())
	i := 0
	set.ForEach(func(member string) bool {
		arr[i] = []byte(member)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(arr)
}

// execSInter intersect multiple sets
func execSInter(db *DB, args [][]byte) redis.Reply {
	sets := make([]*HashSet.Set, 0, len(args))
	for _, arg := range args {
		key := string(arg)
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set.Len() == 0 {
			return &protocol.EmptyMultiBulkReply{}
		}
		sets = append(sets, set)
	}
	result := HashSet.Intersect(sets...)
	return set2reply(result)
}

// execSInterStore intersects multiple sets and store the result in a key
func execSInterStore(db *DB, args [][]byte) redis.Reply {
	dest := string(args[0])
	sets := make([]*HashSet.Set, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		key := string(args[i])
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set.Len() == 0 {
			return protocol.MakeIntReply(0)
		}
		sets = append(sets, set)
	}
	result := HashSet.Intersect(sets...)

	db.PutEntity(dest, &database.DataEntity{
		Data: result,
	})
	db.addAof(utils.ToCmdLine3("sinterstore", args...))
	return protocol.MakeIntReply(int64(result.Len()))
}

// execSUnion adds multiple sets
func execSUnion(db *DB, args [][]byte) redis.Reply {
	sets := make([]*HashSet.Set, 0, len(args))
	for _, arg := range args {
		key := string(arg)
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		sets = append(sets, set)
	}
	result := HashSet.Union(sets...)
	return set2reply(result)
}

// execSUnionStore adds multiple sets and store the result in a key
func execSUnionStore(db *DB, args [][]byte) redis.Reply {
	dest := string(args[0])
	sets := make([]*HashSet.Set, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		key := string(args[i])
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		sets = append(sets, set)
	}
	result := HashSet.Union(sets...)
	db.Remove(dest) // clean ttl
	if result.Len() == 0 {
		return protocol.MakeIntReply(0)
	}

	db.PutEntity(dest, &database.DataEntity{
		Data: result,
	})
	db.addAof(utils.ToCmdLine3("sunionstore", args...))
	return protocol.MakeIntReply(int64(result.Len()))
}

// execSDiff subtracts multiple sets
func execSDiff(db *DB, args [][]byte) redis.Reply {
	sets := make([]*HashSet.Set, 0, len(args))
	for _, arg := range args {
		key := string(arg)
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		sets = append(sets, set)
	}
	result := HashSet.Diff(sets...)
	return set2reply(result)
}

// execSDiffStore subtracts multiple sets and store the result in a key
func execSDiffStore(db *DB, args [][]byte) redis.Reply {
	dest := string(args[0])
	sets := make([]*HashSet.Set, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		key := string(args[i])
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		sets = append(sets, set)
	}
	result := HashSet.Diff(sets...)
	db.Remove(dest) // clean ttl
	if result.Len() == 0 {
		return protocol.MakeIntReply(0)
	}
	db.PutEntity(dest, &database.DataEntity{
		Data: result,
	})
	db.addAof(utils.ToCmdLine3("sdiffstore", args...))
	return protocol.MakeIntReply(int64(result.Len()))
}

// execSRandMember gets random members from set
func execSRandMember(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 && len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'srandmember' command")
	}
	key := string(args[0])

	// get or init entity
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return &protocol.NullBulkReply{}
	}
	if len(args) == 1 {
		// get a random member
		members := set.RandomMembers(1)
		return protocol.MakeBulkReply([]byte(members[0]))
	}
	count64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	count := int(count64)
	if count > 0 {
		members := set.RandomDistinctMembers(count)
		result := make([][]byte, len(members))
		for i, v := range members {
			result[i] = []byte(v)
		}
		return protocol.MakeMultiBulkReply(result)
	} else if count < 0 {
		members := set.RandomMembers(-count)
		result := make([][]byte, len(members))
		for i, v := range members {
			result[i] = []byte(v)
		}
		return protocol.MakeMultiBulkReply(result)
	}
	return &protocol.EmptyMultiBulkReply{}
}

func init() {
	registerCommand("SAdd", execSAdd, writeFirstKey, undoSetChange, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("SIsMember", execSIsMember, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("SRem", execSRem, writeFirstKey, undoSetChange, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("SPop", execSPop, writeFirstKey, undoSetChange, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagRandom, redisFlagFast}, 1, 1, 1)
	registerCommand("SCard", execSCard, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("SMembers", execSMembers, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)
	registerCommand("SInter", execSInter, prepareSetCalculate, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, -1, 1)
	registerCommand("SInterStore", execSInterStore, prepareSetCalculateStore, rollbackFirstKey, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, -1, 1)
	registerCommand("SUnion", execSUnion, prepareSetCalculate, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, -1, 1)
	registerCommand("SUnionStore", execSUnionStore, prepareSetCalculateStore, rollbackFirstKey, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, -1, 1)
	registerCommand("SDiff", execSDiff, prepareSetCalculate, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)
	registerCommand("SDiffStore", execSDiffStore, prepareSetCalculateStore, rollbackFirstKey, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("SRandMember", execSRandMember, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagRandom}, 1, 1, 1)
}
