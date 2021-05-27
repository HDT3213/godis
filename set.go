package godis

import (
	HashSet "github.com/hdt3213/godis/datastruct/set"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/reply"
	"strconv"
)

func (db *DB) getAsSet(key string) (*HashSet.Set, reply.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	set, ok := entity.Data.(*HashSet.Set)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return set, nil
}

func (db *DB) getOrInitSet(key string) (set *HashSet.Set, inited bool, errReply reply.ErrorReply) {
	set, errReply = db.getAsSet(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if set == nil {
		set = HashSet.Make()
		db.PutEntity(key, &DataEntity{
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

	// lock
	db.Lock(key)
	defer db.UnLock(key)

	// get or init entity
	set, _, errReply := db.getOrInitSet(key)
	if errReply != nil {
		return errReply
	}
	counter := 0
	for _, member := range members {
		counter += set.Add(string(member))
	}
	db.AddAof(makeAofCmd("sadd", args))
	return reply.MakeIntReply(int64(counter))
}

// execSIsMember checks if the given value is member of set
func execSIsMember(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	member := string(args[1])

	db.RLock(key)
	defer db.RUnLock(key)

	// get set
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return reply.MakeIntReply(0)
	}

	has := set.Has(member)
	if has {
		return reply.MakeIntReply(1)
	}
	return reply.MakeIntReply(0)
}

// execSRem removes a member from set
func execSRem(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	members := args[1:]

	// lock
	db.Lock(key)
	defer db.UnLock(key)

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return reply.MakeIntReply(0)
	}
	counter := 0
	for _, member := range members {
		counter += set.Remove(string(member))
	}
	if set.Len() == 0 {
		db.Remove(key)
	}
	if counter > 0 {
		db.AddAof(makeAofCmd("srem", args))
	}
	return reply.MakeIntReply(int64(counter))
}

// execSCard gets the number of members in a set
func execSCard(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	db.RLock(key)
	defer db.RUnLock(key)

	// get or init entity
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return reply.MakeIntReply(0)
	}
	return reply.MakeIntReply(int64(set.Len()))
}

// execSMembers gets all members in a set
func execSMembers(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	// lock
	db.RLock(key)
	defer db.RUnLock(key)

	// get or init entity
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return &reply.EmptyMultiBulkReply{}
	}

	arr := make([][]byte, set.Len())
	i := 0
	set.ForEach(func(member string) bool {
		arr[i] = []byte(member)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(arr)
}

// execSInter intersect multiple sets
func execSInter(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}

	// lock
	db.RLocks(keys...)
	defer db.RUnLocks(keys...)

	var result *HashSet.Set
	for _, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set == nil {
			return &reply.EmptyMultiBulkReply{}
		}

		if result == nil {
			// init
			result = HashSet.Make(set.ToSlice()...)
		} else {
			result = result.Intersect(set)
			if result.Len() == 0 {
				// early termination
				return &reply.EmptyMultiBulkReply{}
			}
		}
	}

	arr := make([][]byte, result.Len())
	i := 0
	result.ForEach(func(member string) bool {
		arr[i] = []byte(member)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(arr)
}

// execSInterStore intersects multiple sets and store the result in a key
func execSInterStore(db *DB, args [][]byte) redis.Reply {
	dest := string(args[0])
	keys := make([]string, len(args)-1)
	keyArgs := args[1:]
	for i, arg := range keyArgs {
		keys[i] = string(arg)
	}

	// lock
	db.RWLocks([]string{dest}, keys)
	defer db.RWUnLocks([]string{dest}, keys)

	var result *HashSet.Set
	for _, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set == nil {
			db.Remove(dest) // clean ttl and old value
			return reply.MakeIntReply(0)
		}

		if result == nil {
			// init
			result = HashSet.Make(set.ToSlice()...)
		} else {
			result = result.Intersect(set)
			if result.Len() == 0 {
				// early termination
				db.Remove(dest) // clean ttl and old value
				return reply.MakeIntReply(0)
			}
		}
	}

	set := HashSet.Make(result.ToSlice()...)
	db.PutEntity(dest, &DataEntity{
		Data: set,
	})
	db.AddAof(makeAofCmd("sinterstore", args))
	return reply.MakeIntReply(int64(set.Len()))
}

// execSUnion adds multiple sets
func execSUnion(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}

	// lock
	db.RLocks(keys...)
	defer db.RUnLocks(keys...)

	var result *HashSet.Set
	for _, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set == nil {
			continue
		}

		if result == nil {
			// init
			result = HashSet.Make(set.ToSlice()...)
		} else {
			result = result.Union(set)
		}
	}

	if result == nil {
		// all keys are empty set
		return &reply.EmptyMultiBulkReply{}
	}
	arr := make([][]byte, result.Len())
	i := 0
	result.ForEach(func(member string) bool {
		arr[i] = []byte(member)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(arr)
}

// execSUnionStore adds multiple sets and store the result in a key
func execSUnionStore(db *DB, args [][]byte) redis.Reply {
	dest := string(args[0])
	keys := make([]string, len(args)-1)
	keyArgs := args[1:]
	for i, arg := range keyArgs {
		keys[i] = string(arg)
	}

	// lock
	db.RWLocks([]string{dest}, keys)
	defer db.RWUnLocks([]string{dest}, keys)

	var result *HashSet.Set
	for _, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set == nil {
			continue
		}
		if result == nil {
			// init
			result = HashSet.Make(set.ToSlice()...)
		} else {
			result = result.Union(set)
		}
	}

	db.Remove(dest) // clean ttl
	if result == nil {
		// all keys are empty set
		return &reply.EmptyMultiBulkReply{}
	}

	set := HashSet.Make(result.ToSlice()...)
	db.PutEntity(dest, &DataEntity{
		Data: set,
	})

	db.AddAof(makeAofCmd("sunionstore", args))
	return reply.MakeIntReply(int64(set.Len()))
}

// execSDiff subtracts multiple sets
func execSDiff(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}

	// lock
	db.RLocks(keys...)
	defer db.RUnLocks(keys...)

	var result *HashSet.Set
	for i, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set == nil {
			if i == 0 {
				// early termination
				return &reply.EmptyMultiBulkReply{}
			}
			continue
		}
		if result == nil {
			// init
			result = HashSet.Make(set.ToSlice()...)
		} else {
			result = result.Diff(set)
			if result.Len() == 0 {
				// early termination
				return &reply.EmptyMultiBulkReply{}
			}
		}
	}

	if result == nil {
		// all keys are nil
		return &reply.EmptyMultiBulkReply{}
	}
	arr := make([][]byte, result.Len())
	i := 0
	result.ForEach(func(member string) bool {
		arr[i] = []byte(member)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(arr)
}

// execSDiffStore subtracts multiple sets and store the result in a key
func execSDiffStore(db *DB, args [][]byte) redis.Reply {
	dest := string(args[0])
	keys := make([]string, len(args)-1)
	keyArgs := args[1:]
	for i, arg := range keyArgs {
		keys[i] = string(arg)
	}

	// lock
	db.RWLocks([]string{dest}, keys)
	defer db.RWUnLocks([]string{dest}, keys)

	var result *HashSet.Set
	for i, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set == nil {
			if i == 0 {
				// early termination
				db.Remove(dest)
				return reply.MakeIntReply(0)
			}
			continue
		}
		if result == nil {
			// init
			result = HashSet.Make(set.ToSlice()...)
		} else {
			result = result.Diff(set)
			if result.Len() == 0 {
				// early termination
				db.Remove(dest)
				return reply.MakeIntReply(0)
			}
		}
	}

	if result == nil {
		// all keys are nil
		db.Remove(dest)
		return &reply.EmptyMultiBulkReply{}
	}
	set := HashSet.Make(result.ToSlice()...)
	db.PutEntity(dest, &DataEntity{
		Data: set,
	})

	db.AddAof(makeAofCmd("sdiffstore", args))
	return reply.MakeIntReply(int64(set.Len()))
}

// execSRandMember gets random members from set
func execSRandMember(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 && len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'srandmember' command")
	}
	key := string(args[0])
	// lock
	db.RLock(key)
	defer db.RUnLock(key)

	// get or init entity
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return &reply.NullBulkReply{}
	}
	if len(args) == 1 {
		// get a random member
		members := set.RandomMembers(1)
		return reply.MakeBulkReply([]byte(members[0]))
	}
	count64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	count := int(count64)
	if count > 0 {
		members := set.RandomDistinctMembers(count)
		result := make([][]byte, len(members))
		for i, v := range members {
			result[i] = []byte(v)
		}
		return reply.MakeMultiBulkReply(result)
	} else if count < 0 {
		members := set.RandomMembers(-count)
		result := make([][]byte, len(members))
		for i, v := range members {
			result[i] = []byte(v)
		}
		return reply.MakeMultiBulkReply(result)
	}
	return &reply.EmptyMultiBulkReply{}
}

func init() {
	RegisterCommand("SAdd", execSAdd, nil, -3)
	RegisterCommand("SIsMember", execSIsMember, nil, 3)
	RegisterCommand("SRem", execSRem, nil, -3)
	RegisterCommand("SCard", execSCard, nil, 2)
	RegisterCommand("SMembers", execSMembers, nil, 2)
	RegisterCommand("SInter", execSInter, nil, -2)
	RegisterCommand("SInterStore", execSInterStore, nil, -3)
	RegisterCommand("SUnion", execSUnion, nil, -2)
	RegisterCommand("SUnionStore", execSUnionStore, nil, -3)
	RegisterCommand("SDiff", execSDiff, nil, -2)
	RegisterCommand("SDiffStore", execSDiffStore, nil, -3)
	RegisterCommand("SRandMember", execSRandMember, nil, -2)
}
