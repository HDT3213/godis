package db

import (
    HashSet "github.com/HDT3213/godis/src/datastruct/set"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
    "strconv"
)


func (db *DB)getAsSet(key string)(*HashSet.Set, reply.ErrorReply) {
    entity, exists := db.Get(key)
    if !exists {
        return nil, nil
    }
    set, ok := entity.Data.(*HashSet.Set)
    if !ok {
        return nil, &reply.WrongTypeErrReply{}
    }
    return set, nil
}

func (db *DB) getOrInitSet(key string)(set *HashSet.Set, inited bool, errReply reply.ErrorReply) {
    set, errReply = db.getAsSet(key)
    if errReply != nil {
        return nil, false, errReply
    }
    inited = false
    if set == nil {
        set = HashSet.Make()
        db.Put(key, &DataEntity{
            Data: set,
        })
        inited = true
    }
    return set, inited, nil
}

func SAdd(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'sadd' command"), nil
    }
    key := string(args[0])
    members := args[1:]

    // lock
    db.Lock(key)
    defer db.UnLock(key)

    // get or init entity
    set, _, errReply := db.getOrInitSet(key)
    if errReply != nil {
        return errReply, nil
    }
    counter := 0
    for _, member := range members {
        counter += set.Add(string(member))
    }
    return reply.MakeIntReply(int64(counter)), &extra{toPersist: true}
}

func SIsMember(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'sismember' command"), nil
    }
    key := string(args[0])
    member := string(args[1])

    // get set
    set, errReply := db.getAsSet(key)
    if errReply != nil {
        return errReply, nil
    }
    if set == nil {
        return reply.MakeIntReply(0), nil
    }

    has := set.Has(member)
    if has {
        return reply.MakeIntReply(1), nil
    } else {
        return reply.MakeIntReply(0), nil
    }
}

func SRem(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'srem' command"), nil
    }
    key := string(args[0])
    members := args[1:]

    // lock
    db.Lock(key)
    defer db.UnLock(key)

    set, errReply := db.getAsSet(key)
    if errReply != nil {
        return errReply, nil
    }
    if set == nil {
        return reply.MakeIntReply(0), nil
    }
    counter := 0
    for _, member := range members {
        counter += set.Remove(string(member))
    }
    if set.Len() == 0 {
        db.Remove(key)
    }
    return reply.MakeIntReply(int64(counter)), &extra{toPersist: counter > 0}
}

func SCard(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'scard' command"), nil
    }
    key := string(args[0])

    // get or init entity
    set, errReply := db.getAsSet(key)
    if errReply != nil {
        return errReply, nil
    }
    if set == nil {
        return reply.MakeIntReply(0), nil
    }
    return reply.MakeIntReply(int64(set.Len())), nil
}

func SMembers(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'smembers' command"), nil
    }
    key := string(args[0])

    // lock
    db.Locker.RLock(key)
    defer db.Locker.RUnLock(key)

    // get or init entity
    set, errReply := db.getAsSet(key)
    if errReply != nil {
        return errReply, nil
    }
    if set == nil {
        return &reply.EmptyMultiBulkReply{}, nil
    }


    arr := make([][]byte, set.Len())
    i := 0
    set.ForEach(func (member string)bool {
        arr[i] = []byte(member)
        i++
        return true
    })
    return reply.MakeMultiBulkReply(arr), nil
}

func SInter(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) < 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'sinter' command"), nil
    }
    keys := make([]string, len(args))
    for i, arg := range args {
        keys[i] = string(arg)
    }

    // lock
    db.Locker.RLocks(keys...)
    defer db.Locker.RUnLocks(keys...)

    var result *HashSet.Set
    for _, key := range keys {
        set, errReply := db.getAsSet(key)
        if errReply != nil {
            return errReply, nil
        }
        if set == nil {
            return &reply.EmptyMultiBulkReply{}, nil
        }

        if result == nil {
            // init
            result = HashSet.MakeFromVals(set.ToSlice()...)
        } else {
            result = result.Intersect(set)
            if result.Len() == 0 {
                // early termination
                return &reply.EmptyMultiBulkReply{}, nil
            }
        }
    }

    arr := make([][]byte, result.Len())
    i := 0
    result.ForEach(func (member string)bool {
        arr[i] = []byte(member)
        i++
        return true
    })
    return reply.MakeMultiBulkReply(arr), nil
}

func SInterStore(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'sinterstore' command"), nil
    }
    dest := string(args[0])
    keys := make([]string, len(args) - 1)
    keyArgs := args[1:]
    for i, arg := range keyArgs {
        keys[i] = string(arg)
    }

    // lock
    db.Locker.RLocks(keys...)
    defer db.Locker.RUnLocks(keys...)
    db.Locker.Lock(dest)
    defer db.Locker.UnLock(dest)

    var result *HashSet.Set
    for _, key := range keys {
        set, errReply := db.getAsSet(key)
        if errReply != nil {
            return errReply, nil
        }
        if set == nil {
            db.Remove(dest) // clean ttl and old value
            return &reply.EmptyMultiBulkReply{}, nil
        }

        if result == nil {
            // init
            result = HashSet.MakeFromVals(set.ToSlice()...)
        } else {
            result = result.Intersect(set)
            if result.Len() == 0 {
                // early termination
                db.Remove(dest) // clean ttl and old value
                return reply.MakeIntReply(0), nil
            }
        }
    }

    set := HashSet.MakeFromVals(result.ToSlice()...)
    db.Put(dest, &DataEntity{
        Data: set,
    })

    return reply.MakeIntReply(int64(set.Len())), &extra{toPersist: true}
}

func SUnion(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) < 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'sunion' command"), nil
    }
    keys := make([]string, len(args))
    for i, arg := range args {
        keys[i] = string(arg)
    }

    // lock
    db.Locker.RLocks(keys...)
    defer db.Locker.RUnLocks(keys...)

    var result *HashSet.Set
    for _, key := range keys {
        set, errReply := db.getAsSet(key)
        if errReply != nil {
            return errReply, nil
        }
        if set == nil {
            continue
        }

        if result == nil {
            // init
            result = HashSet.MakeFromVals(set.ToSlice()...)
        } else {
            result = result.Union(set)
        }
    }

    if result == nil {
        // all keys are empty set
        return &reply.EmptyMultiBulkReply{}, nil
    }
    arr := make([][]byte, result.Len())
    i := 0
    result.ForEach(func (member string)bool {
        arr[i] = []byte(member)
        i++
        return true
    })
    return reply.MakeMultiBulkReply(arr), nil
}

func SUnionStore(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'sunionstore' command"), nil
    }
    dest := string(args[0])
    keys := make([]string, len(args) - 1)
    keyArgs := args[1:]
    for i, arg := range keyArgs {
        keys[i] = string(arg)
    }

    // lock
    db.Locker.RLocks(keys...)
    defer db.Locker.RUnLocks(keys...)
    db.Locker.Lock(dest)
    defer db.Locker.UnLock(dest)

    var result *HashSet.Set
    for _, key := range keys {
        set, errReply := db.getAsSet(key)
        if errReply != nil {
            return errReply, nil
        }
        if set == nil {
            continue
        }
        if result == nil {
            // init
            result = HashSet.MakeFromVals(set.ToSlice()...)
        } else {
            result = result.Union(set)
        }
    }

    db.Remove(dest) // clean ttl
    if result == nil {
        // all keys are empty set
        return &reply.EmptyMultiBulkReply{}, nil
    }

    set := HashSet.MakeFromVals(result.ToSlice()...)
    db.Put(dest, &DataEntity{
        Data: set,
    })

    return reply.MakeIntReply(int64(set.Len())), &extra{toPersist: true}
}

func SDiff(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) < 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'sdiff' command"), nil
    }
    keys := make([]string, len(args))
    for i, arg := range args {
        keys[i] = string(arg)
    }

    // lock
    db.Locker.RLocks(keys...)
    defer db.Locker.RUnLocks(keys...)

    var result *HashSet.Set
    for i, key := range keys {
        set, errReply := db.getAsSet(key)
        if errReply != nil {
            return errReply, nil
        }
        if set == nil {
            if i == 0 {
                // early termination
                return &reply.EmptyMultiBulkReply{}, nil
            } else {
                continue
            }
        }
        if result == nil {
            // init
            result = HashSet.MakeFromVals(set.ToSlice()...)
        } else {
            result = result.Diff(set)
            if result.Len() == 0 {
                // early termination
                return &reply.EmptyMultiBulkReply{}, nil
            }
        }
    }

    if result == nil {
        // all keys are nil
        return &reply.EmptyMultiBulkReply{}, nil
    }
    arr := make([][]byte, result.Len())
    i := 0
    result.ForEach(func (member string)bool {
        arr[i] = []byte(member)
        i++
        return true
    })
    return reply.MakeMultiBulkReply(arr), nil
}

func SDiffStore(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'sdiffstore' command"), nil
    }
    dest := string(args[0])
    keys := make([]string, len(args) - 1)
    keyArgs := args[1:]
    for i, arg := range keyArgs {
        keys[i] = string(arg)
    }

    // lock
    db.Locker.RLocks(keys...)
    defer db.Locker.RUnLocks(keys...)
    db.Locker.Lock(dest)
    defer db.Locker.UnLock(dest)

    var result *HashSet.Set
    for i, key := range keys {
        set, errReply := db.getAsSet(key)
        if errReply != nil {
            return errReply, nil
        }
        if set == nil {
            if i == 0 {
                // early termination
                db.Remove(dest)
                return &reply.EmptyMultiBulkReply{}, nil
            } else {
                continue
            }
        }
        if result == nil {
            // init
            result = HashSet.MakeFromVals(set.ToSlice()...)
        } else {
            result = result.Diff(set)
            if result.Len() == 0 {
                // early termination
                db.Remove(dest)
                return &reply.EmptyMultiBulkReply{}, nil
            }
        }
    }

    if result == nil {
        // all keys are nil
        db.Remove(dest)
        return &reply.EmptyMultiBulkReply{}, nil
    }
    set := HashSet.MakeFromVals(result.ToSlice()...)
    db.Put(dest, &DataEntity{
        Data: set,
    })

    return reply.MakeIntReply(int64(set.Len())), &extra{toPersist: true}
}

func SRandMember(db *DB, args [][]byte) (redis.Reply, *extra) {
    if len(args) != 1 && len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'srandmember' command"), nil
    }
    key := string(args[0])
    // lock
    db.Locker.RLock(key)
    defer db.Locker.RUnLock(key)

    // get or init entity
    set, errReply := db.getAsSet(key)
    if errReply != nil {
        return errReply, nil
    }
    if set == nil {
        return &reply.NullBulkReply{}, nil
    }
    if len(args) == 1 {
        members := set.RandomMembers(1)
        return reply.MakeBulkReply([]byte(members[0])), nil
    } else {
        count64, err := strconv.ParseInt(string(args[1]), 10, 64)
        if err != nil {
            return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
        }
        count := int(count64)

        if count > 0 {
            members := set.RandomMembers(count)

            result := make([][]byte, len(members))
            for i, v := range members {
                result[i] = []byte(v)
            }
            return reply.MakeMultiBulkReply(result), nil
        } else if count < 0 {
            members := set.RandomDistinctMembers(-count)
            result := make([][]byte, len(members))
            for i, v := range members {
                result[i] = []byte(v)
            }
            return reply.MakeMultiBulkReply(result), nil
        } else {
            return &reply.EmptyMultiBulkReply{}, nil
        }
    }
}