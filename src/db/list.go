package db

import (
    List "github.com/HDT3213/godis/src/datastruct/list"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
    "strconv"
)

func (db *DB) getAsList(key string)(*List.LinkedList, reply.ErrorReply) {
    entity, ok := db.Get(key)
    if !ok {
        return nil, nil
    }
    bytes, ok := entity.Data.(*List.LinkedList)
    if !ok {
        return nil, &reply.WrongTypeErrReply{}
    }
    return bytes, nil
}

func (db *DB) getOrInitList(key string)(list *List.LinkedList, inited bool, errReply reply.ErrorReply) {
    list, errReply = db.getAsList(key)
    if errReply != nil {
        return nil, false, errReply
    }
    inited = false
    if list == nil {
        list = &List.LinkedList{}
        db.Put(key, &DataEntity{
            Data: list,
        })
        inited = true
    }
    return list, inited, nil
}

func LIndex(db *DB, args [][]byte)(redis.Reply, *extra){
    // parse args
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'lindex' command"), nil
    }
    key := string(args[0])
    index64, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
    }
    index := int(index64)

    // get entity
    list, errReply := db.getAsList(key)
    if errReply != nil {
        return errReply, nil
    }
    if list == nil {
        return &reply.NullBulkReply{}, nil
    }

    size := list.Len() // assert: size > 0
    if index < -1 * size {
        return &reply.NullBulkReply{}, nil
    } else if index < 0 {
        index = size + index
    } else if index >= size {
        return &reply.NullBulkReply{}, nil
    }

    val, _ := list.Get(index).([]byte)
    return reply.MakeBulkReply(val), nil
}

func LLen(db *DB, args [][]byte)(redis.Reply, *extra) {
    // parse args
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'llen' command"), nil
    }
    key := string(args[0])

    list, errReply := db.getAsList(key)
    if errReply != nil {
        return errReply, nil
    }
    if list == nil {
        return reply.MakeIntReply(0), nil
    }

    size := int64(list.Len())
    return reply.MakeIntReply(size), nil
}

func LPop(db *DB, args [][]byte)(redis.Reply, *extra) {
    // parse args
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'lindex' command"), nil
    }
    key := string(args[0])

    // lock
    db.Locker.Lock(key)
    defer db.Locker.UnLock(key)

    // get data
    list, errReply := db.getAsList(key)
    if errReply != nil {
        return errReply, nil
    }
    if list == nil {
        return &reply.NullBulkReply{}, nil
    }

    val, _ := list.Remove(0).([]byte)
    if list.Len() == 0 {
        db.Remove(key)
    }
    return reply.MakeBulkReply(val), &extra{toPersist: true}
}

func LPush(db *DB, args [][]byte)(redis.Reply, *extra) {
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'lpush' command"), nil
    }
    key := string(args[0])
    values := args[1:]

    // lock
    db.Locker.Lock(key)
    defer db.Locker.UnLock(key)

    // get or init entity
    list, _, errReply := db.getOrInitList(key)
    if errReply != nil {
        return errReply, nil
    }

    // insert
    for _, value := range values {
        list.Insert(0, value)
    }

    return reply.MakeIntReply(int64(list.Len())), &extra{toPersist: true}
}

func LPushX(db *DB, args [][]byte)(redis.Reply, *extra) {
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'lpush' command"), nil
    }
    key := string(args[0])
    values := args[1:]

    // lock
    db.Locker.Lock(key)
    defer db.Locker.UnLock(key)

    // get or init entity
    list, errReply := db.getAsList(key)
    if errReply != nil {
        return errReply, nil
    }
    if list == nil {
        return reply.MakeIntReply(0), nil
    }

    // insert
    for _, value := range values {
        list.Insert(0, value)
    }

    return reply.MakeIntReply(int64(list.Len())), &extra{toPersist: true}
}

func LRange(db *DB, args [][]byte)(redis.Reply, *extra){
    // parse args
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'lrange' command"), nil
    }
    key := string(args[0])
    start64, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
    }
    start := int(start64)
    stop64, err := strconv.ParseInt(string(args[2]), 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
    }
    stop := int(stop64)

    // lock key
    db.RLock(key)
    defer db.RUnLock(key)

    // get data
    list, errReply := db.getAsList(key)
    if errReply != nil {
        return errReply, nil
    }
    if list == nil {
        return &reply.EmptyMultiBulkReply{}, nil
    }

    // compute index
    size := list.Len() // assert: size > 0
    if start < -1 * size {
        start = 0
    } else if start < 0 {
        start = size + start
    } else if start >= size {
        return &reply.EmptyMultiBulkReply{}, nil
    }
    if stop < -1 * size {
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
    return reply.MakeMultiBulkReply(result), nil
}

func LRem(db *DB, args [][]byte)(redis.Reply, *extra) {
    // parse args
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'lrem' command"), nil
    }
    key := string(args[0])
    count64, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
    }
    count := int(count64)
    value := args[2]

    // lock
    db.Lock(key)
    defer db.UnLock(key)

    // get data entity
    list, errReply := db.getAsList(key)
    if errReply != nil {
        return errReply, nil
    }
    if list == nil {
        return reply.MakeIntReply(0), nil
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

    return reply.MakeIntReply(int64(removed)), &extra{toPersist: removed > 0}
}

func LSet(db *DB, args [][]byte)(redis.Reply, *extra) {
    // parse args
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'lset' command"), nil
    }
    key := string(args[0])
    index64, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
    }
    index := int(index64)
    value := args[2]

    // lock
    db.Locker.Lock(key)
    defer db.Locker.UnLock(key)

    // get data
    list, errReply := db.getAsList(key)
    if errReply != nil {
        return errReply, nil
    }
    if list == nil {
        return reply.MakeErrReply("ERR no such key"), nil
    }

    size := list.Len() // assert: size > 0
    if index < -1 * size {
        return reply.MakeErrReply("ERR index out of range"), nil
    } else if index < 0 {
        index = size + index
    } else if index >= size {
        return reply.MakeErrReply("ERR index out of range"), nil
    }

    list.Set(index, value)
    return &reply.OkReply{}, &extra{toPersist: true}
}

func RPop(db *DB, args [][]byte)(redis.Reply, *extra) {
    // parse args
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'lindex' command"), nil
    }
    key := string(args[0])

    // lock
    db.Locker.Lock(key)
    defer db.Locker.UnLock(key)

    // get data
    list, errReply := db.getAsList(key)
    if errReply != nil {
        return errReply, nil
    }
    if list == nil {
        return &reply.NullBulkReply{}, nil
    }

    val, _ := list.RemoveLast().([]byte)
    if list.Len() == 0 {
        db.Remove(key)
    }
    return reply.MakeBulkReply(val), &extra{toPersist: true}
}

func RPopLPush(db *DB, args [][]byte)(redis.Reply, *extra) {
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'rpoplpush' command"), nil
    }
    sourceKey := string(args[0])
    destKey := string(args[1])

    // lock
    db.Locker.Locks(sourceKey, destKey)
    defer db.Locker.UnLocks(sourceKey, destKey)

    // get source entity
    sourceList, errReply := db.getAsList(sourceKey)
    if errReply != nil {
        return errReply, nil
    }
    if sourceList == nil {
        return &reply.NullBulkReply{}, nil
    }

    // get dest entity
    destList, _, errReply := db.getOrInitList(destKey)
    if errReply != nil {
        return errReply, nil
    }

    // pop and push
    val, _ := sourceList.RemoveLast().([]byte)
    destList.Insert(0, val)

    if sourceList.Len() == 0 {
        db.Remove(sourceKey)
    }

    return reply.MakeBulkReply(val), &extra{toPersist: true}
}

func RPush(db *DB, args [][]byte)(redis.Reply, *extra) {
    // parse args
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'rpush' command"), nil
    }
    key := string(args[0])
    values := args[1:]

    // lock
    db.Locker.Lock(key)
    defer db.Locker.UnLock(key)

    // get or init entity
    list, _, errReply := db.getOrInitList(key)
    if errReply != nil {
        return errReply, nil
    }

    // put list
    for _, value := range values {
        list.Add(value)
    }

    return reply.MakeIntReply(int64(list.Len())), &extra{toPersist: true}
}

func RPushX(db *DB, args [][]byte)(redis.Reply, *extra) {
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'rpush' command"), nil
    }
    key := string(args[0])
    values := args[1:]

    // lock
    db.Locker.Lock(key)
    defer db.Locker.UnLock(key)

    // get or init entity
    list, errReply := db.getAsList(key)
    if errReply != nil {
        return errReply, nil
    }
    if list == nil {
        return reply.MakeIntReply(0), nil
    }

    // put list
    for _, value := range values {
        list.Add(value)
    }

    return reply.MakeIntReply(int64(list.Len())), &extra{toPersist: true}
}