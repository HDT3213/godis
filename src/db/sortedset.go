package db

import (
    SortedSet "github.com/HDT3213/godis/src/datastruct/sortedset"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
    "strconv"
    "strings"
)

func (db *DB)getAsSortedSet(key string)(*SortedSet.SortedSet, reply.ErrorReply) {
    entity, exists := db.Get(key)
    if !exists {
        return nil, nil
    }
    sortedSet, ok := entity.Data.(*SortedSet.SortedSet)
    if !ok {
        return nil, &reply.WrongTypeErrReply{}
    }
    return sortedSet, nil
}

func (db *DB) getOrInitSortedSet(key string)(sortedSet *SortedSet.SortedSet, inited bool, errReply reply.ErrorReply) {
    sortedSet, errReply = db.getAsSortedSet(key)
    if errReply != nil {
        return nil, false, errReply
    }
    inited = false
    if sortedSet == nil {
        sortedSet = SortedSet.Make()
        db.Put(key, &DataEntity{
            Data: sortedSet,
        })
        inited = true
    }
    return sortedSet, inited, nil
}

func ZAdd(db *DB, args [][]byte)redis.Reply {
    if len(args) < 3 || len(args) % 2 != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'zadd' command")
    }
    key := string(args[0])
    size := (len(args) - 1) / 2
    elements := make([]*SortedSet.Element, size)
    for i := 0; i < size; i++ {
        scoreValue := args[2 * i + 1]
        member := string(args[2 * i + 2])
        score, err := strconv.ParseFloat(string(scoreValue), 64)
        if err != nil {
            return reply.MakeErrReply("ERR value is not a valid float")
        }
        elements[i] = &SortedSet.Element{
            Member:member,
            Score:score,
        }
    }

    // lock
    db.Locker.Lock(key)
    defer db.Locker.UnLock(key)

    // get or init entity
    sortedSet, _, errReply := db.getOrInitSortedSet(key)
    if errReply != nil {
        return errReply
    }

    i := 0
    for _, e := range elements {
        if sortedSet.Add(e.Member, e.Score) {
            i++
        }
    }

    return reply.MakeIntReply(int64(i))
}

func ZScore(db *DB, args [][]byte)redis.Reply {
    // parse args
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'zscore' command")
    }
    key := string(args[0])
    member := string(args[1])

    sortedSet, errReply := db.getAsSortedSet(key)
    if errReply != nil {
        return errReply
    }
    if sortedSet == nil {
        return &reply.NullBulkReply{}
    }

    element, exists := sortedSet.Get(member)
    if !exists {
        return &reply.NullBulkReply{}
    }
    value := strconv.FormatFloat(element.Score, 'f', -1, 64)
    return reply.MakeBulkReply([]byte(value))
}

func ZRank(db *DB, args [][]byte)redis.Reply {
    // parse args
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'zrank' command")
    }
    key := string(args[0])
    member := string(args[1])

    // get entity
    sortedSet, errReply := db.getAsSortedSet(key)
    if errReply != nil {
        return errReply
    }
    if sortedSet == nil {
        return &reply.NullBulkReply{}
    }

    rank := sortedSet.GetRank(member, false)
    if rank < 0 {
        return &reply.NullBulkReply{}
    }
    return reply.MakeIntReply(rank)
}

func ZRevRank(db *DB, args [][]byte)redis.Reply {
    // parse args
    if len(args) != 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'zrevrank' command")
    }
    key := string(args[0])
    member := string(args[1])

    // get entity
    sortedSet, errReply := db.getAsSortedSet(key)
    if errReply != nil {
        return errReply
    }
    if sortedSet == nil {
        return &reply.NullBulkReply{}
    }

    rank := sortedSet.GetRank(member, true)
    if rank < 0 {
        return &reply.NullBulkReply{}
    }
    return reply.MakeIntReply(rank)
}

func ZCard(db *DB, args [][]byte)redis.Reply {
    // parse args
    if len(args) != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'zcard' command")
    }
    key := string(args[0])

    // get entity
    sortedSet, errReply := db.getAsSortedSet(key)
    if errReply != nil {
        return errReply
    }
    if sortedSet == nil {
        return reply.MakeIntReply(0)
    }

    return reply.MakeIntReply(int64(sortedSet.Len()))
}

func ZRange(db *DB, args [][]byte)redis.Reply {
    // parse args
    if len(args) != 3 && len(args) != 4 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'zrange' command")
    }
    withScores := false
    if len(args) == 4 {
        if strings.ToUpper(string(args[3])) != "WITHSCORES" {
            return reply.MakeErrReply("syntax error")
        } else {
            withScores = true
        }
    }
    key := string(args[0])
    start, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range")
    }
    stop, err := strconv.ParseInt(string(args[2]), 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range")
    }
    return range0(db, key, start, stop, withScores, false)
}

func ZRevRange(db *DB, args [][]byte)redis.Reply {
    // parse args
    if len(args) != 3 && len(args) != 4 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'zrevrange' command")
    }
    withScores := false
    if len(args) == 4 {
        if string(args[3]) != "WITHSCORES" {
            return reply.MakeErrReply("syntax error")
        } else {
            withScores = true
        }
    }
    key := string(args[0])
    start, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range")
    }
    stop, err := strconv.ParseInt(string(args[2]), 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range")
    }
    return range0(db, key, start, stop, withScores, true)
}

func range0(db *DB, key string, start int64, stop int64, withScores bool, desc bool)redis.Reply {
    // lock key
    db.Locker.RLock(key)
    defer db.Locker.RUnLock(key)

    // get data
    sortedSet, errReply := db.getAsSortedSet(key)
    if errReply != nil {
        return errReply
    }
    if sortedSet == nil {
        return &reply.EmptyMultiBulkReply{}
    }

    // compute index
    size := sortedSet.Len() // assert: size > 0
    if start < -1 * size {
        start = 0
    } else if start < 0 {
        start = size + start
    } else if start >= size {
        return &reply.EmptyMultiBulkReply{}
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
    slice := sortedSet.Range(start, stop, desc)
    if withScores {
        result := make([][]byte, len(slice) * 2)
        i := 0
        for _, element := range slice {
            result[i] = []byte(element.Member)
            i++
            scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
            result[i] = []byte(scoreStr)
            i++
        }
        return reply.MakeMultiBulkReply(result)
    } else {
        result := make([][]byte, len(slice))
        i := 0
        for _, element := range slice {
            result[i] = []byte(element.Member)
            i++
        }
        return reply.MakeMultiBulkReply(result)
    }
}

func ZCount(db *DB, args [][]byte)redis.Reply {
    if len(args) != 3  {
        return reply.MakeErrReply("ERR wrong number of arguments for 'zcount' command")
    }
    key := string(args[0])

    min, err := SortedSet.ParseScoreBorder(string(args[1]))
    if err != nil {
        return reply.MakeErrReply(err.Error())
    }

    max, err := SortedSet.ParseScoreBorder(string(args[2]))
    if err != nil {
        return reply.MakeErrReply(err.Error())
    }

    db.Locker.RLock(key)
    defer db.Locker.RUnLock(key)

    // get data
    sortedSet, errReply := db.getAsSortedSet(key)
    if errReply != nil {
        return errReply
    }
    if sortedSet == nil {
        return reply.MakeIntReply(0)
    }

    return reply.MakeIntReply(sortedSet.Count(min, max))
}

/*
 * param limit: limit < 0 means no limit
 */
func rangeByScore0(db *DB, key string, min *SortedSet.ScoreBorder, max *SortedSet.ScoreBorder, offset int64, limit int64, withScores bool, desc bool)redis.Reply {
    // lock key
    db.Locker.RLock(key)
    defer db.Locker.RUnLock(key)

    // get data
    sortedSet, errReply := db.getAsSortedSet(key)
    if errReply != nil {
        return errReply
    }
    if sortedSet == nil {
        return &reply.EmptyMultiBulkReply{}
    }

    slice := sortedSet.RangeByScore(min, max, offset, limit, desc)
    if withScores {
        result := make([][]byte, len(slice) * 2)
        i := 0
        for _, element := range slice {
            result[i] = []byte(element.Member)
            i++
            scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
            result[i] = []byte(scoreStr)
            i++
        }
        return reply.MakeMultiBulkReply(result)
    } else {
        result := make([][]byte, len(slice))
        i := 0
        for _, element := range slice {
            result[i] = []byte(element.Member)
            i++
        }
        return reply.MakeMultiBulkReply(result)
    }
}

func ZRangeByScore(db *DB, args [][]byte)redis.Reply {
    if len(args) < 3  {
        return reply.MakeErrReply("ERR wrong number of arguments for 'zrangebyscore' command")
    }
    key := string(args[0])

    min, err := SortedSet.ParseScoreBorder(string(args[1]))
    if err != nil {
        return reply.MakeErrReply(err.Error())
    }

    max, err := SortedSet.ParseScoreBorder(string(args[2]))
    if err != nil {
        return reply.MakeErrReply(err.Error())
    }

    withScores := false
    var offset int64 = 0
    var limit int64 = -1
    if len(args) > 3 {
        for i := 3; i < len(args); {
            s := string(args[i])
            if strings.ToUpper(s) == "WITHSCORES" {
                withScores = true
                i++
            } else if strings.ToUpper(s) == "LIMIT" {
                if len(args) < i+3 {
                    return reply.MakeErrReply("ERR syntax error")
                }
                offset, err = strconv.ParseInt(string(args[i+1]), 10, 64)
                if err != nil {
                    return reply.MakeErrReply("ERR value is not an integer or out of range")
                }
                limit, err = strconv.ParseInt(string(args[i+2]), 10, 64)
                if err != nil {
                    return reply.MakeErrReply("ERR value is not an integer or out of range")
                }
                i += 3
            } else {
                return reply.MakeErrReply("ERR syntax error")
            }
        }
    }
    return rangeByScore0(db, key, min, max, offset, limit, withScores, false)
}

func ZRevRangeByScore(db *DB, args [][]byte)redis.Reply {
    if len(args) < 3  {
        return reply.MakeErrReply("ERR wrong number of arguments for 'zrangebyscore' command")
    }
    key := string(args[0])

    min, err := SortedSet.ParseScoreBorder(string(args[1]))
    if err != nil {
        return reply.MakeErrReply(err.Error())
    }

    max, err := SortedSet.ParseScoreBorder(string(args[2]))
    if err != nil {
        return reply.MakeErrReply(err.Error())
    }

    withScores := false
    var offset int64 = 0
    var limit int64 = -1
    if len(args) > 3 {
        for i := 3; i < len(args); {
            s := string(args[i])
            if strings.ToUpper(s) == "WITHSCORES" {
                withScores = true
                i++
            } else if strings.ToUpper(s) == "LIMIT" {
                if len(args) < i+3 {
                    return reply.MakeErrReply("ERR syntax error")
                }
                offset, err = strconv.ParseInt(string(args[i+1]), 10, 64)
                if err != nil {
                    return reply.MakeErrReply("ERR value is not an integer or out of range")
                }
                limit, err = strconv.ParseInt(string(args[i+2]), 10, 64)
                if err != nil {
                    return reply.MakeErrReply("ERR value is not an integer or out of range")
                }
                i += 3
            } else {
                return reply.MakeErrReply("ERR syntax error")
            }
        }
    }
    return rangeByScore0(db, key, min, max, offset, limit, withScores, true)
}

func ZRemRangeByScore(db *DB, args [][]byte)redis.Reply  {
    if len(args) != 3  {
        return reply.MakeErrReply("ERR wrong number of arguments for 'zremrangebyscore' command")
    }
    key := string(args[0])

    min, err := SortedSet.ParseScoreBorder(string(args[1]))
    if err != nil {
        return reply.MakeErrReply(err.Error())
    }

    max, err := SortedSet.ParseScoreBorder(string(args[2]))
    if err != nil {
        return reply.MakeErrReply(err.Error())
    }

    db.Locker.Lock(key)
    defer db.Locker.UnLock(key)

    // get data
    sortedSet, errReply := db.getAsSortedSet(key)
    if errReply != nil {
        return errReply
    }
    if sortedSet == nil {
        return &reply.EmptyMultiBulkReply{}
    }

    removed := sortedSet.RemoveByScore(min, max)
    return reply.MakeIntReply(removed)
}

func ZRemRangeByRank(db *DB, args [][]byte)redis.Reply  {
    if len(args) != 3  {
        return reply.MakeErrReply("ERR wrong number of arguments for 'zremrangebyrank' command")
    }
    key := string(args[0])
    start, err := strconv.ParseInt(string(args[1]), 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range")
    }
    stop, err := strconv.ParseInt(string(args[2]), 10, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not an integer or out of range")
    }

    db.Locker.Lock(key)
    defer db.Locker.UnLock(key)

    // get data
    sortedSet, errReply := db.getAsSortedSet(key)
    if errReply != nil {
        return errReply
    }
    if sortedSet == nil {
        return reply.MakeIntReply(0)
    }

    // compute index
    size := sortedSet.Len() // assert: size > 0
    if start < -1 * size {
        start = 0
    } else if start < 0 {
        start = size + start
    } else if start >= size {
        return reply.MakeIntReply(0)
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
    removed := sortedSet.RemoveByRank(start, stop)
    return reply.MakeIntReply(removed)
}

func ZRem(db *DB, args [][]byte)redis.Reply {
    // parse args
    if len(args) < 2 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'zrem' command")
    }
    key := string(args[0])
    fields := make([]string, len(args)-1)
    fieldArgs := args[1:]
    for i, v := range fieldArgs {
        fields[i] = string(v)
    }

    db.Locker.Lock(key)
    defer db.Locker.UnLock(key)

    // get entity
    sortedSet, errReply := db.getAsSortedSet(key)
    if errReply != nil {
        return errReply
    }
    if sortedSet == nil {
        return reply.MakeIntReply(0)
    }

    var deleted int64 = 0
    for _, field := range fields {
        if sortedSet.Remove(field) {
            deleted++
        }
    }
    return reply.MakeIntReply(deleted)
}

func ZIncrBy(db *DB, args [][]byte)redis.Reply {
    if len(args) != 3 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'zincrby' command")
    }
    key := string(args[0])
    rawDelta := string(args[1])
    field := string(args[2])
    delta, err := strconv.ParseFloat(rawDelta, 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not a valid float")
    }

    db.Locker.Lock(key)
    defer db.Locker.UnLock(key)

    // get or init entity
    sortedSet, _, errReply := db.getOrInitSortedSet(key)
    if errReply != nil {
        return errReply
    }

    element, exists := sortedSet.Get(field)
    if !exists {
        sortedSet.Add(field, delta)
        return reply.MakeBulkReply(args[1])
    } else {
        score := element.Score +  delta
        sortedSet.Add(field, score)
        bytes := []byte(strconv.FormatFloat(score, 'f', -1, 64))
        return reply.MakeBulkReply(bytes)
    }
}