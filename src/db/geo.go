package db

import (
    "fmt"
    "github.com/HDT3213/godis/src/datastruct/sortedset"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/lib/geohash"
    "github.com/HDT3213/godis/src/redis/reply"
    "strconv"
    "strings"
)

func GeoAdd(db *DB, args [][]byte) redis.Reply {
    if len(args) < 4 || len(args)%3 != 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'geoadd' command")
    }
    key := string(args[0])
    size := (len(args) - 1) / 3
    elements := make([]*sortedset.Element, size)
    for i := 0; i < size; i += 1 {
        lngStr := string(args[3*i+1])
        latStr := string(args[3*i+2])
        lng, err := strconv.ParseFloat(lngStr, 64)
        if err != nil {
            return reply.MakeErrReply("ERR value is not a valid float")
        }
        lat, err := strconv.ParseFloat(latStr, 64)
        if err != nil {
            return reply.MakeErrReply("ERR value is not a valid float")
        }
        if lat < -90 || lat > 90 || lng < -180 || lng > 180 {
            return reply.MakeErrReply(fmt.Sprintf("ERR invalid longitude,latitude pair %s,%s", latStr, lngStr))
        }
        code := float64(geohash.Encode(lat, lng))
        elements[i] = &sortedset.Element{
            Member: string(args[3*i+3]),
            Score:  code,
        }
    }

    // lock
    db.Lock(key)
    defer db.UnLock(key)

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

    db.AddAof(makeAofCmd("geoadd", args))

    return reply.MakeIntReply(int64(i))
}

func GeoPos(db *DB, args [][]byte) redis.Reply {
    // parse args
    if len(args) < 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'geopos' command")
    }
    key := string(args[0])
    sortedSet, errReply := db.getAsSortedSet(key)
    if errReply != nil {
        return errReply
    }
    if sortedSet == nil {
        return &reply.NullBulkReply{}
    }

    positions := make([][]byte, len(args)-1)
    for i := 0; i < len(args)-1; i++ {
        member := string(args[i+1])
        elem, exists := sortedSet.Get(member)
        if !exists {
            positions[i] = (&reply.EmptyMultiBulkReply{}).ToBytes()
            continue
        }
        lat, lng := geohash.Decode(uint64(elem.Score))
        lngStr := strconv.FormatFloat(lng, 'f', -1, 64)
        latStr := strconv.FormatFloat(lat, 'f', -1, 64)
        positions[i] = reply.MakeMultiBulkReply([][]byte{
            []byte(lngStr), []byte(latStr),
        }).ToBytes()
    }
    return reply.MakeMultiRawReply(positions)
}

func GeoDist(db *DB, args [][]byte) redis.Reply {
    // parse args
    if len(args) != 3 && len(args) != 4 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'geodist' command")
    }
    key := string(args[0])
    sortedSet, errReply := db.getAsSortedSet(key)
    if errReply != nil {
        return errReply
    }
    if sortedSet == nil {
        return &reply.NullBulkReply{}
    }

    positions := make([][]float64, 2)
    for i := 1; i < 3; i++ {
        member := string(args[i])
        elem, exists := sortedSet.Get(member)
        if !exists {
            return &reply.NullBulkReply{}
        }
        lat, lng := geohash.Decode(uint64(elem.Score))
        positions[i-1] = []float64{lat, lng}
    }
    unit := "m"
    if len(args) == 4 {
        unit = strings.ToLower(string(args[3]))
    }
    dis := geohash.Distance(positions[0][1], positions[0][0], positions[1][1], positions[1][0])
    switch unit {
    case "m":
        disStr := strconv.FormatFloat(dis, 'f', -1, 64)
        return reply.MakeBulkReply([]byte(disStr))
    case "km":
        disStr := strconv.FormatFloat(dis/1000, 'f', -1, 64)
        return reply.MakeBulkReply([]byte(disStr))
    }
    return reply.MakeErrReply("ERR unsupported unit provided. please use m, km")
}

func GeoHash(db *DB, args [][]byte) redis.Reply {
    // parse args
    if len(args) < 1 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'geohash' command")
    }

    key := string(args[0])
    sortedSet, errReply := db.getAsSortedSet(key)
    if errReply != nil {
        return errReply
    }
    if sortedSet == nil {
        return &reply.NullBulkReply{}
    }

    strs := make([][]byte, len(args)-1)
    for i := 0; i < len(args)-1; i++ {
        member := string(args[i+1])
        elem, exists := sortedSet.Get(member)
        if !exists {
            strs[i] = (&reply.EmptyMultiBulkReply{}).ToBytes()
            continue
        }
        str := geohash.ToString(geohash.FromInt(uint64(elem.Score)))
        strs[i] = []byte(str)
    }
    return reply.MakeMultiBulkReply(strs)
}

func GeoRadius(db *DB, args [][]byte) redis.Reply {
    // parse args
    if len(args) < 5 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'georadius' command")
    }

    key := string(args[0])
    sortedSet, errReply := db.getAsSortedSet(key)
    if errReply != nil {
        return errReply
    }
    if sortedSet == nil {
        return &reply.NullBulkReply{}
    }

    lng, err := strconv.ParseFloat(string(args[1]), 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not a valid float")
    }
    lat, err := strconv.ParseFloat(string(args[2]), 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not a valid float")
    }
    radius, err := strconv.ParseFloat(string(args[3]), 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not a valid float")
    }
    unit := strings.ToLower(string(args[4]))
    if unit == "m" {
    } else if unit == "km" {
        radius *= 1000
    } else {
        return reply.MakeErrReply("ERR unsupported unit provided. please use m, km")
    }
    return geoRadius0(sortedSet, lat, lng, radius)
}

func GeoRadiusByMember(db *DB, args [][]byte) redis.Reply {
    // parse args
    if len(args) < 4 {
        return reply.MakeErrReply("ERR wrong number of arguments for 'georadiusbymember' command")
    }

    key := string(args[0])
    sortedSet, errReply := db.getAsSortedSet(key)
    if errReply != nil {
        return errReply
    }
    if sortedSet == nil {
        return &reply.NullBulkReply{}
    }

    member := string(args[1])
    elem, ok := sortedSet.Get(member)
    if !ok {
        return &reply.NullBulkReply{}
    }
    lat, lng := geohash.Decode(uint64(elem.Score))

    radius, err := strconv.ParseFloat(string(args[2]), 64)
    if err != nil {
        return reply.MakeErrReply("ERR value is not a valid float")
    }
    unit := strings.ToLower(string(args[4]))
    if unit == "m" {
    } else if unit == "km" {
        radius *= 1000
    } else {
        return reply.MakeErrReply("ERR unsupported unit provided. please use m, km")
    }
    return geoRadius0(sortedSet, lat, lng, radius)
}

func geoRadius0(sortedSet *sortedset.SortedSet, lat float64, lng float64, radius float64) redis.Reply {
    areas := geohash.GetNeighbours(lat, lng, radius)
    members := make([][]byte, 0)
    for _, area := range areas {
        lower := &sortedset.ScoreBorder{Value: float64(area[0])}
        upper := &sortedset.ScoreBorder{Value: float64(area[1])}
        elements := sortedSet.RangeByScore(lower, upper, 0, -1, true)
        for _, elem := range elements {
            members = append(members, []byte(elem.Member))
        }
    }
    return reply.MakeMultiBulkReply(members)
}