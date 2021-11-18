package database

import (
	"fmt"
	"github.com/hdt3213/godis/datastruct/sortedset"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/geohash"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/reply"
	"strconv"
	"strings"
)

// execGeoAdd add a location into SortedSet
func execGeoAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 4 || len(args)%3 != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'geoadd' command")
	}
	key := string(args[0])
	size := (len(args) - 1) / 3
	elements := make([]*sortedset.Element, size)
	for i := 0; i < size; i++ {
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
	db.addAof(utils.ToCmdLine3("geoadd", args...))
	return reply.MakeIntReply(int64(i))
}

func undoGeoAdd(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	size := (len(args) - 1) / 3
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[3*i+3])
	}
	return rollbackZSetFields(db, key, fields...)
}

// execGeoPos returns location of a member
func execGeoPos(db *DB, args [][]byte) redis.Reply {
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

	positions := make([]redis.Reply, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		member := string(args[i+1])
		elem, exists := sortedSet.Get(member)
		if !exists {
			positions[i] = &reply.EmptyMultiBulkReply{}
			continue
		}
		lat, lng := geohash.Decode(uint64(elem.Score))
		lngStr := strconv.FormatFloat(lng, 'f', -1, 64)
		latStr := strconv.FormatFloat(lat, 'f', -1, 64)
		positions[i] = reply.MakeMultiBulkReply([][]byte{
			[]byte(lngStr), []byte(latStr),
		})
	}
	return reply.MakeMultiRawReply(positions)
}

// execGeoDist returns the distance between two locations
func execGeoDist(db *DB, args [][]byte) redis.Reply {
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
	dis := geohash.Distance(positions[0][0], positions[0][1], positions[1][0], positions[1][1])
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

// execGeoHash return geo-hash-code of given position
func execGeoHash(db *DB, args [][]byte) redis.Reply {
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

// execGeoRadius returns members within max distance of given point
func execGeoRadius(db *DB, args [][]byte) redis.Reply {
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

// execGeoRadiusByMember returns members within max distance of given member's location
func execGeoRadiusByMember(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) < 3 {
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
	if len(args) > 3 {
		unit := strings.ToLower(string(args[3]))
		if unit == "m" {
		} else if unit == "km" {
			radius *= 1000
		} else {
			return reply.MakeErrReply("ERR unsupported unit provided. please use m, km")
		}
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

func init() {
	RegisterCommand("GeoAdd", execGeoAdd, writeFirstKey, undoGeoAdd, -5)
	RegisterCommand("GeoPos", execGeoPos, readFirstKey, nil, -2)
	RegisterCommand("GeoDist", execGeoDist, readFirstKey, nil, -4)
	RegisterCommand("GeoHash", execGeoHash, readFirstKey, nil, -2)
	RegisterCommand("GeoRadius", execGeoRadius, readFirstKey, nil, -6)
	RegisterCommand("GeoRadiusByMember", execGeoRadiusByMember, readFirstKey, nil, -5)
}
