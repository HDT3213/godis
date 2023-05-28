package database

import (
	"math"
	"strconv"
	"strings"

	SortedSet "github.com/hdt3213/godis/datastruct/sortedset"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

func (db *DB) getAsSortedSet(key string) (*SortedSet.SortedSet, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	sortedSet, ok := entity.Data.(*SortedSet.SortedSet)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return sortedSet, nil
}

func (db *DB) getOrInitSortedSet(key string) (sortedSet *SortedSet.SortedSet, inited bool, errReply protocol.ErrorReply) {
	sortedSet, errReply = db.getAsSortedSet(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if sortedSet == nil {
		sortedSet = SortedSet.Make()
		db.PutEntity(key, &database.DataEntity{
			Data: sortedSet,
		})
		inited = true
	}
	return sortedSet, inited, nil
}

// execZAdd adds member into sorted set
func execZAdd(db *DB, args [][]byte) redis.Reply {
	if len(args)%2 != 1 {
		return protocol.MakeSyntaxErrReply()
	}
	key := string(args[0])
	size := (len(args) - 1) / 2
	elements := make([]*SortedSet.Element, size)
	for i := 0; i < size; i++ {
		scoreValue := args[2*i+1]
		member := string(args[2*i+2])
		score, err := strconv.ParseFloat(string(scoreValue), 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not a valid float")
		}
		elements[i] = &SortedSet.Element{
			Member: member,
			Score:  score,
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

	db.addAof(utils.ToCmdLine3("zadd", args...))

	return protocol.MakeIntReply(int64(i))
}

func undoZAdd(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	size := (len(args) - 1) / 2
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[2*i+2])
	}
	return rollbackZSetFields(db, key, fields...)
}

// execZScore gets score of a member in sortedset
func execZScore(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	member := string(args[1])

	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return &protocol.NullBulkReply{}
	}

	element, exists := sortedSet.Get(member)
	if !exists {
		return &protocol.NullBulkReply{}
	}
	value := strconv.FormatFloat(element.Score, 'f', -1, 64)
	return protocol.MakeBulkReply([]byte(value))
}

// execZRank gets index of a member in sortedset, ascending order, start from 0
func execZRank(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	member := string(args[1])

	// get entity
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return &protocol.NullBulkReply{}
	}

	rank := sortedSet.GetRank(member, false)
	if rank < 0 {
		return &protocol.NullBulkReply{}
	}
	return protocol.MakeIntReply(rank)
}

// execZRevRank gets index of a member in sortedset, descending order, start from 0
func execZRevRank(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	member := string(args[1])

	// get entity
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return &protocol.NullBulkReply{}
	}

	rank := sortedSet.GetRank(member, true)
	if rank < 0 {
		return &protocol.NullBulkReply{}
	}
	return protocol.MakeIntReply(rank)
}

// execZCard gets number of members in sortedset
func execZCard(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])

	// get entity
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}

	return protocol.MakeIntReply(sortedSet.Len())
}

// execZRange gets members in range, sort by score in ascending order
func execZRange(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 3 && len(args) != 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrange' command")
	}
	withScores := false
	if len(args) == 4 {
		if strings.ToUpper(string(args[3])) != "WITHSCORES" {
			return protocol.MakeErrReply("syntax error")
		}
		withScores = true
	}
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	return range0(db, key, start, stop, withScores, false)
}

// execZRevRange gets members in range, sort by score in descending order
func execZRevRange(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 3 && len(args) != 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrevrange' command")
	}
	withScores := false
	if len(args) == 4 {
		if string(args[3]) != "WITHSCORES" {
			return protocol.MakeErrReply("syntax error")
		}
		withScores = true
	}
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	return range0(db, key, start, stop, withScores, true)
}

func range0(db *DB, key string, start int64, stop int64, withScores bool, desc bool) redis.Reply {
	// get data
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	// compute index
	size := sortedSet.Len() // assert: size > 0
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
	slice := sortedSet.RangeByRank(start, stop, desc)
	if withScores {
		result := make([][]byte, len(slice)*2)
		i := 0
		for _, element := range slice {
			result[i] = []byte(element.Member)
			i++
			scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
			result[i] = []byte(scoreStr)
			i++
		}
		return protocol.MakeMultiBulkReply(result)
	}
	result := make([][]byte, len(slice))
	i := 0
	for _, element := range slice {
		result[i] = []byte(element.Member)
		i++
	}
	return protocol.MakeMultiBulkReply(result)
}

// execZCount gets number of members which score within given range
func execZCount(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	min, err := SortedSet.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	max, err := SortedSet.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	// get data
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}

	return protocol.MakeIntReply(sortedSet.RangeCount(min, max))
}

/*
 * param limit: limit < 0 means no limit
 */
func rangeByScore0(db *DB, key string, min SortedSet.Border, max SortedSet.Border, offset int64, limit int64, withScores bool, desc bool) redis.Reply {
	// get data
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	slice := sortedSet.Range(min, max, offset, limit, desc)
	if withScores {
		result := make([][]byte, len(slice)*2)
		i := 0
		for _, element := range slice {
			result[i] = []byte(element.Member)
			i++
			scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
			result[i] = []byte(scoreStr)
			i++
		}
		return protocol.MakeMultiBulkReply(result)
	}
	result := make([][]byte, len(slice))
	i := 0
	for _, element := range slice {
		result[i] = []byte(element.Member)
		i++
	}
	return protocol.MakeMultiBulkReply(result)
}

// execZRangeByScore gets members which score within given range, in ascending order
func execZRangeByScore(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrangebyscore' command")
	}
	key := string(args[0])

	min, err := SortedSet.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	max, err := SortedSet.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
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
					return protocol.MakeErrReply("ERR syntax error")
				}
				offset, err = strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				limit, err = strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				i += 3
			} else {
				return protocol.MakeErrReply("ERR syntax error")
			}
		}
	}
	return rangeByScore0(db, key, min, max, offset, limit, withScores, false)
}

// execZRevRangeByScore gets number of members which score within given range, in descending order
func execZRevRangeByScore(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrangebyscore' command")
	}
	key := string(args[0])

	min, err := SortedSet.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	max, err := SortedSet.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
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
					return protocol.MakeErrReply("ERR syntax error")
				}
				offset, err = strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				limit, err = strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				i += 3
			} else {
				return protocol.MakeErrReply("ERR syntax error")
			}
		}
	}
	return rangeByScore0(db, key, min, max, offset, limit, withScores, true)
}

// execZRemRangeByScore removes members which score within given range
func execZRemRangeByScore(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zremrangebyscore' command")
	}
	key := string(args[0])

	min, err := SortedSet.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	max, err := SortedSet.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	// get data
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	removed := sortedSet.RemoveRange(min, max)
	if removed > 0 {
		db.addAof(utils.ToCmdLine3("zremrangebyscore", args...))
	}
	return protocol.MakeIntReply(removed)
}

// execZRemRangeByRank removes members within given indexes
func execZRemRangeByRank(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	// get data
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}

	// compute index
	size := sortedSet.Len() // assert: size > 0
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return protocol.MakeIntReply(0)
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
	removed := sortedSet.RemoveByRank(start, stop)
	if removed > 0 {
		db.addAof(utils.ToCmdLine3("zremrangebyrank", args...))
	}
	return protocol.MakeIntReply(removed)
}

func execZPopMin(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	count := 1
	if len(args) > 1 {
		var err error
		count, err = strconv.Atoi(string(args[1]))
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
	}

	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	removed := sortedSet.PopMin(count)
	if len(removed) > 0 {
		db.addAof(utils.ToCmdLine3("zpopmin", args...))
	}
	result := make([][]byte, 0, len(removed)*2)
	for _, element := range removed {
		scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
		result = append(result, []byte(element.Member), []byte(scoreStr))
	}
	return protocol.MakeMultiBulkReply(result)
}

// execZRem removes given members
func execZRem(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}

	// get entity
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}

	var deleted int64 = 0
	for _, field := range fields {
		if sortedSet.Remove(field) {
			deleted++
		}
	}
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("zrem", args...))
	}
	return protocol.MakeIntReply(deleted)
}

func undoZRem(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}
	return rollbackZSetFields(db, key, fields...)
}

// execZIncrBy increments the score of a member
func execZIncrBy(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	rawDelta := string(args[1])
	field := string(args[2])
	delta, err := strconv.ParseFloat(rawDelta, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not a valid float")
	}

	// get or init entity
	sortedSet, _, errReply := db.getOrInitSortedSet(key)
	if errReply != nil {
		return errReply
	}

	element, exists := sortedSet.Get(field)
	if !exists {
		sortedSet.Add(field, delta)
		db.addAof(utils.ToCmdLine3("zincrby", args...))
		return protocol.MakeBulkReply(args[1])
	}
	score := element.Score + delta
	sortedSet.Add(field, score)
	bytes := []byte(strconv.FormatFloat(score, 'f', -1, 64))
	db.addAof(utils.ToCmdLine3("zincrby", args...))
	return protocol.MakeBulkReply(bytes)
}

func undoZIncr(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	field := string(args[2])
	return rollbackZSetFields(db, key, field)
}

func execZLexCount(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}

	minEle, maxEle := string(args[1]), string(args[2])
	min, err := SortedSet.ParseLexBorder(minEle)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	max, err := SortedSet.ParseLexBorder(maxEle)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	count := sortedSet.RangeCount(min, max)

	return protocol.MakeIntReply(count)
}

func execZRangeByLex(db *DB, args [][]byte) redis.Reply {
	n := len(args)
	if n > 3 && strings.ToLower(string(args[3])) != "limit" {
		return protocol.MakeErrReply("ERR syntax error")
	}
	if n != 3 && n != 6 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrangebylex' command")
	}

	key := string(args[0])
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}

	minEle, maxEle := string(args[1]), string(args[2])
	min, err := SortedSet.ParseLexBorder(minEle)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	max, err := SortedSet.ParseLexBorder(maxEle)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	offset := int64(0)
	limitCnt := int64(math.MaxInt64)
	if n > 3 {
		var err error
		offset, err = strconv.ParseInt(string(args[4]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		if offset < 0 {
			return protocol.MakeEmptyMultiBulkReply()
		}
		count, err := strconv.ParseInt(string(args[5]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		if count >= 0 {
			limitCnt = count
		}
	}

	elements := sortedSet.Range(min, max, offset, limitCnt, false)
	result := make([][]byte, 0, len(elements))
	for _, ele := range elements {
		result = append(result, []byte(ele.Member))
	}
	if len(result) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	return protocol.MakeMultiBulkReply(result)
}

func execZRemRangeByLex(db *DB, args [][]byte) redis.Reply {
	n := len(args)
	if n != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zremrangebylex' command")
	}

	key := string(args[0])
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}

	minEle, maxEle := string(args[1]), string(args[2])
	min, err := SortedSet.ParseLexBorder(minEle)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	max, err := SortedSet.ParseLexBorder(maxEle)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	count := sortedSet.RemoveRange(min, max)

	return protocol.MakeIntReply(count)
}

func execZRevRangeByLex(db *DB, args [][]byte) redis.Reply {
	n := len(args)
	if n > 3 && strings.ToLower(string(args[3])) != "limit" {
		return protocol.MakeErrReply("ERR syntax error")
	}
	if n != 3 && n != 6 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrangebylex' command")
	}

	key := string(args[0])
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}

	minEle, maxEle := string(args[2]), string(args[1])
	min, err := SortedSet.ParseLexBorder(minEle)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	max, err := SortedSet.ParseLexBorder(maxEle)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	offset := int64(0)
	limitCnt := int64(math.MaxInt64)
	if n > 3 {
		var err error
		offset, err = strconv.ParseInt(string(args[4]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		if offset < 0 {
			return protocol.MakeEmptyMultiBulkReply()
		}
		count, err := strconv.ParseInt(string(args[5]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		if count >= 0 {
			limitCnt = count
		}
	}

	elements := sortedSet.Range(min, max, offset, limitCnt, true)
	result := make([][]byte, 0, len(elements))
	for _, ele := range elements {
		result = append(result, []byte(ele.Member))
	}
	if len(result) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	return protocol.MakeMultiBulkReply(result)
}

func init() {
	registerCommand("ZAdd", execZAdd, writeFirstKey, undoZAdd, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("ZScore", execZScore, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ZIncrBy", execZIncrBy, writeFirstKey, undoZIncr, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("ZRank", execZRank, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ZCount", execZCount, readFirstKey, nil, 4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ZRevRank", execZRevRank, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ZCard", execZCard, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ZRange", execZRange, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ZRangeByScore", execZRangeByScore, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ZRevRange", execZRevRange, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ZRevRangeByScore", execZRevRangeByScore, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ZPopMin", execZPopMin, writeFirstKey, rollbackFirstKey, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("ZRem", execZRem, writeFirstKey, undoZRem, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("ZRemRangeByScore", execZRemRangeByScore, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("ZRemRangeByRank", execZRemRangeByRank, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("ZLexCount", execZLexCount, readFirstKey, nil, 4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ZRangeByLex", execZRangeByLex, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ZRemRangeByLex", execZRemRangeByLex, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("ZRevRangeByLex", execZRevRangeByLex, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
