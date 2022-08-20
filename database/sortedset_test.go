package database

import (
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"math/rand"
	"strconv"
	"testing"
)

func TestZAdd(t *testing.T) {
	testDB.Flush()
	size := 100

	// add new members
	key := utils.RandString(10)
	members := make([]string, size)
	scores := make([]float64, size)
	setArgs := []string{key}
	for i := 0; i < size; i++ {
		members[i] = utils.RandString(10)
		scores[i] = rand.Float64()
		setArgs = append(setArgs, strconv.FormatFloat(scores[i], 'f', -1, 64), members[i])
	}
	result := testDB.Exec(nil, utils.ToCmdLine2("zadd", setArgs...))
	asserts.AssertIntReply(t, result, size)

	// test zscore and zrank
	for i, member := range members {
		result = testDB.Exec(nil, utils.ToCmdLine("ZScore", key, member))
		score := strconv.FormatFloat(scores[i], 'f', -1, 64)
		asserts.AssertBulkReply(t, result, score)
	}

	// test zcard
	result = testDB.Exec(nil, utils.ToCmdLine("zcard", key))
	asserts.AssertIntReply(t, result, size)

	// update members
	setArgs = []string{key}
	for i := 0; i < size; i++ {
		scores[i] = rand.Float64() + 100
		setArgs = append(setArgs, strconv.FormatFloat(scores[i], 'f', -1, 64), members[i])
	}
	result = testDB.Exec(nil, utils.ToCmdLine2("zadd", setArgs...))
	asserts.AssertIntReply(t, result, 0) // return number of new members

	// test updated score
	for i, member := range members {
		result = testDB.Exec(nil, utils.ToCmdLine("zscore", key, member))
		score := strconv.FormatFloat(scores[i], 'f', -1, 64)
		asserts.AssertBulkReply(t, result, score)
	}
}

func TestZRank(t *testing.T) {
	testDB.Flush()
	size := 100
	key := utils.RandString(10)
	members := make([]string, size)
	scores := make([]int, size)
	setArgs := []string{key}
	for i := 0; i < size; i++ {
		members[i] = utils.RandString(10)
		scores[i] = i
		setArgs = append(setArgs, strconv.FormatInt(int64(scores[i]), 10), members[i])
	}
	testDB.Exec(nil, utils.ToCmdLine2("zadd", setArgs...))

	// test  zrank
	for i, member := range members {
		result := testDB.Exec(nil, utils.ToCmdLine("zrank", key, member))
		asserts.AssertIntReply(t, result, i)
		result = testDB.Exec(nil, utils.ToCmdLine("ZRevRank", key, member))
		asserts.AssertIntReply(t, result, size-i-1)
	}
}

func TestZRange(t *testing.T) {
	// prepare
	testDB.Flush()
	size := 100
	key := utils.RandString(10)
	members := make([]string, size)
	scores := make([]int, size)
	setArgs := []string{key}
	for i := 0; i < size; i++ {
		members[i] = strconv.Itoa(i) //utils.RandString(10)
		scores[i] = i
		setArgs = append(setArgs, strconv.FormatInt(int64(scores[i]), 10), members[i])
	}
	testDB.Exec(nil, utils.ToCmdLine2("zadd", setArgs...))
	reverseMembers := make([]string, size)
	for i, v := range members {
		reverseMembers[size-i-1] = v
	}

	start := "0"
	end := "9"
	result := testDB.Exec(nil, utils.ToCmdLine("ZRange", key, start, end))
	asserts.AssertMultiBulkReply(t, result, members[0:10])
	result = testDB.Exec(nil, utils.ToCmdLine("ZRange", key, start, end, "WITHSCORES"))
	asserts.AssertMultiBulkReplySize(t, result, 20)
	result = testDB.Exec(nil, utils.ToCmdLine("ZRevRange", key, start, end))
	asserts.AssertMultiBulkReply(t, result, reverseMembers[0:10])

	start = "0"
	end = "200"
	result = testDB.Exec(nil, utils.ToCmdLine("ZRange", key, start, end))
	asserts.AssertMultiBulkReply(t, result, members)
	result = testDB.Exec(nil, utils.ToCmdLine("ZRevRange", key, start, end))
	asserts.AssertMultiBulkReply(t, result, reverseMembers)

	start = "0"
	end = "-10"
	result = testDB.Exec(nil, utils.ToCmdLine("ZRange", key, start, end))
	asserts.AssertMultiBulkReply(t, result, members[0:size-10+1])
	result = testDB.Exec(nil, utils.ToCmdLine("ZRevRange", key, start, end))
	asserts.AssertMultiBulkReply(t, result, reverseMembers[0:size-10+1])

	start = "0"
	end = "-200"
	result = testDB.Exec(nil, utils.ToCmdLine("ZRange", key, start, end))
	asserts.AssertMultiBulkReply(t, result, members[0:0])
	result = testDB.Exec(nil, utils.ToCmdLine("ZRevRange", key, start, end))
	asserts.AssertMultiBulkReply(t, result, reverseMembers[0:0])

	start = "-10"
	end = "-1"
	result = testDB.Exec(nil, utils.ToCmdLine("ZRange", key, start, end))
	asserts.AssertMultiBulkReply(t, result, members[90:])
	result = testDB.Exec(nil, utils.ToCmdLine("ZRevRange", key, start, end))
	asserts.AssertMultiBulkReply(t, result, reverseMembers[90:])
}

func reverse(src []string) []string {
	result := make([]string, len(src))
	for i, v := range src {
		result[len(src)-i-1] = v
	}
	return result
}

func TestZRangeByScore(t *testing.T) {
	// prepare
	testDB.Flush()
	size := 100
	key := utils.RandString(10)
	members := make([]string, size)
	scores := make([]int, size)
	setArgs := []string{key}
	for i := 0; i < size; i++ {
		members[i] = strconv.FormatInt(int64(i), 10)
		scores[i] = i
		setArgs = append(setArgs, strconv.FormatInt(int64(scores[i]), 10), members[i])
	}
	result := testDB.Exec(nil, utils.ToCmdLine2("zadd", setArgs...))
	asserts.AssertIntReply(t, result, size)

	min := "20"
	max := "30"
	result = testDB.Exec(nil, utils.ToCmdLine("ZRangeByScore", key, min, max))
	asserts.AssertMultiBulkReply(t, result, members[20:31])
	result = testDB.Exec(nil, utils.ToCmdLine("ZRangeByScore", key, min, max, "WithScores"))
	asserts.AssertMultiBulkReplySize(t, result, 22)
	result = execZRevRangeByScore(testDB, utils.ToCmdLine(key, max, min))
	result = testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByScore", key, max, min))
	asserts.AssertMultiBulkReply(t, result, reverse(members[20:31]))

	min = "-10"
	max = "10"
	result = testDB.Exec(nil, utils.ToCmdLine("ZRangeByScore", key, min, max))
	asserts.AssertMultiBulkReply(t, result, members[0:11])
	result = testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByScore", key, max, min))
	asserts.AssertMultiBulkReply(t, result, reverse(members[0:11]))

	min = "90"
	max = "110"
	result = testDB.Exec(nil, utils.ToCmdLine("ZRangeByScore", key, min, max))
	asserts.AssertMultiBulkReply(t, result, members[90:])
	result = testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByScore", key, max, min))
	asserts.AssertMultiBulkReply(t, result, reverse(members[90:]))

	min = "(20"
	max = "(30"
	result = testDB.Exec(nil, utils.ToCmdLine("ZRangeByScore", key, min, max))
	asserts.AssertMultiBulkReply(t, result, members[21:30])
	result = testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByScore", key, max, min))
	asserts.AssertMultiBulkReply(t, result, reverse(members[21:30]))

	min = "20"
	max = "40"
	result = testDB.Exec(nil, utils.ToCmdLine("ZRangeByScore", key, min, max, "LIMIT", "5", "5"))
	asserts.AssertMultiBulkReply(t, result, members[25:30])
	result = testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByScore", key, max, min, "LIMIT", "5", "5"))
	asserts.AssertMultiBulkReply(t, result, reverse(members[31:36]))
}

func TestZRem(t *testing.T) {
	testDB.Flush()
	size := 100
	key := utils.RandString(10)
	members := make([]string, size)
	scores := make([]int, size)
	setArgs := []string{key}
	for i := 0; i < size; i++ {
		members[i] = strconv.FormatInt(int64(i), 10)
		scores[i] = i
		setArgs = append(setArgs, strconv.FormatInt(int64(scores[i]), 10), members[i])
	}
	testDB.Exec(nil, utils.ToCmdLine2("zadd", setArgs...))

	args := []string{key}
	args = append(args, members[0:10]...)
	result := testDB.Exec(nil, utils.ToCmdLine2("zrem", args...))
	asserts.AssertIntReply(t, result, 10)
	result = testDB.Exec(nil, utils.ToCmdLine("zcard", key))
	asserts.AssertIntReply(t, result, size-10)

	// test ZRemRangeByRank
	testDB.Flush()
	size = 100
	key = utils.RandString(10)
	members = make([]string, size)
	scores = make([]int, size)
	setArgs = []string{key}
	for i := 0; i < size; i++ {
		members[i] = strconv.FormatInt(int64(i), 10)
		scores[i] = i
		setArgs = append(setArgs, strconv.FormatInt(int64(scores[i]), 10), members[i])
	}
	testDB.Exec(nil, utils.ToCmdLine2("zadd", setArgs...))

	result = testDB.Exec(nil, utils.ToCmdLine("ZRemRangeByRank", key, "0", "9"))
	asserts.AssertIntReply(t, result, 10)
	result = testDB.Exec(nil, utils.ToCmdLine("zcard", key))
	asserts.AssertIntReply(t, result, size-10)

	// test ZRemRangeByScore
	testDB.Flush()
	size = 100
	key = utils.RandString(10)
	members = make([]string, size)
	scores = make([]int, size)
	setArgs = []string{key}
	for i := 0; i < size; i++ {
		members[i] = strconv.FormatInt(int64(i), 10)
		scores[i] = i
		setArgs = append(setArgs, strconv.FormatInt(int64(scores[i]), 10), members[i])
	}
	testDB.Exec(nil, utils.ToCmdLine2("zadd", setArgs...))

	result = testDB.Exec(nil, utils.ToCmdLine("ZRemRangeByScore", key, "0", "9"))
	asserts.AssertIntReply(t, result, 10)
	result = testDB.Exec(nil, utils.ToCmdLine("zcard", key))
	asserts.AssertIntReply(t, result, size-10)
}

func TestZCount(t *testing.T) {
	// prepare
	testDB.Flush()
	size := 100
	key := utils.RandString(10)
	members := make([]string, size)
	scores := make([]int, size)
	setArgs := []string{key}
	for i := 0; i < size; i++ {
		members[i] = strconv.FormatInt(int64(i), 10)
		scores[i] = i
		setArgs = append(setArgs, strconv.FormatInt(int64(scores[i]), 10), members[i])
	}
	testDB.Exec(nil, utils.ToCmdLine2("zadd", setArgs...))

	min := "20"
	max := "30"
	result := testDB.Exec(nil, utils.ToCmdLine("zcount", key, min, max))
	asserts.AssertIntReply(t, result, 11)

	min = "-10"
	max = "10"
	result = testDB.Exec(nil, utils.ToCmdLine("zcount", key, min, max))
	asserts.AssertIntReply(t, result, 11)

	min = "90"
	max = "110"
	result = testDB.Exec(nil, utils.ToCmdLine("zcount", key, min, max))
	asserts.AssertIntReply(t, result, 10)

	min = "(20"
	max = "(30"
	result = testDB.Exec(nil, utils.ToCmdLine("zcount", key, min, max))
	asserts.AssertIntReply(t, result, 9)
}

func TestZIncrBy(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	result := testDB.Exec(nil, utils.ToCmdLine("ZIncrBy", key, "10", "a"))
	asserts.AssertBulkReply(t, result, "10")
	result = testDB.Exec(nil, utils.ToCmdLine("ZIncrBy", key, "10", "a"))
	asserts.AssertBulkReply(t, result, "20")

	result = testDB.Exec(nil, utils.ToCmdLine("ZScore", key, "a"))
	asserts.AssertBulkReply(t, result, "20")
}

func TestZPopMin(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	result := testDB.Exec(nil, utils.ToCmdLine("ZAdd", key, "1", "a", "1", "b", "2", "c"))
	asserts.AssertNotError(t, result)
	result = testDB.Exec(nil, utils.ToCmdLine("ZPopMin", key, "2"))
	asserts.AssertMultiBulkReply(t, result, []string{"a", "1", "b", "1"})
	result = testDB.Exec(nil, utils.ToCmdLine("ZRange", key, "0", "-1"))
	asserts.AssertMultiBulkReply(t, result, []string{"c"})

	result = testDB.Exec(nil, utils.ToCmdLine("ZPopMin", key+"1", "2"))
	asserts.AssertMultiBulkReplySize(t, result, 0)

	testDB.Exec(nil, utils.ToCmdLine("set", key+"2", "2"))
	result = testDB.Exec(nil, utils.ToCmdLine("ZPopMin", key+"2", "2"))
	asserts.AssertErrReply(t, result, "WRONGTYPE Operation against a key holding the wrong kind of value")
}
