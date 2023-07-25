package database

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol/asserts"
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

func TestZLexCount(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	// a b c d e
	result := testDB.Exec(nil, utils.ToCmdLine("ZAdd", key, "0", "e", "0", "d", "0", "c", "0", "b", "0", "a"))
	asserts.AssertNotError(t, result)

	// case1
	result1 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "(-", "(+"))
	asserts.AssertIntReply(t, result1, 0)

	// case2
	result2 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "(-", "(g"))
	asserts.AssertIntReply(t, result2, 5)

	// case3
	result3 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "(-", "(c"))
	asserts.AssertIntReply(t, result3, 2)

	// case4
	result4 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "(-", "[c"))
	asserts.AssertIntReply(t, result4, 3)

	// case5
	result5 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "(a", "(+"))
	asserts.AssertIntReply(t, result5, 0)

	// case6
	result6 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "[-", "[+"))
	asserts.AssertIntReply(t, result6, 0)

	// case
	result7 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "[-", "(g"))
	asserts.AssertIntReply(t, result7, 5)

	// case8
	result8 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "[-", "(c"))
	asserts.AssertIntReply(t, result8, 2)

	// case9
	result9 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "[-", "[c"))
	asserts.AssertIntReply(t, result9, 3)

	// case10
	result10 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "(a", "[+"))
	asserts.AssertIntReply(t, result10, 0)

	// case11
	result11 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "-", "+"))
	asserts.AssertIntReply(t, result11, 5)

	// case12
	result12 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "-", "(c"))
	asserts.AssertIntReply(t, result12, 2)

	// case13
	result13 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "-", "[c"))
	asserts.AssertIntReply(t, result13, 3)

	// case14
	result14 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "(aa", "(c"))
	asserts.AssertIntReply(t, result14, 1)

	// case15
	result15 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "(aa", "[c"))
	asserts.AssertIntReply(t, result15, 2)

	// case16
	result16 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "[aa", "(c"))
	asserts.AssertIntReply(t, result16, 1)

	// case17
	result17 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "[aa", "[c"))
	asserts.AssertIntReply(t, result17, 2)

	// case18
	result18 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "(a", "(ee"))
	asserts.AssertIntReply(t, result18, 4)

	// case19
	result19 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "(a", "[ee"))
	asserts.AssertIntReply(t, result19, 4)

	// case20
	result20 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "[a", "(ee"))
	asserts.AssertIntReply(t, result20, 5)

	// case21
	result21 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "[a", "[ee"))
	asserts.AssertIntReply(t, result21, 5)

	// case22
	result22 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "(aa", "(ee"))
	asserts.AssertIntReply(t, result22, 4)

	// case23
	result23 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "(aa", "[ee"))
	asserts.AssertIntReply(t, result23, 4)

	// case24
	result24 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "[aa", "(ee"))
	asserts.AssertIntReply(t, result24, 4)

	// case25
	result25 := testDB.Exec(nil, utils.ToCmdLine("ZLexCount", key, "[aa", "[ee"))
	asserts.AssertIntReply(t, result25, 4)
}

func TestZRangeByLex(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	// a b c d e
	result := testDB.Exec(nil, utils.ToCmdLine("ZAdd", key, "0", "e", "0", "d", "0", "c", "0", "b", "0", "a"))
	asserts.AssertNotError(t, result)

	// case1
	result1 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "-", "+"))
	asserts.AssertMultiBulkReply(t, result1, []string{"a", "b", "c", "d", "e"})

	// case2
	result2 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "-", "(z"))
	asserts.AssertMultiBulkReply(t, result2, []string{"a", "b", "c", "d", "e"})

	// case3
	result3 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "(-", "[z"))
	asserts.AssertMultiBulkReply(t, result3, []string{"a", "b", "c", "d", "e"})

	// case4
	result4 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "[a", "[e"))
	asserts.AssertMultiBulkReply(t, result4, []string{"a", "b", "c", "d", "e"})

	// case5
	result5 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "(a", "[e"))
	asserts.AssertMultiBulkReply(t, result5, []string{"b", "c", "d", "e"})

	// case6
	result6 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "[a", "(e"))
	asserts.AssertMultiBulkReply(t, result6, []string{"a", "b", "c", "d"})

	// case7
	result7 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "(a", "(e"))
	asserts.AssertMultiBulkReply(t, result7, []string{"b", "c", "d"})

	// case8
	result8 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "(aa", "(ee"))
	asserts.AssertMultiBulkReply(t, result8, []string{"b", "c", "d", "e"})

	// case9
	result9 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "(aa", "[ee"))
	asserts.AssertMultiBulkReply(t, result9, []string{"b", "c", "d", "e"})

	// case10
	result10 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "[aa", "(ee"))
	asserts.AssertMultiBulkReply(t, result10, []string{"b", "c", "d", "e"})

	// case11
	result11 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "[aa", "[ee"))
	asserts.AssertMultiBulkReply(t, result11, []string{"b", "c", "d", "e"})

	// case12
	result12 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "(aa", "(e"))
	asserts.AssertMultiBulkReply(t, result12, []string{"b", "c", "d"})

	// case13
	result13 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "(aa", "[e"))
	asserts.AssertMultiBulkReply(t, result13, []string{"b", "c", "d", "e"})

	// case14
	result14 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "[aa", "(e"))
	asserts.AssertMultiBulkReply(t, result14, []string{"b", "c", "d"})

	// case15
	result15 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "[aa", "[e"))
	asserts.AssertMultiBulkReply(t, result15, []string{"b", "c", "d", "e"})

	// case16
	result16 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "(a", "(ee"))
	asserts.AssertMultiBulkReply(t, result16, []string{"b", "c", "d", "e"})

	// case17
	result17 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "(a", "[ee"))
	asserts.AssertMultiBulkReply(t, result17, []string{"b", "c", "d", "e"})

	// case18
	result18 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "[a", "(ee"))
	asserts.AssertMultiBulkReply(t, result18, []string{"a", "b", "c", "d", "e"})

	// case19
	result19 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "[a", "[ee"))
	asserts.AssertMultiBulkReply(t, result19, []string{"a", "b", "c", "d", "e"})

	// case20
	result20 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "(-", "(+"))
	asserts.AssertMultiBulkReplySize(t, result20, 0)

	// case21
	result21 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "(a", "(+"))
	asserts.AssertMultiBulkReplySize(t, result21, 0)

	// case22
	result22 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "[-", "[+"))
	asserts.AssertMultiBulkReplySize(t, result22, 0)

	// case23
	result23 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "(z", "(g"))
	asserts.AssertMultiBulkReplySize(t, result23, 0)

	// case24
	result24 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "[-", "(g"))
	asserts.AssertMultiBulkReply(t, result24, []string{"a", "b", "c", "d", "e"})

	// case25
	result25 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "[-", "(c"))
	asserts.AssertMultiBulkReply(t, result25, []string{"a", "b"})

	// case26
	result26 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "[-", "[c"))
	asserts.AssertMultiBulkReply(t, result26, []string{"a", "b", "c"})

	// case27
	result27 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "(a", "(e", "limit", "0", "-1"))
	asserts.AssertMultiBulkReply(t, result27, []string{"b", "c", "d"})

	// case28
	result28 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "(a", "(e", "limit", "0", "1"))
	asserts.AssertMultiBulkReply(t, result28, []string{"b"})

	// case28
	result29 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "(a", "(e", "limit", "-1", "1"))
	asserts.AssertMultiBulkReplySize(t, result29, 0)

	// case30
	result30 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "[a", "[e", "limit", "2", "100"))
	asserts.AssertMultiBulkReply(t, result30, []string{"c", "d", "e"})

	// case30
	result31 := testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "-", "+", "limit", "2", "2"))
	asserts.AssertMultiBulkReply(t, result31, []string{"c", "d"})

}

func TestZRemRangeByLex(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	// a b c d e
	asserts.AssertNotError(t, testDB.Exec(nil, utils.ToCmdLine("ZAdd", key, "0", "e", "0", "d", "0", "c", "0", "b", "0", "a")))

	// case1
	asserts.AssertIntReply(t,
		testDB.Exec(nil, utils.ToCmdLine("ZRemRangeByLex", key, "-", "+")),
		5)

	asserts.AssertMultiBulkReplySize(t,
		testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "-", "+")),
		0)

	// case2
	testDB.Exec(nil, utils.ToCmdLine("ZAdd", key, "0", "e", "0", "d", "0", "c", "0", "b", "0", "a"))

	asserts.AssertIntReply(t,
		testDB.Exec(nil, utils.ToCmdLine("ZRemRangeByLex", key, "-", "[c")),
		3)

	asserts.AssertMultiBulkReply(t,
		testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "-", "+")),
		[]string{"d", "e"})

	// case3
	testDB.Exec(nil, utils.ToCmdLine("ZAdd", key, "0", "a", "0", "b", "0", "c"))

	asserts.AssertMultiBulkReply(t,
		testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "-", "+")),
		[]string{"a", "b", "c", "d", "e"})

	asserts.AssertIntReply(t,
		testDB.Exec(nil, utils.ToCmdLine("ZRemRangeByLex", key, "(c", "+")),
		2)

	asserts.AssertMultiBulkReply(t,
		testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "-", "+")),
		[]string{"a", "b", "c"})

	// case4
	testDB.Exec(nil, utils.ToCmdLine("ZAdd", key, "0", "d", "0", "e"))

	asserts.AssertMultiBulkReply(t,
		testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "-", "+")),
		[]string{"a", "b", "c", "d", "e"})

	asserts.AssertIntReply(t,
		testDB.Exec(nil, utils.ToCmdLine("ZRemRangeByLex", key, "(a", "(d")),
		2)

	asserts.AssertMultiBulkReply(t,
		testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "-", "+")),
		[]string{"a", "d", "e"})

	// case5
	testDB.Exec(nil, utils.ToCmdLine("ZAdd", key, "0", "b", "0", "c"))

	asserts.AssertMultiBulkReply(t,
		testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "-", "+")),
		[]string{"a", "b", "c", "d", "e"})

	asserts.AssertIntReply(t,
		testDB.Exec(nil, utils.ToCmdLine("ZRemRangeByLex", key, "[a", "[d")),
		4)

	asserts.AssertMultiBulkReply(t,
		testDB.Exec(nil, utils.ToCmdLine("ZRangeByLex", key, "-", "+")),
		[]string{"e"})
}

func TestZRevRangeByLex(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	// a b c d e
	result := testDB.Exec(nil, utils.ToCmdLine("ZAdd", key, "0", "e", "0", "d", "0", "c", "0", "b", "0", "a"))
	asserts.AssertNotError(t, result)

	// case1
	result1 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "+", "-"))
	asserts.AssertMultiBulkReply(t, result1, []string{"e", "d", "c", "b", "a"})

	// case2
	result2 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "(z", "-"))
	asserts.AssertMultiBulkReply(t, result2, []string{"e", "d", "c", "b", "a"})

	// case3
	result3 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "[z", "-"))
	asserts.AssertMultiBulkReply(t, result3, []string{"e", "d", "c", "b", "a"})

	// case4
	result4 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "[e", "[a"))
	asserts.AssertMultiBulkReply(t, result4, []string{"e", "d", "c", "b", "a"})

	// case5
	result5 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "[e", "(a"))
	asserts.AssertMultiBulkReply(t, result5, []string{"e", "d", "c", "b"})

	// case6
	result6 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "(e", "[a"))
	asserts.AssertMultiBulkReply(t, result6, []string{"d", "c", "b", "a"})

	// case7
	result7 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "(e", "(a"))
	asserts.AssertMultiBulkReply(t, result7, []string{"d", "c", "b"})

	// case8
	result8 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "(ee", "(aa"))
	asserts.AssertMultiBulkReply(t, result8, []string{"e", "d", "c", "b"})

	// case9
	result9 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "[ee", "(aa"))
	asserts.AssertMultiBulkReply(t, result9, []string{"e", "d", "c", "b"})

	// case10
	result10 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "(ee", "[aa"))
	asserts.AssertMultiBulkReply(t, result10, []string{"e", "d", "c", "b"})

	// case11
	result11 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "[ee", "[aa"))
	asserts.AssertMultiBulkReply(t, result11, []string{"e", "d", "c", "b"})

	// case12
	result12 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "(e", "(aa"))
	asserts.AssertMultiBulkReply(t, result12, []string{"d", "c", "b"})

	// case13
	result13 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "[e", "(aa"))
	asserts.AssertMultiBulkReply(t, result13, []string{"e", "d", "c", "b"})

	// case14
	result14 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "(e", "[aa"))
	asserts.AssertMultiBulkReply(t, result14, []string{"d", "c", "b"})

	// case15
	result15 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "[e", "[aa"))
	asserts.AssertMultiBulkReply(t, result15, []string{"e", "d", "c", "b"})

	// case16
	result16 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "(ee", "(a"))
	asserts.AssertMultiBulkReply(t, result16, []string{"e", "d", "c", "b"})

	// case17
	result17 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "[ee", "(a"))
	asserts.AssertMultiBulkReply(t, result17, []string{"e", "d", "c", "b"})

	// case18
	result18 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "(ee", "[a"))
	asserts.AssertMultiBulkReply(t, result18, []string{"e", "d", "c", "b", "a"})

	// case19
	result19 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "[ee", "[a"))
	asserts.AssertMultiBulkReply(t, result19, []string{"e", "d", "c", "b", "a"})

	// case20
	result20 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "(+", "(-"))
	asserts.AssertMultiBulkReplySize(t, result20, 0)

	// case21
	result21 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "(+", "(a"))
	asserts.AssertMultiBulkReplySize(t, result21, 0)

	// case22
	result22 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "[+", "[-"))
	asserts.AssertMultiBulkReplySize(t, result22, 0)

	// case23
	result23 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "(g", "[-"))
	asserts.AssertMultiBulkReply(t, result23, []string{"e", "d", "c", "b", "a"})

	// case24
	result24 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "(c", "[-"))
	asserts.AssertMultiBulkReply(t, result24, []string{"b", "a"})

	// case25
	result25 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "[c", "[-"))
	asserts.AssertMultiBulkReply(t, result25, []string{"c", "b", "a"})

	// case26
	result26 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "(e", "(a", "limit", "0", "-1"))
	asserts.AssertMultiBulkReply(t, result26, []string{"d", "c", "b"})

	// case27
	result27 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "(e", "(a", "limit", "0", "1"))
	asserts.AssertMultiBulkReply(t, result27, []string{"d"})

	// case28
	result28 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "(e", "(a", "limit", "-1", "1"))
	asserts.AssertMultiBulkReplySize(t, result28, 0)

	// case29
	result29 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "[e", "[a", "limit", "2", "100"))
	asserts.AssertMultiBulkReply(t, result29, []string{"c", "b", "a"})

	// case30
	result30 := testDB.Exec(nil, utils.ToCmdLine("ZRevRangeByLex", key, "+", "-", "limit", "2", "2"))
	asserts.AssertMultiBulkReply(t, result30, []string{"c", "b"})
}
