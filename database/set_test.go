package database

import (
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"math/rand"
	"strconv"
	"testing"
)

// basic add get and remove
func TestSAdd(t *testing.T) {
	testDB.Flush()
	size := 100

	// test sadd
	key := utils.RandString(10)
	for i := 0; i < size; i++ {
		member := strconv.Itoa(i)
		result := testDB.Exec(nil, utils.ToCmdLine("sadd", key, member))
		asserts.AssertIntReply(t, result, 1)
	}
	// test scard
	result := testDB.Exec(nil, utils.ToCmdLine("SCard", key))
	asserts.AssertIntReply(t, result, size)

	// test is member
	for i := 0; i < size; i++ {
		member := strconv.Itoa(i)
		result = testDB.Exec(nil, utils.ToCmdLine("SIsMember", key, member))
		asserts.AssertIntReply(t, result, 1)
	}

	// test members
	result = testDB.Exec(nil, utils.ToCmdLine("SMembers", key))
	multiBulk, ok := result.(*protocol.MultiBulkReply)
	if !ok {
		t.Errorf("expected bulk protocol, actually %s", result.ToBytes())
		return
	}
	if len(multiBulk.Args) != size {
		t.Errorf("expected %d elements, actually %d", size, len(multiBulk.Args))
		return
	}
}

func TestSRem(t *testing.T) {
	testDB.Flush()
	size := 100

	// mock data
	key := utils.RandString(10)
	for i := 0; i < size; i++ {
		member := strconv.Itoa(i)
		testDB.Exec(nil, utils.ToCmdLine("sadd", key, member))
	}
	for i := 0; i < size; i++ {
		member := strconv.Itoa(i)
		testDB.Exec(nil, utils.ToCmdLine("srem", key, member))
		result := testDB.Exec(nil, utils.ToCmdLine("SIsMember", key, member))
		asserts.AssertIntReply(t, result, 0)
	}
}

func TestSPop(t *testing.T) {
	testDB.Flush()
	size := 100

	// mock data
	key := utils.RandString(10)
	for i := 0; i < size; i++ {
		member := strconv.Itoa(i)
		testDB.Exec(nil, utils.ToCmdLine("sadd", key, member))
	}

	result := testDB.Exec(nil, utils.ToCmdLine("spop", key))
	asserts.AssertMultiBulkReplySize(t, result, 1)

	currentSize := size - 1
	for currentSize > 0 {
		count := rand.Intn(currentSize) + 1
		resultSpop := testDB.Exec(nil, utils.ToCmdLine("spop", key, strconv.FormatInt(int64(count), 10)))
		multiBulk, ok := resultSpop.(*protocol.MultiBulkReply)
		if !ok {
			t.Errorf("expected bulk protocol, actually %s", resultSpop.ToBytes())
			return
		}
		removedSize := len(multiBulk.Args)
		for _, arg := range multiBulk.Args {
			resultSIsMember := testDB.Exec(nil, utils.ToCmdLine("SIsMember", key, string(arg)))
			asserts.AssertIntReply(t, resultSIsMember, 0)
		}
		currentSize -= removedSize
		resultSCard := testDB.Exec(nil, utils.ToCmdLine("SCard", key))
		asserts.AssertIntReply(t, resultSCard, currentSize)
	}
}

func TestSInter(t *testing.T) {
	testDB.Flush()
	size := 100
	step := 10

	keys := make([]string, 0)
	start := 0
	for i := 0; i < 4; i++ {
		key := utils.RandString(10) + strconv.Itoa(i)
		keys = append(keys, key)
		for j := start; j < size+start; j++ {
			member := strconv.Itoa(j)
			testDB.Exec(nil, utils.ToCmdLine("sadd", key, member))
		}
		start += step
	}
	result := testDB.Exec(nil, utils.ToCmdLine2("sinter", keys...))
	asserts.AssertMultiBulkReplySize(t, result, 70)

	destKey := utils.RandString(10)
	keysWithDest := []string{destKey}
	keysWithDest = append(keysWithDest, keys...)
	result = testDB.Exec(nil, utils.ToCmdLine2("SInterStore", keysWithDest...))
	asserts.AssertIntReply(t, result, 70)

	// test empty set
	testDB.Flush()
	key0 := utils.RandString(10)
	testDB.Remove(key0)
	key1 := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("sadd", key1, "a", "b"))
	key2 := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("sadd", key2, "1", "2"))
	result = testDB.Exec(nil, utils.ToCmdLine("sinter", key0, key1, key2))
	asserts.AssertMultiBulkReplySize(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("sinter", key1, key2))
	asserts.AssertMultiBulkReplySize(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("sinterstore", utils.RandString(10), key0, key1, key2))
	asserts.AssertIntReply(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("sinterstore", utils.RandString(10), key1, key2))
	asserts.AssertIntReply(t, result, 0)
}

func TestSUnion(t *testing.T) {
	testDB.Flush()
	size := 100
	step := 10

	keys := make([]string, 0)
	start := 0
	for i := 0; i < 4; i++ {
		key := utils.RandString(10)
		keys = append(keys, key)
		for j := start; j < size+start; j++ {
			member := strconv.Itoa(j)
			testDB.Exec(nil, utils.ToCmdLine("sadd", key, member))
		}
		start += step
	}
	result := testDB.Exec(nil, utils.ToCmdLine2("sunion", keys...))
	asserts.AssertMultiBulkReplySize(t, result, 130)

	destKey := utils.RandString(10)
	keysWithDest := []string{destKey}
	keysWithDest = append(keysWithDest, keys...)
	result = testDB.Exec(nil, utils.ToCmdLine2("SUnionStore", keysWithDest...))
	asserts.AssertIntReply(t, result, 130)
}

func TestSDiff(t *testing.T) {
	testDB.Flush()
	size := 100
	step := 20

	keys := make([]string, 0)
	start := 0
	for i := 0; i < 3; i++ {
		key := utils.RandString(10)
		keys = append(keys, key)
		for j := start; j < size+start; j++ {
			member := strconv.Itoa(j)
			testDB.Exec(nil, utils.ToCmdLine("sadd", key, member))
		}
		start += step
	}
	result := testDB.Exec(nil, utils.ToCmdLine2("SDiff", keys...))
	asserts.AssertMultiBulkReplySize(t, result, step)

	destKey := utils.RandString(10)
	keysWithDest := []string{destKey}
	keysWithDest = append(keysWithDest, keys...)
	result = testDB.Exec(nil, utils.ToCmdLine2("SDiffStore", keysWithDest...))
	asserts.AssertIntReply(t, result, step)

	// test empty set
	testDB.Flush()
	key0 := utils.RandString(10)
	testDB.Remove(key0)
	key1 := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("sadd", key1, "a", "b"))
	key2 := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("sadd", key2, "a", "b"))
	result = testDB.Exec(nil, utils.ToCmdLine("sdiff", key0, key1, key2))
	asserts.AssertMultiBulkReplySize(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("sdiff", key1, key2))
	asserts.AssertMultiBulkReplySize(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("SDiffStore", utils.RandString(10), key0, key1, key2))
	asserts.AssertIntReply(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("SDiffStore", utils.RandString(10), key1, key2))
	asserts.AssertIntReply(t, result, 0)
}

func TestSRandMember(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	for j := 0; j < 100; j++ {
		member := strconv.Itoa(j)
		testDB.Exec(nil, utils.ToCmdLine("sadd", key, member))
	}
	result := testDB.Exec(nil, utils.ToCmdLine("SRandMember", key))
	br, ok := result.(*protocol.BulkReply)
	if !ok && len(br.Arg) > 0 {
		t.Errorf("expected bulk protocol, actually %s", result.ToBytes())
		return
	}

	result = testDB.Exec(nil, utils.ToCmdLine("SRandMember", key, "10"))
	asserts.AssertMultiBulkReplySize(t, result, 10)
	multiBulk, ok := result.(*protocol.MultiBulkReply)
	if !ok {
		t.Errorf("expected bulk protocol, actually %s", result.ToBytes())
		return
	}
	m := make(map[string]struct{})
	for _, arg := range multiBulk.Args {
		m[string(arg)] = struct{}{}
	}
	if len(m) != 10 {
		t.Errorf("expected 10 members, actually %d", len(m))
		return
	}

	result = testDB.Exec(nil, utils.ToCmdLine("SRandMember", key, "110"))
	asserts.AssertMultiBulkReplySize(t, result, 100)

	result = testDB.Exec(nil, utils.ToCmdLine("SRandMember", key, "-10"))
	asserts.AssertMultiBulkReplySize(t, result, 10)

	result = testDB.Exec(nil, utils.ToCmdLine("SRandMember", key, "-110"))
	asserts.AssertMultiBulkReplySize(t, result, 110)
}
