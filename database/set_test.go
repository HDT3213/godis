package database

import (
	"fmt"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/reply"
	"github.com/hdt3213/godis/redis/reply/asserts"
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
	multiBulk, ok := result.(*reply.MultiBulkReply)
	if !ok {
		t.Error(fmt.Sprintf("expected bulk reply, actually %s", result.ToBytes()))
		return
	}
	if len(multiBulk.Args) != size {
		t.Error(fmt.Sprintf("expected %d elements, actually %d", size, len(multiBulk.Args)))
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

func TestSInter(t *testing.T) {
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
	testDB.Exec(nil, utils.ToCmdLine("sadd", key1, "1", "2"))
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
	br, ok := result.(*reply.BulkReply)
	if !ok && len(br.Arg) > 0 {
		t.Error(fmt.Sprintf("expected bulk reply, actually %s", result.ToBytes()))
		return
	}

	result = testDB.Exec(nil, utils.ToCmdLine("SRandMember", key, "10"))
	asserts.AssertMultiBulkReplySize(t, result, 10)
	multiBulk, ok := result.(*reply.MultiBulkReply)
	if !ok {
		t.Error(fmt.Sprintf("expected bulk reply, actually %s", result.ToBytes()))
		return
	}
	m := make(map[string]struct{})
	for _, arg := range multiBulk.Args {
		m[string(arg)] = struct{}{}
	}
	if len(m) != 10 {
		t.Error(fmt.Sprintf("expected 10 members, actually %d", len(m)))
		return
	}

	result = testDB.Exec(nil, utils.ToCmdLine("SRandMember", key, "110"))
	asserts.AssertMultiBulkReplySize(t, result, 100)

	result = testDB.Exec(nil, utils.ToCmdLine("SRandMember", key, "-10"))
	asserts.AssertMultiBulkReplySize(t, result, 10)

	result = testDB.Exec(nil, utils.ToCmdLine("SRandMember", key, "-110"))
	asserts.AssertMultiBulkReplySize(t, result, 110)
}
