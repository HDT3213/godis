package godis

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
	FlushAll(testDB, [][]byte{})
	size := 100

	// test sadd
	key := utils.RandString(10)
	for i := 0; i < size; i++ {
		member := strconv.Itoa(i)
		result := SAdd(testDB, utils.ToBytesList(key, member))
		asserts.AssertIntReply(t, result, 1)
	}
	// test scard
	result := SCard(testDB, utils.ToBytesList(key))
	asserts.AssertIntReply(t, result, size)

	// test is member
	for i := 0; i < size; i++ {
		member := strconv.Itoa(i)
		result := SIsMember(testDB, utils.ToBytesList(key, member))
		asserts.AssertIntReply(t, result, 1)
	}

	// test members
	result = SMembers(testDB, utils.ToBytesList(key))
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
	FlushAll(testDB, [][]byte{})
	size := 100

	// mock data
	key := utils.RandString(10)
	for i := 0; i < size; i++ {
		member := strconv.Itoa(i)
		SAdd(testDB, utils.ToBytesList(key, member))
	}
	for i := 0; i < size; i++ {
		member := strconv.Itoa(i)
		SRem(testDB, utils.ToBytesList(key, member))
		result := SIsMember(testDB, utils.ToBytesList(key, member))
		asserts.AssertIntReply(t, result, 0)
	}
}

func TestSInter(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	size := 100
	step := 10

	keys := make([]string, 0)
	start := 0
	for i := 0; i < 4; i++ {
		key := utils.RandString(10)
		keys = append(keys, key)
		for j := start; j < size+start; j++ {
			member := strconv.Itoa(j)
			SAdd(testDB, utils.ToBytesList(key, member))
		}
		start += step
	}
	result := SInter(testDB, utils.ToBytesList(keys...))
	asserts.AssertMultiBulkReplySize(t, result, 70)

	destKey := utils.RandString(10)
	keysWithDest := []string{destKey}
	keysWithDest = append(keysWithDest, keys...)
	result = SInterStore(testDB, utils.ToBytesList(keysWithDest...))
	asserts.AssertIntReply(t, result, 70)

	// test empty set
	FlushAll(testDB, [][]byte{})
	key0 := utils.RandString(10)
	Del(testDB, utils.ToBytesList(key0))
	key1 := utils.RandString(10)
	SAdd(testDB, utils.ToBytesList(key1, "a", "b"))
	key2 := utils.RandString(10)
	SAdd(testDB, utils.ToBytesList(key2, "1", "2"))
	result = SInter(testDB, utils.ToBytesList(key0, key1, key2))
	asserts.AssertMultiBulkReplySize(t, result, 0)
	result = SInter(testDB, utils.ToBytesList(key1, key2))
	asserts.AssertMultiBulkReplySize(t, result, 0)
	result = SInterStore(testDB, utils.ToBytesList(utils.RandString(10), key0, key1, key2))
	asserts.AssertIntReply(t, result, 0)
	result = SInterStore(testDB, utils.ToBytesList(utils.RandString(10), key1, key2))
	asserts.AssertIntReply(t, result, 0)
}

func TestSUnion(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	size := 100
	step := 10

	keys := make([]string, 0)
	start := 0
	for i := 0; i < 4; i++ {
		key := utils.RandString(10)
		keys = append(keys, key)
		for j := start; j < size+start; j++ {
			member := strconv.Itoa(j)
			SAdd(testDB, utils.ToBytesList(key, member))
		}
		start += step
	}
	result := SUnion(testDB, utils.ToBytesList(keys...))
	asserts.AssertMultiBulkReplySize(t, result, 130)

	destKey := utils.RandString(10)
	keysWithDest := []string{destKey}
	keysWithDest = append(keysWithDest, keys...)
	result = SUnionStore(testDB, utils.ToBytesList(keysWithDest...))
	asserts.AssertIntReply(t, result, 130)
}

func TestSDiff(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	size := 100
	step := 20

	keys := make([]string, 0)
	start := 0
	for i := 0; i < 3; i++ {
		key := utils.RandString(10)
		keys = append(keys, key)
		for j := start; j < size+start; j++ {
			member := strconv.Itoa(j)
			SAdd(testDB, utils.ToBytesList(key, member))
		}
		start += step
	}
	result := SDiff(testDB, utils.ToBytesList(keys...))
	asserts.AssertMultiBulkReplySize(t, result, step)

	destKey := utils.RandString(10)
	keysWithDest := []string{destKey}
	keysWithDest = append(keysWithDest, keys...)
	result = SDiffStore(testDB, utils.ToBytesList(keysWithDest...))
	asserts.AssertIntReply(t, result, step)

	// test empty set
	FlushAll(testDB, [][]byte{})
	key0 := utils.RandString(10)
	Del(testDB, utils.ToBytesList(key0))
	key1 := utils.RandString(10)
	SAdd(testDB, utils.ToBytesList(key1, "a", "b"))
	key2 := utils.RandString(10)
	SAdd(testDB, utils.ToBytesList(key2, "a", "b"))
	result = SDiff(testDB, utils.ToBytesList(key0, key1, key2))
	asserts.AssertMultiBulkReplySize(t, result, 0)
	result = SDiff(testDB, utils.ToBytesList(key1, key2))
	asserts.AssertMultiBulkReplySize(t, result, 0)
	result = SDiffStore(testDB, utils.ToBytesList(utils.RandString(10), key0, key1, key2))
	asserts.AssertIntReply(t, result, 0)
	result = SDiffStore(testDB, utils.ToBytesList(utils.RandString(10), key1, key2))
	asserts.AssertIntReply(t, result, 0)
}

func TestSRandMember(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	key := utils.RandString(10)
	for j := 0; j < 100; j++ {
		member := strconv.Itoa(j)
		SAdd(testDB, utils.ToBytesList(key, member))
	}
	result := SRandMember(testDB, utils.ToBytesList(key))
	br, ok := result.(*reply.BulkReply)
	if !ok && len(br.Arg) > 0 {
		t.Error(fmt.Sprintf("expected bulk reply, actually %s", result.ToBytes()))
		return
	}

	result = SRandMember(testDB, utils.ToBytesList(key, "10"))
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

	result = SRandMember(testDB, utils.ToBytesList(key, "110"))
	asserts.AssertMultiBulkReplySize(t, result, 100)

	result = SRandMember(testDB, utils.ToBytesList(key, "-10"))
	asserts.AssertMultiBulkReplySize(t, result, 10)

	result = SRandMember(testDB, utils.ToBytesList(key, "-110"))
	asserts.AssertMultiBulkReplySize(t, result, 110)
}
