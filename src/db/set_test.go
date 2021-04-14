package db

import (
	"fmt"
	"github.com/hdt3213/godis/src/redis/reply"
	"github.com/hdt3213/godis/src/redis/reply/asserts"
	"strconv"
	"testing"
)

// basic add get and remove
func TestSAdd(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	size := 100

	// test sadd
	key := RandString(10)
	for i := 0; i < size; i++ {
		member := strconv.Itoa(i)
		result := SAdd(testDB, toArgs(key, member))
		asserts.AssertIntReply(t, result, 1)
	}
	// test scard
	result := SCard(testDB, toArgs(key))
	asserts.AssertIntReply(t, result, size)

	// test is member
	for i := 0; i < size; i++ {
		member := strconv.Itoa(i)
		result := SIsMember(testDB, toArgs(key, member))
		asserts.AssertIntReply(t, result, 1)
	}

	// test members
	result = SMembers(testDB, toArgs(key))
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
	key := RandString(10)
	for i := 0; i < size; i++ {
		member := strconv.Itoa(i)
		SAdd(testDB, toArgs(key, member))
	}
	for i := 0; i < size; i++ {
		member := strconv.Itoa(i)
		SRem(testDB, toArgs(key, member))
		result := SIsMember(testDB, toArgs(key, member))
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
		key := RandString(10)
		keys = append(keys, key)
		for j := start; j < size+start; j++ {
			member := strconv.Itoa(j)
			SAdd(testDB, toArgs(key, member))
		}
		start += step
	}
	result := SInter(testDB, toArgs(keys...))
	asserts.AssertMultiBulkReplySize(t, result, 70)

	destKey := RandString(10)
	keysWithDest := []string{destKey}
	keysWithDest = append(keysWithDest, keys...)
	result = SInterStore(testDB, toArgs(keysWithDest...))
	asserts.AssertIntReply(t, result, 70)
}

func TestSUnion(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	size := 100
	step := 10

	keys := make([]string, 0)
	start := 0
	for i := 0; i < 4; i++ {
		key := RandString(10)
		keys = append(keys, key)
		for j := start; j < size+start; j++ {
			member := strconv.Itoa(j)
			SAdd(testDB, toArgs(key, member))
		}
		start += step
	}
	result := SUnion(testDB, toArgs(keys...))
	asserts.AssertMultiBulkReplySize(t, result, 130)

	destKey := RandString(10)
	keysWithDest := []string{destKey}
	keysWithDest = append(keysWithDest, keys...)
	result = SUnionStore(testDB, toArgs(keysWithDest...))
	asserts.AssertIntReply(t, result, 130)
}

func TestSDiff(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	size := 100
	step := 20

	keys := make([]string, 0)
	start := 0
	for i := 0; i < 3; i++ {
		key := RandString(10)
		keys = append(keys, key)
		for j := start; j < size+start; j++ {
			member := strconv.Itoa(j)
			SAdd(testDB, toArgs(key, member))
		}
		start += step
	}
	result := SDiff(testDB, toArgs(keys...))
	asserts.AssertMultiBulkReplySize(t, result, step)

	destKey := RandString(10)
	keysWithDest := []string{destKey}
	keysWithDest = append(keysWithDest, keys...)
	result = SDiffStore(testDB, toArgs(keysWithDest...))
	asserts.AssertIntReply(t, result, step)
}

func TestSRandMember(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	key := RandString(10)
	for j := 0; j < 100; j++ {
		member := strconv.Itoa(j)
		SAdd(testDB, toArgs(key, member))
	}
	result := SRandMember(testDB, toArgs(key))
	br, ok := result.(*reply.BulkReply)
	if !ok && len(br.Arg) > 0 {
		t.Error(fmt.Sprintf("expected bulk reply, actually %s", result.ToBytes()))
		return
	}

	result = SRandMember(testDB, toArgs(key, "10"))
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

	result = SRandMember(testDB, toArgs(key, "110"))
	asserts.AssertMultiBulkReplySize(t, result, 100)

	result = SRandMember(testDB, toArgs(key, "-10"))
	asserts.AssertMultiBulkReplySize(t, result, 10)

	result = SRandMember(testDB, toArgs(key, "-110"))
	asserts.AssertMultiBulkReplySize(t, result, 110)
}
