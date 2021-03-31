package db

import (
	"fmt"
	"github.com/HDT3213/godis/src/redis/reply"
	"github.com/HDT3213/godis/src/redis/reply/asserts"
	"strconv"
	"testing"
	"time"
)

func TestExists(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	key := RandString(10)
	value := RandString(10)
	Set(testDB, toArgs(key, value))
	result := Exists(testDB, toArgs(key))
	asserts.AssertIntReply(t, result, 1)
	key = RandString(10)
	result = Exists(testDB, toArgs(key))
	asserts.AssertIntReply(t, result, 0)
}

func TestType(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	key := RandString(10)
	value := RandString(10)
	Set(testDB, toArgs(key, value))
	result := Type(testDB, toArgs(key))
	asserts.AssertStatusReply(t, result, "string")

	Del(testDB, toArgs(key))
	result = Type(testDB, toArgs(key))
	asserts.AssertStatusReply(t, result, "none")
	RPush(testDB, toArgs(key, value))
	result = Type(testDB, toArgs(key))
	asserts.AssertStatusReply(t, result, "list")

	Del(testDB, toArgs(key))
	HSet(testDB, toArgs(key, key, value))
	result = Type(testDB, toArgs(key))
	asserts.AssertStatusReply(t, result, "hash")

	Del(testDB, toArgs(key))
	SAdd(testDB, toArgs(key, value))
	result = Type(testDB, toArgs(key))
	asserts.AssertStatusReply(t, result, "set")

	Del(testDB, toArgs(key))
	ZAdd(testDB, toArgs(key, "1", value))
	result = Type(testDB, toArgs(key))
	asserts.AssertStatusReply(t, result, "zset")
}

func TestRename(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	key := RandString(10)
	value := RandString(10)
	newKey := key + RandString(2)
	Set(testDB, toArgs(key, value, "ex", "1000"))
	result := Rename(testDB, toArgs(key, newKey))
	if _, ok := result.(*reply.OkReply); !ok {
		t.Error("expect ok")
		return
	}
	result = Exists(testDB, toArgs(key))
	asserts.AssertIntReply(t, result, 0)
	result = Exists(testDB, toArgs(newKey))
	asserts.AssertIntReply(t, result, 1)
	// check ttl
	result = TTL(testDB, toArgs(newKey))
	intResult, ok := result.(*reply.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int reply, actually %s", result.ToBytes()))
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}
}

func TestRenameNx(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	key := RandString(10)
	value := RandString(10)
	newKey := key + RandString(2)
	Set(testDB, toArgs(key, value, "ex", "1000"))
	result := RenameNx(testDB, toArgs(key, newKey))
	if _, ok := result.(*reply.OkReply); !ok {
		t.Error("expect ok")
		return
	}
	result = Exists(testDB, toArgs(key))
	asserts.AssertIntReply(t, result, 0)
	result = Exists(testDB, toArgs(newKey))
	asserts.AssertIntReply(t, result, 1)
	result = TTL(testDB, toArgs(newKey))
	intResult, ok := result.(*reply.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int reply, actually %s", result.ToBytes()))
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}
}

func TestTTL(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	key := RandString(10)
	value := RandString(10)
	Set(testDB, toArgs(key, value))

	result := Expire(testDB, toArgs(key, "1000"))
	asserts.AssertIntReply(t, result, 1)
	result = TTL(testDB, toArgs(key))
	intResult, ok := result.(*reply.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int reply, actually %s", result.ToBytes()))
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}

	result = Persist(testDB, toArgs(key))
	asserts.AssertIntReply(t, result, 1)
	result = TTL(testDB, toArgs(key))
	asserts.AssertIntReply(t, result, -1)

	result = PExpire(testDB, toArgs(key, "1000000"))
	asserts.AssertIntReply(t, result, 1)
	result = PTTL(testDB, toArgs(key))
	intResult, ok = result.(*reply.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int reply, actually %s", result.ToBytes()))
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}
}

func TestExpireAt(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	key := RandString(10)
	value := RandString(10)
	Set(testDB, toArgs(key, value))

	expireAt := time.Now().Add(time.Minute).Unix()
	result := ExpireAt(testDB, toArgs(key, strconv.FormatInt(expireAt, 10)))
	asserts.AssertIntReply(t, result, 1)
	result = TTL(testDB, toArgs(key))
	intResult, ok := result.(*reply.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int reply, actually %s", result.ToBytes()))
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}

	expireAt = time.Now().Add(time.Minute).Unix()
	result = PExpireAt(testDB, toArgs(key, strconv.FormatInt(expireAt*1000, 10)))
	asserts.AssertIntReply(t, result, 1)
	result = TTL(testDB, toArgs(key))
	intResult, ok = result.(*reply.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int reply, actually %s", result.ToBytes()))
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}
}

func TestKeys(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	key := RandString(10)
	value := RandString(10)
	Set(testDB, toArgs(key, value))
	Set(testDB, toArgs("a:"+key, value))
	Set(testDB, toArgs("b:"+key, value))

	result := Keys(testDB, toArgs("*"))
	asserts.AssertMultiBulkReplySize(t, result, 3)
	result = Keys(testDB, toArgs("a:*"))
	asserts.AssertMultiBulkReplySize(t, result, 1)
	result = Keys(testDB, toArgs("?:*"))
	asserts.AssertMultiBulkReplySize(t, result, 2)
}
