package db

import (
	"fmt"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/reply"
	"github.com/hdt3213/godis/redis/reply/asserts"
	"strconv"
	"testing"
	"time"
)

func TestExists(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	key := utils.RandString(10)
	value := utils.RandString(10)
	Set(testDB, utils.ToBytesList(key, value))
	result := Exists(testDB, utils.ToBytesList(key))
	asserts.AssertIntReply(t, result, 1)
	key = utils.RandString(10)
	result = Exists(testDB, utils.ToBytesList(key))
	asserts.AssertIntReply(t, result, 0)
}

func TestType(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	key := utils.RandString(10)
	value := utils.RandString(10)
	Set(testDB, utils.ToBytesList(key, value))
	result := Type(testDB, utils.ToBytesList(key))
	asserts.AssertStatusReply(t, result, "string")

	Del(testDB, utils.ToBytesList(key))
	result = Type(testDB, utils.ToBytesList(key))
	asserts.AssertStatusReply(t, result, "none")
	RPush(testDB, utils.ToBytesList(key, value))
	result = Type(testDB, utils.ToBytesList(key))
	asserts.AssertStatusReply(t, result, "list")

	Del(testDB, utils.ToBytesList(key))
	HSet(testDB, utils.ToBytesList(key, key, value))
	result = Type(testDB, utils.ToBytesList(key))
	asserts.AssertStatusReply(t, result, "hash")

	Del(testDB, utils.ToBytesList(key))
	SAdd(testDB, utils.ToBytesList(key, value))
	result = Type(testDB, utils.ToBytesList(key))
	asserts.AssertStatusReply(t, result, "set")

	Del(testDB, utils.ToBytesList(key))
	ZAdd(testDB, utils.ToBytesList(key, "1", value))
	result = Type(testDB, utils.ToBytesList(key))
	asserts.AssertStatusReply(t, result, "zset")
}

func TestRename(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	key := utils.RandString(10)
	value := utils.RandString(10)
	newKey := key + utils.RandString(2)
	Set(testDB, utils.ToBytesList(key, value, "ex", "1000"))
	result := Rename(testDB, utils.ToBytesList(key, newKey))
	if _, ok := result.(*reply.OkReply); !ok {
		t.Error("expect ok")
		return
	}
	result = Exists(testDB, utils.ToBytesList(key))
	asserts.AssertIntReply(t, result, 0)
	result = Exists(testDB, utils.ToBytesList(newKey))
	asserts.AssertIntReply(t, result, 1)
	// check ttl
	result = TTL(testDB, utils.ToBytesList(newKey))
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
	key := utils.RandString(10)
	value := utils.RandString(10)
	newKey := key + utils.RandString(2)
	Set(testDB, utils.ToBytesList(key, value, "ex", "1000"))
	result := RenameNx(testDB, utils.ToBytesList(key, newKey))
	asserts.AssertIntReply(t, result, 1)
	result = Exists(testDB, utils.ToBytesList(key))
	asserts.AssertIntReply(t, result, 0)
	result = Exists(testDB, utils.ToBytesList(newKey))
	asserts.AssertIntReply(t, result, 1)
	result = TTL(testDB, utils.ToBytesList(newKey))
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
	key := utils.RandString(10)
	value := utils.RandString(10)
	Set(testDB, utils.ToBytesList(key, value))

	result := Expire(testDB, utils.ToBytesList(key, "1000"))
	asserts.AssertIntReply(t, result, 1)
	result = TTL(testDB, utils.ToBytesList(key))
	intResult, ok := result.(*reply.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int reply, actually %s", result.ToBytes()))
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}

	result = Persist(testDB, utils.ToBytesList(key))
	asserts.AssertIntReply(t, result, 1)
	result = TTL(testDB, utils.ToBytesList(key))
	asserts.AssertIntReply(t, result, -1)

	result = PExpire(testDB, utils.ToBytesList(key, "1000000"))
	asserts.AssertIntReply(t, result, 1)
	result = PTTL(testDB, utils.ToBytesList(key))
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
	key := utils.RandString(10)
	value := utils.RandString(10)
	Set(testDB, utils.ToBytesList(key, value))

	expireAt := time.Now().Add(time.Minute).Unix()
	result := ExpireAt(testDB, utils.ToBytesList(key, strconv.FormatInt(expireAt, 10)))
	asserts.AssertIntReply(t, result, 1)
	result = TTL(testDB, utils.ToBytesList(key))
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
	result = PExpireAt(testDB, utils.ToBytesList(key, strconv.FormatInt(expireAt*1000, 10)))
	asserts.AssertIntReply(t, result, 1)
	result = TTL(testDB, utils.ToBytesList(key))
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
	key := utils.RandString(10)
	value := utils.RandString(10)
	Set(testDB, utils.ToBytesList(key, value))
	Set(testDB, utils.ToBytesList("a:"+key, value))
	Set(testDB, utils.ToBytesList("b:"+key, value))

	result := Keys(testDB, utils.ToBytesList("*"))
	asserts.AssertMultiBulkReplySize(t, result, 3)
	result = Keys(testDB, utils.ToBytesList("a:*"))
	asserts.AssertMultiBulkReplySize(t, result, 1)
	result = Keys(testDB, utils.ToBytesList("?:*"))
	asserts.AssertMultiBulkReplySize(t, result, 2)
}
