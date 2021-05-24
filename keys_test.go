package godis

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
	execFlushAll(testDB, [][]byte{})
	key := utils.RandString(10)
	value := utils.RandString(10)
	execSet(testDB, utils.ToBytesList(key, value))
	result := execExists(testDB, utils.ToBytesList(key))
	asserts.AssertIntReply(t, result, 1)
	key = utils.RandString(10)
	result = execExists(testDB, utils.ToBytesList(key))
	asserts.AssertIntReply(t, result, 0)
}

func TestType(t *testing.T) {
	execFlushAll(testDB, [][]byte{})
	key := utils.RandString(10)
	value := utils.RandString(10)
	execSet(testDB, utils.ToBytesList(key, value))
	result := execType(testDB, utils.ToBytesList(key))
	asserts.AssertStatusReply(t, result, "string")

	testDB.Remove(key)
	result = execType(testDB, utils.ToBytesList(key))
	asserts.AssertStatusReply(t, result, "none")
	execRPush(testDB, utils.ToBytesList(key, value))
	result = execType(testDB, utils.ToBytesList(key))
	asserts.AssertStatusReply(t, result, "list")

	testDB.Remove(key)
	execHSet(testDB, utils.ToBytesList(key, key, value))
	result = execType(testDB, utils.ToBytesList(key))
	asserts.AssertStatusReply(t, result, "hash")

	testDB.Remove(key)
	execSAdd(testDB, utils.ToBytesList(key, value))
	result = execType(testDB, utils.ToBytesList(key))
	asserts.AssertStatusReply(t, result, "set")

	testDB.Remove(key)
	execZAdd(testDB, utils.ToBytesList(key, "1", value))
	result = execType(testDB, utils.ToBytesList(key))
	asserts.AssertStatusReply(t, result, "zset")
}

func TestRename(t *testing.T) {
	execFlushAll(testDB, [][]byte{})
	key := utils.RandString(10)
	value := utils.RandString(10)
	newKey := key + utils.RandString(2)
	execSet(testDB, utils.ToBytesList(key, value, "ex", "1000"))
	result := execRename(testDB, utils.ToBytesList(key, newKey))
	if _, ok := result.(*reply.OkReply); !ok {
		t.Error("expect ok")
		return
	}
	result = execExists(testDB, utils.ToBytesList(key))
	asserts.AssertIntReply(t, result, 0)
	result = execExists(testDB, utils.ToBytesList(newKey))
	asserts.AssertIntReply(t, result, 1)
	// check ttl
	result = execTTL(testDB, utils.ToBytesList(newKey))
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
	execFlushAll(testDB, [][]byte{})
	key := utils.RandString(10)
	value := utils.RandString(10)
	newKey := key + utils.RandString(2)
	execSet(testDB, utils.ToBytesList(key, value, "ex", "1000"))
	result := execRenameNx(testDB, utils.ToBytesList(key, newKey))
	asserts.AssertIntReply(t, result, 1)
	result = execExists(testDB, utils.ToBytesList(key))
	asserts.AssertIntReply(t, result, 0)
	result = execExists(testDB, utils.ToBytesList(newKey))
	asserts.AssertIntReply(t, result, 1)
	result = execTTL(testDB, utils.ToBytesList(newKey))
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
	execFlushAll(testDB, [][]byte{})
	key := utils.RandString(10)
	value := utils.RandString(10)
	execSet(testDB, utils.ToBytesList(key, value))

	result := execExpire(testDB, utils.ToBytesList(key, "1000"))
	asserts.AssertIntReply(t, result, 1)
	result = execTTL(testDB, utils.ToBytesList(key))
	intResult, ok := result.(*reply.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int reply, actually %s", result.ToBytes()))
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}

	result = execPersist(testDB, utils.ToBytesList(key))
	asserts.AssertIntReply(t, result, 1)
	result = execTTL(testDB, utils.ToBytesList(key))
	asserts.AssertIntReply(t, result, -1)

	result = execPExpire(testDB, utils.ToBytesList(key, "1000000"))
	asserts.AssertIntReply(t, result, 1)
	result = execPTTL(testDB, utils.ToBytesList(key))
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
	execFlushAll(testDB, [][]byte{})
	key := utils.RandString(10)
	value := utils.RandString(10)
	execSet(testDB, utils.ToBytesList(key, value))

	expireAt := time.Now().Add(time.Minute).Unix()
	result := execExpireAt(testDB, utils.ToBytesList(key, strconv.FormatInt(expireAt, 10)))
	asserts.AssertIntReply(t, result, 1)
	result = execTTL(testDB, utils.ToBytesList(key))
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
	result = execPExpireAt(testDB, utils.ToBytesList(key, strconv.FormatInt(expireAt*1000, 10)))
	asserts.AssertIntReply(t, result, 1)
	result = execTTL(testDB, utils.ToBytesList(key))
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
	execFlushAll(testDB, [][]byte{})
	key := utils.RandString(10)
	value := utils.RandString(10)
	execSet(testDB, utils.ToBytesList(key, value))
	execSet(testDB, utils.ToBytesList("a:"+key, value))
	execSet(testDB, utils.ToBytesList("b:"+key, value))

	result := execKeys(testDB, utils.ToBytesList("*"))
	asserts.AssertMultiBulkReplySize(t, result, 3)
	result = execKeys(testDB, utils.ToBytesList("a:*"))
	asserts.AssertMultiBulkReplySize(t, result, 1)
	result = execKeys(testDB, utils.ToBytesList("?:*"))
	asserts.AssertMultiBulkReplySize(t, result, 2)
}
