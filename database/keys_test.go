package database

import (
	"fmt"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"strconv"
	"testing"
	"time"
)

func TestExists(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value))
	result := testDB.Exec(nil, utils.ToCmdLine("exists", key))
	asserts.AssertIntReply(t, result, 1)
	key = utils.RandString(10)
	result = testDB.Exec(nil, utils.ToCmdLine("exists", key))
	asserts.AssertIntReply(t, result, 0)
}

func TestType(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value))
	result := testDB.Exec(nil, utils.ToCmdLine("type", key))
	asserts.AssertStatusReply(t, result, "string")

	testDB.Remove(key)
	result = testDB.Exec(nil, utils.ToCmdLine("type", key))
	asserts.AssertStatusReply(t, result, "none")
	execRPush(testDB, utils.ToCmdLine(key, value))
	result = testDB.Exec(nil, utils.ToCmdLine("type", key))
	asserts.AssertStatusReply(t, result, "list")

	testDB.Remove(key)
	testDB.Exec(nil, utils.ToCmdLine("hset", key, key, value))
	result = testDB.Exec(nil, utils.ToCmdLine("type", key))
	asserts.AssertStatusReply(t, result, "hash")

	testDB.Remove(key)
	testDB.Exec(nil, utils.ToCmdLine("sadd", key, value))
	result = testDB.Exec(nil, utils.ToCmdLine("type", key))
	asserts.AssertStatusReply(t, result, "set")

	testDB.Remove(key)
	testDB.Exec(nil, utils.ToCmdLine("zadd", key, "1", value))
	result = testDB.Exec(nil, utils.ToCmdLine("type", key))
	asserts.AssertStatusReply(t, result, "zset")
}

func TestRename(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	newKey := key + utils.RandString(2)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value, "ex", "1000"))
	result := testDB.Exec(nil, utils.ToCmdLine("rename", key, newKey))
	if _, ok := result.(*protocol.OkReply); !ok {
		t.Error("expect ok")
		return
	}
	result = testDB.Exec(nil, utils.ToCmdLine("exists", key))
	asserts.AssertIntReply(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("exists", newKey))
	asserts.AssertIntReply(t, result, 1)
	// check ttl
	result = testDB.Exec(nil, utils.ToCmdLine("ttl", newKey))
	intResult, ok := result.(*protocol.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int protocol, actually %s", result.ToBytes()))
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}
}

func TestRenameNx(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	newKey := key + utils.RandString(2)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value, "ex", "1000"))
	result := testDB.Exec(nil, utils.ToCmdLine("RenameNx", key, newKey))
	asserts.AssertIntReply(t, result, 1)
	result = testDB.Exec(nil, utils.ToCmdLine("exists", key))
	asserts.AssertIntReply(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("exists", newKey))
	asserts.AssertIntReply(t, result, 1)
	result = testDB.Exec(nil, utils.ToCmdLine("ttl", newKey))
	intResult, ok := result.(*protocol.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int protocol, actually %s", result.ToBytes()))
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}
}

func TestTTL(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value))

	result := testDB.Exec(nil, utils.ToCmdLine("expire", key, "1000"))
	asserts.AssertIntReply(t, result, 1)
	result = testDB.Exec(nil, utils.ToCmdLine("ttl", key))
	intResult, ok := result.(*protocol.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int protocol, actually %s", result.ToBytes()))
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}

	result = testDB.Exec(nil, utils.ToCmdLine("persist", key))
	asserts.AssertIntReply(t, result, 1)
	result = testDB.Exec(nil, utils.ToCmdLine("ttl", key))
	asserts.AssertIntReply(t, result, -1)

	result = testDB.Exec(nil, utils.ToCmdLine("PExpire", key, "1000000"))
	asserts.AssertIntReply(t, result, 1)
	result = testDB.Exec(nil, utils.ToCmdLine("PTTL", key))
	intResult, ok = result.(*protocol.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int protocol, actually %s", result.ToBytes()))
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}
}

func TestExpire(t *testing.T) {
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("SET", key, value))
	testDB.Exec(nil, utils.ToCmdLine("PEXPIRE", key, "100"))
	time.Sleep(2 * time.Second)
	result := testDB.Exec(nil, utils.ToCmdLine("TTL", key))
	asserts.AssertIntReply(t, result, -2)

}

func TestExpireAt(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value))

	expireAt := time.Now().Add(time.Minute).Unix()
	result := testDB.Exec(nil, utils.ToCmdLine("ExpireAt", key, strconv.FormatInt(expireAt, 10)))

	asserts.AssertIntReply(t, result, 1)
	result = testDB.Exec(nil, utils.ToCmdLine("ttl", key))
	intResult, ok := result.(*protocol.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int protocol, actually %s", result.ToBytes()))
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}

	expireAt = time.Now().Add(time.Minute).Unix()
	result = testDB.Exec(nil, utils.ToCmdLine("PExpireAt", key, strconv.FormatInt(expireAt*1000, 10)))
	asserts.AssertIntReply(t, result, 1)
	result = testDB.Exec(nil, utils.ToCmdLine("ttl", key))
	intResult, ok = result.(*protocol.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int protocol, actually %s", result.ToBytes()))
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}
}

func TestKeys(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value))
	testDB.Exec(nil, utils.ToCmdLine("set", "a:"+key, value))
	testDB.Exec(nil, utils.ToCmdLine("set", "b:"+key, value))

	result := testDB.Exec(nil, utils.ToCmdLine("keys", "*"))
	asserts.AssertMultiBulkReplySize(t, result, 3)
	result = testDB.Exec(nil, utils.ToCmdLine("keys", "a:*"))
	asserts.AssertMultiBulkReplySize(t, result, 1)
	result = testDB.Exec(nil, utils.ToCmdLine("keys", "?:*"))
	asserts.AssertMultiBulkReplySize(t, result, 2)
}
