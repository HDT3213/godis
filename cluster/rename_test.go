package cluster

import (
	"fmt"
	"github.com/hdt3213/godis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/reply"
	"github.com/hdt3213/godis/redis/reply/asserts"
	"testing"
)

func TestRename(t *testing.T) {
	testDB := testCluster.db
	godis.FlushAll(testDB, [][]byte{})
	key := utils.RandString(10)
	value := utils.RandString(10)
	newKey := key + utils.RandString(2)
	godis.Set(testDB, utils.ToBytesList(key, value, "ex", "1000"))
	result := Rename(testCluster, nil, utils.ToBytesList("RENAME", key, newKey))
	if _, ok := result.(*reply.OkReply); !ok {
		t.Error("expect ok")
		return
	}
	result = godis.Exists(testDB, utils.ToBytesList(key))
	asserts.AssertIntReply(t, result, 0)
	result = godis.Exists(testDB, utils.ToBytesList(newKey))
	asserts.AssertIntReply(t, result, 1)
	// check ttl
	result = godis.TTL(testDB, utils.ToBytesList(newKey))
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
	testDB := testCluster.db
	godis.FlushAll(testDB, [][]byte{})
	key := utils.RandString(10)
	value := utils.RandString(10)
	newKey := key + utils.RandString(2)
	godis.Set(testCluster.db, utils.ToBytesList(key, value, "ex", "1000"))
	result := RenameNx(testCluster, nil, utils.ToBytesList("RENAMENX", key, newKey))
	asserts.AssertIntReply(t, result, 1)
	result = godis.Exists(testDB, utils.ToBytesList(key))
	asserts.AssertIntReply(t, result, 0)
	result = godis.Exists(testDB, utils.ToBytesList(newKey))
	asserts.AssertIntReply(t, result, 1)
	result = godis.TTL(testDB, utils.ToBytesList(newKey))
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
