package database

import (
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"testing"
	"time"
)

func TestRollbackGivenKeys(t *testing.T) {
	testDB.Flush()

	// rollback to string
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("SET", key, value, "EX", "200"))
	undoCmdLines := rollbackGivenKeys(testDB, key)
	rawExpire, _ := testDB.ttlMap.Get(key)
	expireTime, _ := rawExpire.(time.Time)
	// override given key
	value2 := value + utils.RandString(5)
	testDB.Exec(nil, utils.ToCmdLine("SET", key, value2, "EX", "1000"))
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
	asserts.AssertBulkReply(t, actual, value)
	rawExpire2, _ := testDB.ttlMap.Get(key)
	expireTime2, _ := rawExpire2.(time.Time)
	timeDiff := expireTime.Sub(expireTime2)
	if timeDiff < -time.Millisecond || timeDiff > time.Millisecond {
		t.Error("rollback ttl failed")
	}
}

func TestRollbackToList(t *testing.T) {
	key := utils.RandString(10)
	value := utils.RandString(10)
	value2 := utils.RandString(10)
	testDB.Remove(key)
	testDB.Exec(nil, utils.ToCmdLine("RPUSH", key, value, value2))
	undoCmdLines := rollbackGivenKeys(testDB, key)
	testDB.Exec(nil, utils.ToCmdLine("LREM", key, value2))
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	actual := testDB.Exec(nil, utils.ToCmdLine("LRANGE", key, "0", "-1"))
	asserts.AssertMultiBulkReply(t, actual, []string{value, value2})
}

func TestRollbackToSet(t *testing.T) {
	key := utils.RandString(10)
	value := utils.RandString(10)
	value2 := utils.RandString(10)
	testDB.Remove(key)
	cmdLine := utils.ToCmdLine("SADD", key, value)
	testDB.Exec(nil, cmdLine)
	undoCmdLines := rollbackFirstKey(testDB, cmdLine[1:])
	testDB.Exec(nil, utils.ToCmdLine("SADD", key, value2))
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	actual := testDB.Exec(nil, utils.ToCmdLine("SMembers", key))
	asserts.AssertMultiBulkReply(t, actual, []string{value})
}

func TestRollbackSetMembers(t *testing.T) {
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Remove(key)

	// undo srem
	cmdLine := utils.ToCmdLine("SADD", key, value)
	testDB.Exec(nil, cmdLine)
	undoCmdLines := undoSetChange(testDB, cmdLine[1:])
	testDB.Exec(nil, utils.ToCmdLine("SREM", key, value))
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	actual := testDB.Exec(nil, utils.ToCmdLine("SIsMember", key, value))
	asserts.AssertIntReply(t, actual, 1)

	// undo sadd
	testDB.Remove(key)
	value2 := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("SADD", key, value2))
	cmdLine = utils.ToCmdLine("SADD", key, value)
	undoCmdLines = undoSetChange(testDB, cmdLine[1:])
	testDB.Exec(nil, cmdLine)
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	actual = testDB.Exec(nil, utils.ToCmdLine("SIsMember", key, value))
	asserts.AssertIntReply(t, actual, 0)

	// undo sadd, only member
	testDB.Remove(key)
	undoCmdLines = rollbackSetMembers(testDB, key, value)
	testDB.Exec(nil, utils.ToCmdLine("SAdd", key, value))
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	actual = testDB.Exec(nil, utils.ToCmdLine("type", key))
	asserts.AssertStatusReply(t, actual, "none")
}

func TestRollbackToHash(t *testing.T) {
	key := utils.RandString(10)
	value := utils.RandString(10)
	value2 := utils.RandString(10)
	testDB.Remove(key)
	testDB.Exec(nil, utils.ToCmdLine("HSet", key, value, value))
	undoCmdLines := rollbackGivenKeys(testDB, key)
	testDB.Exec(nil, utils.ToCmdLine("HSet", key, value, value2))
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	actual := testDB.Exec(nil, utils.ToCmdLine("HGET", key, value))
	asserts.AssertBulkReply(t, actual, value)
}

func TestRollbackHashFields(t *testing.T) {
	key := utils.RandString(10)
	value := utils.RandString(10)
	value2 := utils.RandString(10)
	testDB.Remove(key)
	testDB.Exec(nil, utils.ToCmdLine("HSet", key, value, value))
	undoCmdLines := rollbackHashFields(testDB, key, value, value2)
	testDB.Exec(nil, utils.ToCmdLine("HSet", key, value, value2, value2, value2))
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	actual := testDB.Exec(nil, utils.ToCmdLine("HGET", key, value))
	asserts.AssertBulkReply(t, actual, value)
	actual = testDB.Exec(nil, utils.ToCmdLine("HGET", key, value2))
	asserts.AssertNullBulk(t, actual)

	testDB.Remove(key)
	undoCmdLines = rollbackHashFields(testDB, key, value)
	testDB.Exec(nil, utils.ToCmdLine("HSet", key, value, value))
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	actual = testDB.Exec(nil, utils.ToCmdLine("type", key))
	asserts.AssertStatusReply(t, actual, "none")
}

func TestRollbackToZSet(t *testing.T) {
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Remove(key)
	testDB.Exec(nil, utils.ToCmdLine("ZADD", key, "1", value))
	undoCmdLines := rollbackGivenKeys(testDB, key)
	testDB.Exec(nil, utils.ToCmdLine("ZADD", key, "2", value))
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	actual := testDB.Exec(nil, utils.ToCmdLine("ZSCORE", key, value))
	asserts.AssertBulkReply(t, actual, "1")
}

func TestRollbackZSetFields(t *testing.T) {
	key := utils.RandString(10)
	value := utils.RandString(10)
	value2 := utils.RandString(10)
	testDB.Remove(key)
	testDB.Exec(nil, utils.ToCmdLine("ZADD", key, "1", value))
	undoCmdLines := rollbackZSetFields(testDB, key, value, value2)
	testDB.Exec(nil, utils.ToCmdLine("ZADD", key, "2", value, "3", value2))
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	actual := testDB.Exec(nil, utils.ToCmdLine("ZSCORE", key, value))
	asserts.AssertBulkReply(t, actual, "1")
	actual = testDB.Exec(nil, utils.ToCmdLine("ZSCORE", key, value2))
	asserts.AssertNullBulk(t, actual)

	testDB.Remove(key)
	undoCmdLines = rollbackZSetFields(testDB, key, value)
	testDB.Exec(nil, utils.ToCmdLine("ZADD", key, "1", value))
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	actual = testDB.Exec(nil, utils.ToCmdLine("type", key))
	asserts.AssertStatusReply(t, actual, "none")
}
