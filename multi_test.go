package godis

import (
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/reply/asserts"
	"testing"
)

func TestMulti(t *testing.T) {
	testDB.Flush()
	conn := new(connection.FakeConn)
	result := testDB.Exec(conn, utils.ToCmdLine("multi"))
	asserts.AssertNotError(t, result)
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(conn, utils.ToCmdLine("set", key, value))
	key2 := utils.RandString(10)
	testDB.Exec(conn, utils.ToCmdLine("rpush", key2, value))
	result = testDB.Exec(conn, utils.ToCmdLine("exec"))
	asserts.AssertNotError(t, result)
	result = testDB.Exec(conn, utils.ToCmdLine("get", key))
	asserts.AssertBulkReply(t, result, value)
	result = testDB.Exec(conn, utils.ToCmdLine("lrange", key2, "0", "-1"))
	asserts.AssertMultiBulkReply(t, result, []string{value})
	if len(conn.GetWatching()) > 0 {
		t.Error("watching map should be reset")
	}
	if len(conn.GetQueuedCmdLine()) > 0 {
		t.Error("queue should be reset")
	}
}

func TestRollback(t *testing.T) {
	testDB.Flush()
	conn := new(connection.FakeConn)
	result := testDB.Exec(conn, utils.ToCmdLine("multi"))
	asserts.AssertNotError(t, result)
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(conn, utils.ToCmdLine("set", key, value))
	testDB.Exec(conn, utils.ToCmdLine("rpush", key, value))
	result = testDB.Exec(conn, utils.ToCmdLine("exec"))
	asserts.AssertErrReply(t, result, "EXECABORT Transaction discarded because of previous errors.")
	result = testDB.Exec(conn, utils.ToCmdLine("type", key))
	asserts.AssertStatusReply(t, result, "none")
	if len(conn.GetWatching()) > 0 {
		t.Error("watching map should be reset")
	}
	if len(conn.GetQueuedCmdLine()) > 0 {
		t.Error("queue should be reset")
	}
}

func TestDiscard(t *testing.T) {
	testDB.Flush()
	conn := new(connection.FakeConn)
	result := testDB.Exec(conn, utils.ToCmdLine("multi"))
	asserts.AssertNotError(t, result)
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(conn, utils.ToCmdLine("set", key, value))
	key2 := utils.RandString(10)
	testDB.Exec(conn, utils.ToCmdLine("rpush", key2, value))
	result = testDB.Exec(conn, utils.ToCmdLine("discard"))
	asserts.AssertNotError(t, result)
	result = testDB.Exec(conn, utils.ToCmdLine("get", key))
	asserts.AssertNullBulk(t, result)
	result = testDB.Exec(conn, utils.ToCmdLine("lrange", key2, "0", "-1"))
	asserts.AssertMultiBulkReplySize(t, result, 0)
	if len(conn.GetWatching()) > 0 {
		t.Error("watching map should be reset")
	}
	if len(conn.GetQueuedCmdLine()) > 0 {
		t.Error("queue should be reset")
	}
}

func TestWatch(t *testing.T) {
	testDB.Flush()
	conn := new(connection.FakeConn)
	for i := 0; i < 3; i++ {
		key := utils.RandString(10)
		value := utils.RandString(10)
		testDB.Exec(conn, utils.ToCmdLine("watch", key))
		testDB.Exec(conn, utils.ToCmdLine("set", key, value))
		result := testDB.Exec(conn, utils.ToCmdLine("multi"))
		asserts.AssertNotError(t, result)
		key2 := utils.RandString(10)
		value2 := utils.RandString(10)
		testDB.Exec(conn, utils.ToCmdLine("set", key2, value2))
		result = testDB.Exec(conn, utils.ToCmdLine("exec"))
		asserts.AssertNotError(t, result)
		result = testDB.Exec(conn, utils.ToCmdLine("get", key2))
		asserts.AssertNullBulk(t, result)
		if len(conn.GetWatching()) > 0 {
			t.Error("watching map should be reset")
		}
		if len(conn.GetQueuedCmdLine()) > 0 {
			t.Error("queue should be reset")
		}
	}
}
