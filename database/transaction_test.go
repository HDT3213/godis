package database

import (
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"testing"
)

func TestMulti(t *testing.T) {
	conn := new(connection.FakeConn)
	testServer.Exec(conn, utils.ToCmdLine("FLUSHALL"))
	result := testServer.Exec(conn, utils.ToCmdLine("multi"))
	asserts.AssertNotError(t, result)
	key := utils.RandString(10)
	value := utils.RandString(10)
	testServer.Exec(conn, utils.ToCmdLine("set", key, value))
	key2 := utils.RandString(10)
	testServer.Exec(conn, utils.ToCmdLine("rpush", key2, value))
	result = testServer.Exec(conn, utils.ToCmdLine("exec"))
	asserts.AssertNotError(t, result)
	result = testServer.Exec(conn, utils.ToCmdLine("get", key))
	asserts.AssertBulkReply(t, result, value)
	result = testServer.Exec(conn, utils.ToCmdLine("lrange", key2, "0", "-1"))
	asserts.AssertMultiBulkReply(t, result, []string{value})
	if len(conn.GetWatching()) > 0 {
		t.Error("watching map should be reset")
	}
	if len(conn.GetQueuedCmdLine()) > 0 {
		t.Error("queue should be reset")
	}
}

func TestSyntaxErr(t *testing.T) {
	conn := new(connection.FakeConn)
	testServer.Exec(conn, utils.ToCmdLine("FLUSHALL"))
	result := testServer.Exec(conn, utils.ToCmdLine("multi"))
	asserts.AssertNotError(t, result)
	result = testServer.Exec(conn, utils.ToCmdLine("set"))
	asserts.AssertErrReply(t, result, "ERR wrong number of arguments for 'set' command")
	testServer.Exec(conn, utils.ToCmdLine("get", "a"))
	result = testServer.Exec(conn, utils.ToCmdLine("exec"))
	asserts.AssertErrReply(t, result, "EXECABORT Transaction discarded because of previous errors.")
	result = testServer.Exec(conn, utils.ToCmdLine("get", "a"))
	asserts.AssertNotError(t, result)
}

func TestRollback(t *testing.T) {
	conn := new(connection.FakeConn)
	testServer.Exec(conn, utils.ToCmdLine("FLUSHALL"))
	result := testServer.Exec(conn, utils.ToCmdLine("multi"))
	asserts.AssertNotError(t, result)
	key := utils.RandString(10)
	value := utils.RandString(10)
	testServer.Exec(conn, utils.ToCmdLine("set", key, value))
	testServer.Exec(conn, utils.ToCmdLine("rpush", key, value))
	result = testServer.Exec(conn, utils.ToCmdLine("exec"))
	asserts.AssertErrReply(t, result, "EXECABORT Transaction discarded because of previous errors.")
	result = testServer.Exec(conn, utils.ToCmdLine("type", key))
	asserts.AssertStatusReply(t, result, "none")
	if len(conn.GetWatching()) > 0 {
		t.Error("watching map should be reset")
	}
	if len(conn.GetQueuedCmdLine()) > 0 {
		t.Error("queue should be reset")
	}
}

func TestDiscard(t *testing.T) {
	conn := new(connection.FakeConn)
	testServer.Exec(conn, utils.ToCmdLine("FLUSHALL"))
	result := testServer.Exec(conn, utils.ToCmdLine("multi"))
	asserts.AssertNotError(t, result)
	key := utils.RandString(10)
	value := utils.RandString(10)
	testServer.Exec(conn, utils.ToCmdLine("set", key, value))
	key2 := utils.RandString(10)
	testServer.Exec(conn, utils.ToCmdLine("rpush", key2, value))
	result = testServer.Exec(conn, utils.ToCmdLine("discard"))
	asserts.AssertNotError(t, result)
	result = testServer.Exec(conn, utils.ToCmdLine("get", key))
	asserts.AssertNullBulk(t, result)
	result = testServer.Exec(conn, utils.ToCmdLine("lrange", key2, "0", "-1"))
	asserts.AssertMultiBulkReplySize(t, result, 0)
	if len(conn.GetWatching()) > 0 {
		t.Error("watching map should be reset")
	}
	if len(conn.GetQueuedCmdLine()) > 0 {
		t.Error("queue should be reset")
	}
}

func TestWatch(t *testing.T) {
	conn := new(connection.FakeConn)
	testServer.Exec(conn, utils.ToCmdLine("FLUSHALL"))
	for i := 0; i < 3; i++ {
		key := utils.RandString(10)
		value := utils.RandString(10)
		testServer.Exec(conn, utils.ToCmdLine("watch", key))
		testServer.Exec(conn, utils.ToCmdLine("set", key, value))
		result := testServer.Exec(conn, utils.ToCmdLine("multi"))
		asserts.AssertNotError(t, result)
		key2 := utils.RandString(10)
		value2 := utils.RandString(10)
		testServer.Exec(conn, utils.ToCmdLine("set", key2, value2))
		result = testServer.Exec(conn, utils.ToCmdLine("exec"))
		asserts.AssertNotError(t, result)
		result = testServer.Exec(conn, utils.ToCmdLine("get", key2))
		asserts.AssertNullBulk(t, result)
		if len(conn.GetWatching()) > 0 {
			t.Error("watching map should be reset")
		}
		if len(conn.GetQueuedCmdLine()) > 0 {
			t.Error("queue should be reset")
		}
	}
}
