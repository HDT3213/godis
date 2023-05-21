package cluster

import (
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"testing"
)

func TestRename(t *testing.T) {
	testNodeA := testCluster[0]
	conn := new(connection.FakeConn)

	// cross node rename
	for i := 0; i < 10; i++ {
		testNodeA.Exec(conn, utils.ToCmdLine("FlushALL"))
		key := utils.RandString(10)
		value := utils.RandString(10)
		newKey := utils.RandString(10)
		testNodeA.Exec(conn, utils.ToCmdLine("SET", key, value, "ex", "100000"))
		result := testNodeA.Exec(conn, utils.ToCmdLine("RENAME", key, newKey))
		asserts.AssertStatusReply(t, result, "OK")
		result = testNodeA.Exec(conn, utils.ToCmdLine("EXISTS", key))
		asserts.AssertIntReply(t, result, 0)
		result = testNodeA.Exec(conn, utils.ToCmdLine("EXISTS", newKey))
		asserts.AssertIntReply(t, result, 1)
		result = testNodeA.Exec(conn, utils.ToCmdLine("TTL", newKey))
		asserts.AssertIntReplyGreaterThan(t, result, 0)
	}
}

func TestRenameNx(t *testing.T) {
	testNodeA := testCluster[0]
	conn := new(connection.FakeConn)

	// cross node rename
	for i := 0; i < 10; i++ {
		testNodeA.Exec(conn, utils.ToCmdLine("FlushALL"))
		key := utils.RandString(10)
		value := utils.RandString(10)
		newKey := utils.RandString(10)
		testNodeA.Exec(conn, utils.ToCmdLine("SET", key, value, "ex", "100000"))
		result := testNodeA.Exec(conn, utils.ToCmdLine("RENAMENX", key, newKey))
		asserts.AssertIntReply(t, result, 1)
		result = testNodeA.Exec(conn, utils.ToCmdLine("EXISTS", key))
		asserts.AssertIntReply(t, result, 0)
		result = testNodeA.Exec(conn, utils.ToCmdLine("EXISTS", newKey))
		asserts.AssertIntReply(t, result, 1)
		result = testNodeA.Exec(conn, utils.ToCmdLine("TTL", newKey))
		asserts.AssertIntReplyGreaterThan(t, result, 0)

		value2 := value + "ccc"
		testNodeA.Exec(conn, utils.ToCmdLine("SET", key, value2, "ex", "100000"))
		result = testNodeA.Exec(conn, utils.ToCmdLine("RENAMENX", key, newKey))
		asserts.AssertIntReply(t, result, 0)
		result = testNodeA.Exec(conn, utils.ToCmdLine("EXISTS", key))
		asserts.AssertIntReply(t, result, 1)
	}
}
