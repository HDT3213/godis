package cluster

import (
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"testing"
)

func TestRename(t *testing.T) {
	conn := new(connection.FakeConn)
	testNodeA.db.Exec(conn, utils.ToCmdLine("FlushALL"))

	// cross node rename
	key := testNodeA.self + utils.RandString(10)
	value := utils.RandString(10)
	newKey := testNodeB.self + utils.RandString(10) // route to testNodeB, see mockPicker.PickNode
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", key, value, "ex", "1000"))
	result := Rename(testNodeA, conn, utils.ToCmdLine("RENAME", key, newKey))
	asserts.AssertStatusReply(t, result, "OK")
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("EXISTS", key))
	asserts.AssertIntReply(t, result, 0)
	result = testNodeB.db.Exec(conn, utils.ToCmdLine("EXISTS", newKey))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeB.db.Exec(conn, utils.ToCmdLine("TTL", newKey))
	asserts.AssertIntReplyGreaterThan(t, result, 0)

	// same node rename
	key = testNodeA.self + utils.RandString(10)
	value = utils.RandString(10)
	newKey = key + utils.RandString(2)
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", key, value, "ex", "1000"))
	result = Rename(testNodeA, conn, utils.ToCmdLine("RENAME", key, newKey))
	asserts.AssertStatusReply(t, result, "OK")
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("EXISTS", key))
	asserts.AssertIntReply(t, result, 0)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("EXISTS", newKey))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("TTL", newKey))
	asserts.AssertIntReplyGreaterThan(t, result, 0)

	// test src prepare failed
	*simulateATimout = true
	key = testNodeA.self + utils.RandString(10)
	newKey = testNodeB.self + utils.RandString(10) // route to testNodeB, see mockPicker.PickNode
	value = utils.RandString(10)
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", key, value, "ex", "1000"))
	result = Rename(testNodeB, conn, utils.ToCmdLine("RENAME", key, newKey))
	asserts.AssertErrReply(t, result, "ERR timeout")
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("EXISTS", key))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("TTL", key))
	asserts.AssertIntReplyGreaterThan(t, result, 0)
	result = testNodeB.db.Exec(conn, utils.ToCmdLine("EXISTS", newKey))
	asserts.AssertIntReply(t, result, 0)
	*simulateATimout = false

	// test dest prepare failed
	*simulateBTimout = true
	key = testNodeA.self + utils.RandString(10)
	newKey = testNodeB.self + utils.RandString(10) // route to testNodeB, see mockPicker.PickNode
	value = utils.RandString(10)
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", key, value, "ex", "1000"))
	result = Rename(testNodeA, conn, utils.ToCmdLine("RENAME", key, newKey))
	asserts.AssertErrReply(t, result, "ERR timeout")
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("EXISTS", key))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("TTL", key))
	asserts.AssertIntReplyGreaterThan(t, result, 0)
	result = testNodeB.db.Exec(conn, utils.ToCmdLine("EXISTS", newKey))
	asserts.AssertIntReply(t, result, 0)
	*simulateBTimout = false

	result = Rename(testNodeA, conn, utils.ToCmdLine("RENAME", key))
	asserts.AssertErrReply(t, result, "ERR wrong number of arguments for 'rename' command")
}

func TestRenameNx(t *testing.T) {
	conn := new(connection.FakeConn)
	testNodeA.db.Exec(conn, utils.ToCmdLine("FlushALL"))
	// cross node rename
	key := testNodeA.self + utils.RandString(10)
	value := utils.RandString(10)
	newKey := testNodeB.self + utils.RandString(10) // route to testNodeB, see mockPicker.PickNode
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", key, value, "ex", "1000"))
	result := RenameNx(testNodeA, conn, utils.ToCmdLine("RENAMENX", key, newKey))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("EXISTS", key))
	asserts.AssertIntReply(t, result, 0)
	result = testNodeB.db.Exec(conn, utils.ToCmdLine("EXISTS", newKey))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeB.db.Exec(conn, utils.ToCmdLine("TTL", newKey))
	asserts.AssertIntReplyGreaterThan(t, result, 0)

	// cross node rename, dest key exist
	key = testNodeA.self + utils.RandString(10)
	value = utils.RandString(10)
	newKey = testNodeB.self + utils.RandString(10) // route to testNodeB, see mockPicker.PickNode
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", key, value, "ex", "1000"))
	testNodeB.db.Exec(conn, utils.ToCmdLine("SET", newKey, newKey))
	result = RenameNx(testNodeA, conn, utils.ToCmdLine("RENAMENX", key, newKey))
	asserts.AssertIntReply(t, result, 0)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("EXISTS", key))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("TTL", key))
	asserts.AssertIntReplyGreaterThan(t, result, 0)
	result = testNodeB.db.Exec(conn, utils.ToCmdLine("GET", newKey))
	asserts.AssertBulkReply(t, result, newKey)

	// same node rename
	key = testNodeA.self + utils.RandString(10)
	value = utils.RandString(10)
	newKey = key + utils.RandString(2)
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", key, value, "ex", "1000"))
	result = RenameNx(testNodeA, conn, utils.ToCmdLine("RENAMENX", key, newKey))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("EXISTS", key))
	asserts.AssertIntReply(t, result, 0)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("EXISTS", newKey))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("TTL", newKey))
	asserts.AssertIntReplyGreaterThan(t, result, 0)

	// test src prepare failed
	*simulateATimout = true
	key = testNodeA.self + utils.RandString(10)
	newKey = testNodeB.self + utils.RandString(10) // route to testNodeB, see mockPicker.PickNode
	value = utils.RandString(10)
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", key, value, "ex", "1000"))
	result = RenameNx(testNodeB, conn, utils.ToCmdLine("RENAMENX", key, newKey))
	asserts.AssertErrReply(t, result, "ERR timeout")
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("EXISTS", key))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("TTL", key))
	asserts.AssertIntReplyGreaterThan(t, result, 0)
	result = testNodeB.db.Exec(conn, utils.ToCmdLine("EXISTS", newKey))
	asserts.AssertIntReply(t, result, 0)
	*simulateATimout = false

	// test dest prepare failed
	*simulateBTimout = true
	key = testNodeA.self + utils.RandString(10)
	newKey = testNodeB.self + utils.RandString(10) // route to testNodeB, see mockPicker.PickNode
	value = utils.RandString(10)
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", key, value, "ex", "1000"))
	result = RenameNx(testNodeA, conn, utils.ToCmdLine("RENAMENX", key, newKey))
	asserts.AssertErrReply(t, result, "ERR timeout")
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("EXISTS", key))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("TTL", key))
	asserts.AssertIntReplyGreaterThan(t, result, 0)
	result = testNodeB.db.Exec(conn, utils.ToCmdLine("EXISTS", newKey))
	asserts.AssertIntReply(t, result, 0)
	*simulateBTimout = false

	result = RenameNx(testNodeA, conn, utils.ToCmdLine("RENAMENX", key))
	asserts.AssertErrReply(t, result, "ERR wrong number of arguments for 'renamenx' command")

}
