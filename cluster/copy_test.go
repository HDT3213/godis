package cluster

import (
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"testing"
)

func TestCopy(t *testing.T) {
	conn := new(connection.FakeConn)
	testNodeA.db.Exec(conn, utils.ToCmdLine("FlushALL"))

	// cross node copy
	srcKey := testNodeA.self + utils.RandString(10)
	value := utils.RandString(10)
	destKey := testNodeB.self + utils.RandString(10)
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", srcKey, value))
	result := Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("GET", srcKey))
	asserts.AssertBulkReply(t, result, value)
	result = testNodeB.db.Exec(conn, utils.ToCmdLine("GET", destKey))
	asserts.AssertBulkReply(t, result, value)
	// key exists
	result = Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey))
	asserts.AssertErrReply(t, result, keyExistsErr)
	// replace
	value = utils.RandString(10)
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", srcKey, value))
	result = Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey, "REPLACE"))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("GET", srcKey))
	asserts.AssertBulkReply(t, result, value)
	result = testNodeB.db.Exec(conn, utils.ToCmdLine("GET", destKey))
	asserts.AssertBulkReply(t, result, value)
	// test copy expire time
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", srcKey, value, "EX", "1000"))
	result = Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey, "REPLACE"))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("TTL", srcKey))
	asserts.AssertIntReplyGreaterThan(t, result, 0)
	result = testNodeB.db.Exec(conn, utils.ToCmdLine("TTL", destKey))
	asserts.AssertIntReplyGreaterThan(t, result, 0)

	// same node copy
	srcKey = testNodeA.self + utils.RandString(10)
	value = utils.RandString(10)
	destKey = srcKey + utils.RandString(2)
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", srcKey, value))
	result = Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("GET", srcKey))
	asserts.AssertBulkReply(t, result, value)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("GET", destKey))
	asserts.AssertBulkReply(t, result, value)
	// key exists
	result = Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey))
	asserts.AssertIntReply(t, result, 0)
	// replace
	value = utils.RandString(10)
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", srcKey, value))
	result = Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey, "REPLACE"))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("GET", srcKey))
	asserts.AssertBulkReply(t, result, value)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("GET", destKey))
	asserts.AssertBulkReply(t, result, value)
	// test copy expire time
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", srcKey, value, "EX", "1000"))
	result = Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey, "REPLACE"))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("TTL", srcKey))
	asserts.AssertIntReplyGreaterThan(t, result, 0)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("TTL", destKey))
	asserts.AssertIntReplyGreaterThan(t, result, 0)

	// test src prepare failed
	*simulateATimout = true
	srcKey = testNodeA.self + utils.RandString(10)
	destKey = testNodeB.self + utils.RandString(10) // route to testNodeB, see mockPicker.PickNode
	value = utils.RandString(10)
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", srcKey, value, "ex", "1000"))
	result = Rename(testNodeB, conn, utils.ToCmdLine("RENAME", srcKey, destKey))
	asserts.AssertErrReply(t, result, "ERR timeout")
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("EXISTS", srcKey))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("TTL", srcKey))
	asserts.AssertIntReplyGreaterThan(t, result, 0)
	result = testNodeB.db.Exec(conn, utils.ToCmdLine("EXISTS", destKey))
	asserts.AssertIntReply(t, result, 0)
	*simulateATimout = false

	// test dest prepare failed
	*simulateBTimout = true
	srcKey = testNodeA.self + utils.RandString(10)
	destKey = testNodeB.self + utils.RandString(10) // route to testNodeB, see mockPicker.PickNode
	value = utils.RandString(10)
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", srcKey, value, "ex", "1000"))
	result = Rename(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey))
	asserts.AssertErrReply(t, result, "ERR timeout")
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("EXISTS", srcKey))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.db.Exec(conn, utils.ToCmdLine("TTL", srcKey))
	asserts.AssertIntReplyGreaterThan(t, result, 0)
	result = testNodeB.db.Exec(conn, utils.ToCmdLine("EXISTS", destKey))
	asserts.AssertIntReply(t, result, 0)
	*simulateBTimout = false

	// Copying to another database
	srcKey = testNodeA.self + utils.RandString(10)
	value = utils.RandString(10)
	destKey = srcKey + utils.RandString(2)
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", srcKey, value))
	result = Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey, "db", "1"))
	asserts.AssertErrReply(t, result, copyToAnotherDBErr)

	result = Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey))
	asserts.AssertErrReply(t, result, "ERR wrong number of arguments for 'copy' command")
}
