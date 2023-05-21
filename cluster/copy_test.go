package cluster

import (
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"testing"
)

func TestCopy(t *testing.T) {
	conn := new(connection.FakeConn)
	testNodeA := testCluster[0]
	testNodeB := testCluster[1]
	testNodeA.Exec(conn, utils.ToCmdLine("FlushALL"))
	testNodeB.Exec(conn, utils.ToCmdLine("FlushALL"))

	// cross node copy
	srcKey := "127.0.0.1:6399Bk2r3Sz0V5" // use fix key to ensure hashing to different node
	destKey := "127.0.0.1:7379CcdC0QOopF"
	value := utils.RandString(10)
	testNodeA.Exec(conn, utils.ToCmdLine("SET", srcKey, value))
	result := Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.Exec(conn, utils.ToCmdLine("GET", srcKey))
	asserts.AssertBulkReply(t, result, value)
	result = testNodeB.Exec(conn, utils.ToCmdLine("GET", destKey))
	asserts.AssertBulkReply(t, result, value)
	// key exists
	result = Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey))
	asserts.AssertIntReply(t, result, 0)
	// replace
	value = utils.RandString(10)
	testNodeA.Exec(conn, utils.ToCmdLine("SET", srcKey, value))
	result = Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey, "REPLACE"))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.Exec(conn, utils.ToCmdLine("GET", srcKey))
	asserts.AssertBulkReply(t, result, value)
	result = testNodeB.Exec(conn, utils.ToCmdLine("GET", destKey))
	asserts.AssertBulkReply(t, result, value)
	// test copy expire time
	testNodeA.Exec(conn, utils.ToCmdLine("SET", srcKey, value, "EX", "1000"))
	result = Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey, "REPLACE"))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.Exec(conn, utils.ToCmdLine("TTL", srcKey))
	asserts.AssertIntReplyGreaterThan(t, result, 0)
	result = testNodeB.Exec(conn, utils.ToCmdLine("TTL", destKey))
	asserts.AssertIntReplyGreaterThan(t, result, 0)

	// same node copy
	srcKey = "{" + testNodeA.self + "}" + utils.RandString(10)
	destKey = "{" + testNodeA.self + "}" + utils.RandString(9)
	value = utils.RandString(10)
	testNodeA.Exec(conn, utils.ToCmdLine("SET", srcKey, value))
	result = Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.Exec(conn, utils.ToCmdLine("GET", srcKey))
	asserts.AssertBulkReply(t, result, value)
	result = testNodeA.Exec(conn, utils.ToCmdLine("GET", destKey))
	asserts.AssertBulkReply(t, result, value)
	// key exists
	result = Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey))
	asserts.AssertIntReply(t, result, 0)
	// replace
	value = utils.RandString(10)
	testNodeA.Exec(conn, utils.ToCmdLine("SET", srcKey, value))
	result = Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey, "REPLACE"))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.Exec(conn, utils.ToCmdLine("GET", srcKey))
	asserts.AssertBulkReply(t, result, value)
	result = testNodeA.Exec(conn, utils.ToCmdLine("GET", destKey))
	asserts.AssertBulkReply(t, result, value)
	// test copy expire time
	testNodeA.Exec(conn, utils.ToCmdLine("SET", srcKey, value, "EX", "1000"))
	result = Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey, "REPLACE"))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.Exec(conn, utils.ToCmdLine("TTL", srcKey))
	asserts.AssertIntReplyGreaterThan(t, result, 0)
	result = testNodeA.Exec(conn, utils.ToCmdLine("TTL", destKey))
	asserts.AssertIntReplyGreaterThan(t, result, 0)
}

func TestCopyTimeout(t *testing.T) {
	conn := new(connection.FakeConn)
	testNodeA := testCluster[0]
	testNodeB := testCluster[1]
	testNodeA.Exec(conn, utils.ToCmdLine("FlushALL"))
	testNodeB.Exec(conn, utils.ToCmdLine("FlushALL"))

	// test src prepare failed
	timeoutFlags[0] = true
	srcKey := "127.0.0.1:6399Bk2r3Sz0V5" // use fix key to ensure hashing to different node
	destKey := "127.0.0.1:7379CcdC0QOopF"
	value := utils.RandString(10)
	testNodeA.Exec(conn, utils.ToCmdLine("SET", srcKey, value, "ex", "1000"))
	result := Rename(testNodeB, conn, utils.ToCmdLine("RENAME", srcKey, destKey))
	asserts.AssertErrReply(t, result, "ERR timeout")
	result = testNodeA.Exec(conn, utils.ToCmdLine("EXISTS", srcKey))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.Exec(conn, utils.ToCmdLine("TTL", srcKey))
	asserts.AssertIntReplyGreaterThan(t, result, 0)
	result = testNodeB.Exec(conn, utils.ToCmdLine("EXISTS", destKey))
	asserts.AssertIntReply(t, result, 0)
	timeoutFlags[0] = false

	// test dest prepare failed
	timeoutFlags[1] = true
	value = utils.RandString(10)
	testNodeA.Exec(conn, utils.ToCmdLine("SET", srcKey, value, "ex", "1000"))
	result = Rename(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey))
	asserts.AssertErrReply(t, result, "ERR timeout")
	result = testNodeA.Exec(conn, utils.ToCmdLine("EXISTS", srcKey))
	asserts.AssertIntReply(t, result, 1)
	result = testNodeA.Exec(conn, utils.ToCmdLine("TTL", srcKey))
	asserts.AssertIntReplyGreaterThan(t, result, 0)
	result = testNodeB.Exec(conn, utils.ToCmdLine("EXISTS", destKey))
	asserts.AssertIntReply(t, result, 0)
	timeoutFlags[1] = false
	// Copying to another database
	srcKey = testNodeA.self + utils.RandString(10)
	value = utils.RandString(10)
	destKey = srcKey + utils.RandString(2)
	testNodeA.Exec(conn, utils.ToCmdLine("SET", srcKey, value))
	result = Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey, destKey, "db", "1"))
	asserts.AssertErrReply(t, result, copyToAnotherDBErr)

	result = Copy(testNodeA, conn, utils.ToCmdLine("COPY", srcKey))
	asserts.AssertErrReply(t, result, "ERR wrong number of arguments for 'copy' command")
}
