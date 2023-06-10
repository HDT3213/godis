package cluster

import (
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"testing"
)

func TestMSet(t *testing.T) {
	conn := connection.NewFakeConn()
	allowFastTransaction = false
	testNodeA := testCluster[0]
	ret := MSet(testNodeA, conn, toArgs("MSET", "a", "a", "b", "b"))
	asserts.AssertNotError(t, ret)
	ret = testNodeA.Exec(conn, toArgs("MGET", "a", "b"))
	asserts.AssertMultiBulkReply(t, ret, []string{"a", "b"})
}

func TestMSetNx(t *testing.T) {
	conn := connection.NewFakeConn()
	allowFastTransaction = false
	testNodeA := testCluster[0]
	FlushAll(testNodeA, conn, toArgs("FLUSHALL"))
	ret := MSetNX(testNodeA, conn, toArgs("MSETNX", "a", "a", "b", "b"))
	asserts.AssertNotError(t, ret)
	ret = MSetNX(testNodeA, conn, toArgs("MSETNX", "a", "a", "c", "c"))
	asserts.AssertNotError(t, ret)
	ret = testNodeA.Exec(conn, toArgs("MGET", "a", "b", "c"))
	asserts.AssertMultiBulkReply(t, ret, []string{"a", "b", ""})
}
