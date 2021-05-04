package cluster

import (
	"github.com/hdt3213/godis/redis/reply/asserts"
	"testing"
)

func TestMSet(t *testing.T) {
	allowFastTransaction = false
	ret := MSet(testCluster, nil, toArgs("MSET", "a", "a", "b", "b"))
	asserts.AssertNotError(t, ret)
	ret = testCluster.Exec(nil, toArgs("MGET", "a", "b"))
	asserts.AssertMultiBulkReply(t, ret, []string{"a", "b"})
}

func TestMSetNx(t *testing.T) {
	allowFastTransaction = false
	FlushAll(testCluster, nil, toArgs("FLUSHALL"))
	ret := MSetNX(testCluster, nil, toArgs("MSETNX", "a", "a", "b", "b"))
	asserts.AssertNotError(t, ret)
}
