package cluster

import (
	"github.com/hdt3213/godis/redis/reply/asserts"
	"math/rand"
	"strconv"
	"testing"
)

func TestRollback(t *testing.T) {
	// rollback uncommitted transaction
	FlushAll(testCluster, nil, toArgs("FLUSHALL"))
	txId := rand.Int63()
	txIdStr := strconv.FormatInt(txId, 10)
	keys := []string{"a", "b"}
	groupMap := testCluster.groupBy(keys)
	args := []string{txIdStr}
	args = append(args, keys...)
	testCluster.Exec(nil, toArgs("SET", "a", "a"))
	ret := PrepareDel(testCluster, nil, makeArgs("PrepareDel", args...))
	asserts.AssertNotError(t, ret)
	RequestRollback(testCluster, nil, txId, groupMap)
	ret = testCluster.Exec(nil, toArgs("GET", "a"))
	asserts.AssertBulkReply(t, ret, "a")

	// rollback committed transaction
	FlushAll(testCluster, nil, toArgs("FLUSHALL"))
	txId = rand.Int63()
	txIdStr = strconv.FormatInt(txId, 10)
	args = []string{txIdStr}
	args = append(args, keys...)
	testCluster.Exec(nil, toArgs("SET", "a", "a"))
	ret = PrepareDel(testCluster, nil, makeArgs("PrepareDel", args...))
	asserts.AssertNotError(t, ret)
	_, err := RequestCommit(testCluster, nil, txId, groupMap)
	if err != nil {
		t.Errorf("del failed %v", err)
		return
	}
	ret = testCluster.Exec(nil, toArgs("GET", "a"))
	asserts.AssertNullBulk(t, ret)
	RequestRollback(testCluster, nil, txId, groupMap)
	ret = testCluster.Exec(nil, toArgs("GET", "a"))
	asserts.AssertBulkReply(t, ret, "a")
}
