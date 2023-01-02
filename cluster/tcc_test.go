package cluster

import (
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"math/rand"
	"strconv"
	"testing"
)

func TestRollback(t *testing.T) {
	// rollback uncommitted transaction
	testNodeA := testCluster[0]
	conn := new(connection.FakeConn)
	FlushAll(testNodeA, conn, toArgs("FLUSHALL"))
	txID := rand.Int63()
	txIDStr := strconv.FormatInt(txID, 10)
	keys := []string{"a", "{a}1"}
	groupMap := map[string][]string{
		testNodeA.self: keys,
	}
	args := []string{txIDStr, "DEL"}
	args = append(args, keys...)
	testNodeA.db.Exec(conn, toArgs("SET", "a", "a"))
	ret := execPrepare(testNodeA, conn, makeArgs("Prepare", args...))
	asserts.AssertNotError(t, ret)
	requestRollback(testNodeA, conn, txID, groupMap)
	ret = testNodeA.db.Exec(conn, toArgs("GET", "a"))
	asserts.AssertBulkReply(t, ret, "a")

	// rollback committed transaction
	FlushAll(testNodeA, conn, toArgs("FLUSHALL"))
	testNodeA.db.Exec(conn, toArgs("SET", "a", "a"))
	txID = rand.Int63()
	txIDStr = strconv.FormatInt(txID, 10)
	args = []string{txIDStr, "DEL"}
	args = append(args, keys...)
	ret = execPrepare(testNodeA, conn, makeArgs("Prepare", args...))
	asserts.AssertNotError(t, ret)
	_, err := requestCommit(testNodeA, conn, txID, groupMap)
	if err != nil {
		t.Errorf("del failed %v", err)
		return
	}
	ret = testNodeA.db.Exec(conn, toArgs("GET", "a")) // call db.Exec to skip key router
	asserts.AssertNullBulk(t, ret)
	requestRollback(testNodeA, conn, txID, groupMap)
	ret = testNodeA.db.Exec(conn, toArgs("GET", "a"))
	asserts.AssertBulkReply(t, ret, "a")
}
