package cluster

import (
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"testing"
)

func TestDel(t *testing.T) {
	conn := &connection.FakeConn{}
	allowFastTransaction = false
	testCluster.Exec(conn, toArgs("SET", "a", "a"))
	ret := Del(testCluster, conn, toArgs("DEL", "a", "b", "c"))
	asserts.AssertNotError(t, ret)
	ret = testCluster.Exec(conn, toArgs("GET", "a"))
	asserts.AssertNullBulk(t, ret)
}
