package cluster

import (
	"github.com/hdt3213/godis/redis/reply/asserts"
	"testing"
)

func TestDel(t *testing.T) {
	allowFastTransaction = false
	testCluster.Exec(nil, toArgs("SET", "a", "a"))
	ret := Del(testCluster, nil, toArgs("DEL", "a", "b", "c"))
	asserts.AssertNotError(t, ret)
	ret = testCluster.Exec(nil, toArgs("GET", "a"))
	asserts.AssertNullBulk(t, ret)
}
