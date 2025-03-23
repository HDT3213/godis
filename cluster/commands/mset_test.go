package commands

import (
	"testing"

	"github.com/hdt3213/godis/cluster/core"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol/asserts"
)

func TestMset(t *testing.T) {
	id1 := "1"
	id2 := "2"
	nodes := core.MakeTestCluster([]string{id1, id2})
	node1 := nodes[id1]
	c := connection.NewFakeConn()
	// 1, 2 will be routed to node1 and node2, see MakeTestCluster
	res := execMSet(node1, c, utils.ToCmdLine("mset", "1", "1", "2", "2"))
	asserts.AssertStatusReply(t, res, "OK")
	res2 := execMGet(node1, c, utils.ToCmdLine("mget", "1", "2"))
	asserts.AssertMultiBulkReply(t, res2, []string{"1", "2"})
}

func TestMsetNx(t *testing.T) {
	id1 := "1"
	id2 := "2"
	nodes := core.MakeTestCluster([]string{id1, id2})
	node1 := nodes[id1]
	c := connection.NewFakeConn()
	// 1, 2 will be routed to node1 and node2, see MakeTestCluster
	res := execMSetNx(node1, c, utils.ToCmdLine("mset", "1", "1", "2", "2"))
	asserts.AssertIntReply(t, res, 1)
	res2 := execMGet(node1, c, utils.ToCmdLine("mget", "1", "2"))
	asserts.AssertMultiBulkReply(t, res2, []string{"1", "2"})

	res = execMSetNx(node1, c, utils.ToCmdLine("mset", "3", "3", "2", "2"))
	asserts.AssertIntReply(t, res, 0)
	core.RegisterDefaultCmd("get")
	res = node1.Exec(c, utils.ToCmdLine("get", "3"))
	asserts.AssertNullBulk(t, res)
}