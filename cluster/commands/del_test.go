package commands

import (
	"testing"

	"github.com/hdt3213/godis/cluster/core"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol/asserts"
)

func TestDel(t *testing.T) {
	id1 := "1"
	id2 := "2"
	nodes := core.MakeTestCluster([]string{id1, id2})
	node1 := nodes[id1]
	c := connection.NewFakeConn()
	core.RegisterDefaultCmd("set")
	core.RegisterDefaultCmd("exists")
	node1.Exec(c, utils.ToCmdLine("set", "1", "1"))
	node1.Exec(c, utils.ToCmdLine("set", "2", "2"))

	// 1, 2 will be routed to node1 and node2, see MakeTestCluster
	res := execDel(node1, c, utils.ToCmdLine("del", "1", "2"))
	asserts.AssertIntReply(t, res, 2)

	res = node1.Exec(c, utils.ToCmdLine("exists", "1"))
	asserts.AssertIntReply(t, res, 0)

	res = node1.Exec(c, utils.ToCmdLine("exists", "2"))
	asserts.AssertIntReply(t, res, 0)

	// in one node
	node1.Exec(c, utils.ToCmdLine("set", "3", "3"))
	res = execDel(node1, c, utils.ToCmdLine("del", "3"))
	asserts.AssertIntReply(t, res, 1)
	res = node1.Exec(c, utils.ToCmdLine("exists", "3"))
	asserts.AssertIntReply(t, res, 0)
}