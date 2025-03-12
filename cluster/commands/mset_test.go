package commands

import (
	"testing"

	"github.com/hdt3213/godis/cluster/core"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
)

func TestMset(t *testing.T) {
	id1 := "1"
	id2 := "2"
	nodes := core.MakeTestCluster([]string{id1, id2})
	node1 := nodes[id1]
	c := connection.NewFakeConn()
	// 1, 2 will be routed to node1 and node2, see MakeTestCluster
	res := execMSet(node1, c, utils.ToCmdLine("mset", "1", "1", "2", "2"))
	println(res)
}