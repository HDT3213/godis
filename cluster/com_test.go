package cluster

import (
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"testing"
)

func TestExec(t *testing.T) {
	testCluster := testCluster[0]
	conn := connection.NewFakeConn()
	for i := 0; i < 1000; i++ {
		key := RandString(4)
		value := RandString(4)
		testCluster.Exec(conn, toArgs("SET", key, value))
		ret := testCluster.Exec(conn, toArgs("GET", key))
		asserts.AssertBulkReply(t, ret, value)
	}
}

func TestAuth(t *testing.T) {
	passwd := utils.RandString(10)
	config.Properties.RequirePass = passwd
	defer func() {
		config.Properties.RequirePass = ""
	}()
	conn := connection.NewFakeConn()
	testCluster := testCluster[0]
	ret := testCluster.Exec(conn, toArgs("GET", "a"))
	asserts.AssertErrReply(t, ret, "NOAUTH Authentication required")
	ret = testCluster.Exec(conn, toArgs("AUTH", passwd))
	asserts.AssertStatusReply(t, ret, "OK")
	ret = testCluster.Exec(conn, toArgs("GET", "a"))
	asserts.AssertNotError(t, ret)
}

func TestRelay(t *testing.T) {
	testNodeA := testCluster[1]
	key := RandString(4)
	value := RandString(4)
	conn := connection.NewFakeConn()
	ret := testNodeA.relay(addresses[1], conn, toArgs("SET", key, value))
	asserts.AssertNotError(t, ret)
	ret = testNodeA.relay(addresses[1], conn, toArgs("GET", key))
	asserts.AssertBulkReply(t, ret, value)
}

func TestBroadcast(t *testing.T) {
	testCluster2 := testCluster[0]
	key := RandString(4)
	value := RandString(4)
	rets := testCluster2.broadcast(connection.NewFakeConn(), toArgs("SET", key, value))
	for _, v := range rets {
		asserts.AssertNotError(t, v)
	}
}
