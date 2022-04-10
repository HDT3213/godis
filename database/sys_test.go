package database

import (
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"testing"
)

func TestPing(t *testing.T) {
	actual := Ping(testDB, utils.ToCmdLine())
	asserts.AssertStatusReply(t, actual, "PONG")
	val := utils.RandString(5)
	actual = Ping(testDB, utils.ToCmdLine(val))
	asserts.AssertStatusReply(t, actual, val)
	actual = Ping(testDB, utils.ToCmdLine(val, val))
	asserts.AssertErrReply(t, actual, "ERR wrong number of arguments for 'ping' command")
}

func TestAuth(t *testing.T) {
	passwd := utils.RandString(10)
	c := &connection.FakeConn{}
	ret := testServer.Exec(c, utils.ToCmdLine("AUTH"))
	asserts.AssertErrReply(t, ret, "ERR wrong number of arguments for 'auth' command")
	ret = testServer.Exec(c, utils.ToCmdLine("AUTH", passwd))
	asserts.AssertErrReply(t, ret, "ERR Client sent AUTH, but no password is set")

	config.Properties.RequirePass = passwd
	defer func() {
		config.Properties.RequirePass = ""
	}()
	ret = testServer.Exec(c, utils.ToCmdLine("AUTH", passwd+"wrong"))
	asserts.AssertErrReply(t, ret, "ERR invalid password")
	ret = testServer.Exec(c, utils.ToCmdLine("PING"))
	asserts.AssertErrReply(t, ret, "NOAUTH Authentication required")
	ret = testServer.Exec(c, utils.ToCmdLine("AUTH", passwd))
	asserts.AssertStatusReply(t, ret, "OK")

}
