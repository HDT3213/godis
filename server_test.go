package godis

import (
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/reply/asserts"
	"testing"
)

func TestPing(t *testing.T) {
	actual := Ping(testDB, utils.ToBytesList())
	asserts.AssertStatusReply(t, actual, "PONG")
	val := utils.RandString(5)
	actual = Ping(testDB, utils.ToBytesList(val))
	asserts.AssertStatusReply(t, actual, val)
	actual = Ping(testDB, utils.ToBytesList(val, val))
	asserts.AssertErrReply(t, actual, "ERR wrong number of arguments for 'ping' command")
}

func TestAuth(t *testing.T) {
	passwd := utils.RandString(10)
	c := &connection.FakeConn{}
	ret := Auth(testDB, c, utils.ToBytesList())
	asserts.AssertErrReply(t, ret, "ERR wrong number of arguments for 'auth' command")
	ret = Auth(testDB, c, utils.ToBytesList(passwd))
	asserts.AssertErrReply(t, ret, "ERR Client sent AUTH, but no password is set")

	config.Properties.RequirePass = passwd
	defer func() {
		config.Properties.RequirePass = ""
	}()
	ret = Auth(testDB, c, utils.ToBytesList(passwd+passwd))
	asserts.AssertErrReply(t, ret, "ERR invalid password")
	ret = Auth(testDB, c, utils.ToBytesList(passwd))
	asserts.AssertStatusReply(t, ret, "OK")

}