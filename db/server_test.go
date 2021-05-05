package db

import (
	"github.com/hdt3213/godis/lib/utils"
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
