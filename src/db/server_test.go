package db

import (
	"github.com/hdt3213/godis/src/redis/reply/asserts"
	"testing"
)

func TestPing(t *testing.T) {
	actual := Ping(testDB, toArgs())
	asserts.AssertStatusReply(t, actual, "PONG")
	val := RandString(5)
	actual = Ping(testDB, toArgs(val))
	asserts.AssertStatusReply(t, actual, val)
	actual = Ping(testDB, toArgs(val, val))
	asserts.AssertErrReply(t, actual, "ERR wrong number of arguments for 'ping' command")
}
