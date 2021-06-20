package cluster

import (
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/reply"
	"github.com/hdt3213/godis/redis/reply/asserts"
	"testing"
)

func TestMultiExecOnSelf(t *testing.T) {
	testCluster.db.Flush()
	conn := new(connection.FakeConn)
	result := testCluster.Exec(conn, toArgs("MULTI"))
	asserts.AssertNotError(t, result)
	key := utils.RandString(10)
	value := utils.RandString(10)
	testCluster.Exec(conn, utils.ToCmdLine("set", key, value))
	key2 := utils.RandString(10)
	testCluster.Exec(conn, utils.ToCmdLine("rpush", key2, value))
	result = testCluster.Exec(conn, utils.ToCmdLine("exec"))
	asserts.AssertNotError(t, result)
	result = testCluster.Exec(conn, utils.ToCmdLine("get", key))
	asserts.AssertBulkReply(t, result, value)
	result = testCluster.Exec(conn, utils.ToCmdLine("lrange", key2, "0", "-1"))
	asserts.AssertMultiBulkReply(t, result, []string{value})
}

func TestEmptyMulti(t *testing.T) {
	testCluster.db.Flush()
	conn := new(connection.FakeConn)
	result := testCluster.Exec(conn, toArgs("MULTI"))
	asserts.AssertNotError(t, result)
	result = testCluster.Exec(conn, utils.ToCmdLine("PING"))
	asserts.AssertNotError(t, result)
	result = testCluster.Exec(conn, utils.ToCmdLine("EXEC"))
	asserts.AssertNotError(t, result)
	mbr := result.(*reply.MultiRawReply)
	asserts.AssertStatusReply(t, mbr.Replies[0], "PONG")
}

func TestMultiExecOnOthers(t *testing.T) {
	testCluster.db.Flush()
	conn := new(connection.FakeConn)
	result := testCluster.Exec(conn, toArgs("MULTI"))
	asserts.AssertNotError(t, result)
	key := utils.RandString(10)
	value := utils.RandString(10)
	testCluster.Exec(conn, utils.ToCmdLine("rpush", key, value))
	testCluster.Exec(conn, utils.ToCmdLine("lrange", key, "0", "-1"))

	cmdLines := conn.GetQueuedCmdLine()
	relayCmdLine := [][]byte{ // relay it to executing node
		relayMultiBytes,
	}
	relayCmdLine = append(relayCmdLine, encodeCmdLine(cmdLines)...)
	rawRelayResult := execRelayedMulti(testCluster, conn, relayCmdLine)
	if reply.IsErrorReply(rawRelayResult) {
		t.Error()
	}
	relayResult, ok := rawRelayResult.(*reply.MultiBulkReply)
	if !ok {
		t.Error()
	}
	rep, err := parseEncodedMultiRawReply(relayResult.Args)
	if err != nil {
		t.Error()
	}
	if len(rep.Replies) != 2 {
		t.Errorf("expect 2 replies actual %d", len(rep.Replies))
	}
	asserts.AssertMultiBulkReply(t, rep.Replies[1], []string{value})
}
