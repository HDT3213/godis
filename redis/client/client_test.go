package client

import (
	"bytes"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"strconv"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})
	client, err := MakeClient("localhost:6379")
	if err != nil {
		t.Error(err)
	}
	client.Start()

	result := client.Send([][]byte{
		[]byte("PING"),
	})
	if statusRet, ok := result.(*protocol.StatusReply); ok {
		if statusRet.Status != "PONG" {
			t.Error("`ping` failed, result: " + statusRet.Status)
		}
	}

	result = client.Send([][]byte{
		[]byte("SET"),
		[]byte("a"),
		[]byte("a"),
	})
	if statusRet, ok := result.(*protocol.StatusReply); ok {
		if statusRet.Status != "OK" {
			t.Error("`set` failed, result: " + statusRet.Status)
		}
	}

	result = client.Send([][]byte{
		[]byte("GET"),
		[]byte("a"),
	})
	if bulkRet, ok := result.(*protocol.BulkReply); ok {
		if string(bulkRet.Arg) != "a" {
			t.Error("`get` failed, result: " + string(bulkRet.Arg))
		}
	}

	result = client.Send([][]byte{
		[]byte("DEL"),
		[]byte("a"),
	})
	if intRet, ok := result.(*protocol.IntReply); ok {
		if intRet.Code != 1 {
			t.Error("`del` failed, result: " + strconv.FormatInt(intRet.Code, 10))
		}
	}

	client.doHeartbeat() // random do heartbeat
	result = client.Send([][]byte{
		[]byte("GET"),
		[]byte("a"),
	})
	if _, ok := result.(*protocol.NullBulkReply); !ok {
		t.Error("`get` failed, result: " + string(result.ToBytes()))
	}

	result = client.Send([][]byte{
		[]byte("DEL"),
		[]byte("arr"),
	})

	result = client.Send([][]byte{
		[]byte("RPUSH"),
		[]byte("arr"),
		[]byte("1"),
		[]byte("2"),
		[]byte("c"),
	})
	if intRet, ok := result.(*protocol.IntReply); ok {
		if intRet.Code != 3 {
			t.Error("`rpush` failed, result: " + strconv.FormatInt(intRet.Code, 10))
		}
	}

	result = client.Send([][]byte{
		[]byte("LRANGE"),
		[]byte("arr"),
		[]byte("0"),
		[]byte("-1"),
	})
	if multiBulkRet, ok := result.(*protocol.MultiBulkReply); ok {
		if len(multiBulkRet.Args) != 3 ||
			string(multiBulkRet.Args[0]) != "1" ||
			string(multiBulkRet.Args[1]) != "2" ||
			string(multiBulkRet.Args[2]) != "c" {
			t.Error("`lrange` failed, result: " + string(multiBulkRet.ToBytes()))
		}
	}

	client.Close()
	ret := client.Send(utils.ToCmdLine("ping"))
	asserts.AssertErrReply(t, ret, "client closed")
}

func TestReconnect(t *testing.T) {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})
	client, err := MakeClient("localhost:6379")
	if err != nil {
		t.Error(err)
	}
	client.Start()

	_ = client.conn.Close()
	time.Sleep(time.Second) // wait for reconnecting
	success := false
	for i := 0; i < 3; i++ {
		result := client.Send([][]byte{
			[]byte("PING"),
		})
		if bytes.Equal(result.ToBytes(), []byte("+PONG\r\n")) {
			success = true
			break
		}
	}
	if !success {
		t.Error("reconnect error")
	}
}
