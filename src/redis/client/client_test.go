package client

import (
    "github.com/HDT3213/godis/src/lib/logger"
    "github.com/HDT3213/godis/src/redis/reply"
    "testing"
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
    if statusRet, ok := result.(*reply.StatusReply); ok {
        if statusRet.Status != "PONG" {
            t.Error("`ping` failed, result: " + statusRet.Status)
        }
    }

    result = client.Send([][]byte{
        []byte("SET"),
        []byte("a"),
        []byte("a"),
    })
    if statusRet, ok := result.(*reply.StatusReply); ok {
        if statusRet.Status != "OK" {
            t.Error("`set` failed, result: " + statusRet.Status)
        }
    }

    result = client.Send([][]byte{
        []byte("GET"),
        []byte("a"),
    })
    if bulkRet, ok := result.(*reply.BulkReply); ok {
        if string(bulkRet.Arg) != "a" {
            t.Error("`get` failed, result: " + string(bulkRet.Arg))
        }
    }

    result = client.Send([][]byte{
        []byte("DEL"),
        []byte("a"),
    })
    if intRet, ok := result.(*reply.IntReply); ok {
        if intRet.Code != 1 {
            t.Error("`del` failed, result: " + string(intRet.Code))
        }
    }

    result = client.Send([][]byte{
        []byte("GET"),
        []byte("a"),
    })
    if _, ok := result.(*reply.NullBulkReply); !ok {
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
    if intRet, ok := result.(*reply.IntReply); ok {
        if intRet.Code != 3 {
            t.Error("`rpush` failed, result: " + string(intRet.Code))
        }
    }

    result = client.Send([][]byte{
        []byte("LRANGE"),
        []byte("arr"),
        []byte("0"),
        []byte("-1"),
    })
    if multiBulkRet, ok := result.(*reply.MultiBulkReply); ok {
        if len(multiBulkRet.Args) != 3 ||
            string(multiBulkRet.Args[0]) != "1" ||
            string(multiBulkRet.Args[1]) != "2" ||
            string(multiBulkRet.Args[2]) != "c" {
            t.Error("`lrange` failed, result: " + string(multiBulkRet.ToBytes()))
        }
    }

    client.Close()
}
