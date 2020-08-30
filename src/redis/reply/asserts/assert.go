package asserts

import (
    "fmt"
    "github.com/HDT3213/godis/src/datastruct/utils"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
    "testing"
)

func AssertIntReply(t *testing.T, actual redis.Reply, expected int) {
    intResult, ok := actual.(*reply.IntReply)
    if !ok {
        t.Error(fmt.Sprintf("expected int reply, actually %s", actual.ToBytes()))
        return
    }
    if intResult.Code != int64(expected) {
        t.Error(fmt.Sprintf("expected %d, actually %d", expected, intResult.Code))
    }
}

func AssertBulkReply(t *testing.T, actual redis.Reply, expected string) {
    bulkReply, ok := actual.(*reply.BulkReply)
    if !ok {
        t.Error(fmt.Sprintf("expected bulk reply, actually %s", actual.ToBytes()))
        return
    }
    if !utils.BytesEquals(bulkReply.Arg, []byte(expected)) {
        t.Error(fmt.Sprintf("expected %s, actually %s", expected, actual.ToBytes()))
    }
}

func AssertMultiBulkReply(t *testing.T, actual redis.Reply, expected []string) {
    multiBulk, ok := actual.(*reply.MultiBulkReply)
    if !ok {
        t.Error(fmt.Sprintf("expected bulk reply, actually %s", actual.ToBytes()))
        return
    }
    if len(multiBulk.Args) != len(expected) {
        t.Error(fmt.Sprintf("expected %d elements, actually %d", len(expected), len(multiBulk.Args)))
        return
    }
    for i, v := range multiBulk.Args {
        actual := string(v)
        if actual != expected[i] {
            t.Error(fmt.Sprintf("expected %s, actually %s", expected[i], actual))
        }
    }
}
