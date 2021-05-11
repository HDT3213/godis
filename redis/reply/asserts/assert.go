package asserts

import (
	"fmt"
	"github.com/hdt3213/godis/datastruct/utils"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/reply"
	"runtime"
	"testing"
)

// AssertIntReply checks if the given redis.Reply is the expected integer
func AssertIntReply(t *testing.T, actual redis.Reply, expected int) {
	intResult, ok := actual.(*reply.IntReply)
	if !ok {
		t.Errorf("expected int reply, actually %s, %s", actual.ToBytes(), printStack())
		return
	}
	if intResult.Code != int64(expected) {
		t.Errorf("expected %d, actually %d, %s", expected, intResult.Code, printStack())
	}
}

// AssertBulkReply checks if the given redis.Reply is the expected string
func AssertBulkReply(t *testing.T, actual redis.Reply, expected string) {
	bulkReply, ok := actual.(*reply.BulkReply)
	if !ok {
		t.Errorf("expected bulk reply, actually %s, %s", actual.ToBytes(), printStack())
		return
	}
	if !utils.BytesEquals(bulkReply.Arg, []byte(expected)) {
		t.Errorf("expected %s, actually %s, %s", expected, actual.ToBytes(), printStack())
	}
}

// AssertStatusReply checks if the given redis.Reply is the expected status
func AssertStatusReply(t *testing.T, actual redis.Reply, expected string) {
	statusReply, ok := actual.(*reply.StatusReply)
	if !ok {
		// may be a reply.OkReply e.g.
		expectBytes := reply.MakeStatusReply(expected).ToBytes()
		if utils.BytesEquals(actual.ToBytes(), expectBytes) {
			return
		}
		t.Errorf("expected bulk reply, actually %s, %s", actual.ToBytes(), printStack())
		return
	}
	if statusReply.Status != expected {
		t.Errorf("expected %s, actually %s, %s", expected, actual.ToBytes(), printStack())
	}
}

// AssertErrReply checks if the given redis.Reply is the expected error
func AssertErrReply(t *testing.T, actual redis.Reply, expected string) {
	errReply, ok := actual.(reply.ErrorReply)
	if !ok {
		expectBytes := reply.MakeErrReply(expected).ToBytes()
		if utils.BytesEquals(actual.ToBytes(), expectBytes) {
			return
		}
		t.Errorf("expected err reply, actually %s, %s", actual.ToBytes(), printStack())
		return
	}
	if errReply.Error() != expected {
		t.Errorf("expected %s, actually %s, %s", expected, actual.ToBytes(), printStack())
	}
}

// AssertNotError checks if the given redis.Reply is not error reply
func AssertNotError(t *testing.T, result redis.Reply) {
	if result == nil {
		t.Errorf("result is nil %s", printStack())
		return
	}
	bytes := result.ToBytes()
	if len(bytes) == 0 {
		t.Errorf("result is empty %s", printStack())
		return
	}
	if bytes[0] == '-' {
		t.Errorf("result is err reply %s", printStack())
	}
}

// AssertNullBulk checks if the given redis.Reply is reply.NullBulkReply
func AssertNullBulk(t *testing.T, result redis.Reply) {
	if result == nil {
		t.Errorf("result is nil %s", printStack())
		return
	}
	bytes := result.ToBytes()
	if len(bytes) == 0 {
		t.Errorf("result is empty %s", printStack())
		return
	}
	expect := (&reply.NullBulkReply{}).ToBytes()
	if !utils.BytesEquals(expect, bytes) {
		t.Errorf("result is not null-bulk-reply %s", printStack())
	}
}

// AssertMultiBulkReply checks if the given redis.Reply has the expected content
func AssertMultiBulkReply(t *testing.T, actual redis.Reply, expected []string) {
	multiBulk, ok := actual.(*reply.MultiBulkReply)
	if !ok {
		t.Errorf("expected bulk reply, actually %s, %s", actual.ToBytes(), printStack())
		return
	}
	if len(multiBulk.Args) != len(expected) {
		t.Errorf("expected %d elements, actually %d, %s",
			len(expected), len(multiBulk.Args), printStack())
		return
	}
	for i, v := range multiBulk.Args {
		str := string(v)
		if str != expected[i] {
			t.Errorf("expected %s, actually %s, %s", expected[i], actual, printStack())
		}
	}
}

// AssertMultiBulkReplySize check if redis.Reply has expected length
func AssertMultiBulkReplySize(t *testing.T, actual redis.Reply, expected int) {
	multiBulk, ok := actual.(*reply.MultiBulkReply)
	if !ok {
		if expected == 0 &&
			utils.BytesEquals(actual.ToBytes(), reply.MakeEmptyMultiBulkReply().ToBytes()) {
			return
		}
		t.Errorf("expected bulk reply, actually %s, %s", actual.ToBytes(), printStack())
		return
	}
	if len(multiBulk.Args) != expected {
		t.Errorf("expected %d elements, actually %d, %s", expected, len(multiBulk.Args), printStack())
		return
	}
}

func printStack() string {
	_, file, no, ok := runtime.Caller(2)
	if ok {
		return fmt.Sprintf("at %s#%d", file, no)
	}
	return ""
}
