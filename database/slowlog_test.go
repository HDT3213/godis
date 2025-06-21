package database

import (
	"github.com/hdt3213/godis/redis/protocol"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSlowLogger_Record(t *testing.T) {
	logger := NewSlowLogger(5, 1000) // 5条最大，1ms阈值

	// 记录一个慢查询
	start := time.Now()
	time.Sleep(2 * time.Millisecond) // 确保超过阈值
	logger.Record(start, [][]byte{[]byte("GET"), []byte("key1")}, "127.0.0.1:12345")

	// 验证记录
	if logger.Len() != 1 {
		t.Errorf("Expected 1 entry, got %d", logger.Len())
	}

	// 记录一个低于阈值的查询
	start = time.Now()
	logger.Record(start, [][]byte{[]byte("SET"), []byte("key2")}, "127.0.0.1:12346")
	if logger.Len() != 1 {
		t.Errorf("Below threshold query should not be recorded, got %d entries", logger.Len())
	}
}

func TestSlowLogger_GetEntries(t *testing.T) {
	logger := NewSlowLogger(10, 1000) // 10条最大，1ms阈值

	// 添加5个条目
	for i := 0; i < 5; i++ {
		start := time.Now()
		time.Sleep(2 * time.Millisecond)
		args := [][]byte{
			[]byte("CMD"),
			[]byte(strconv.Itoa(i)),
		}
		logger.Record(start, args, "client")
	}

	// 测试获取全部条目
	entries := logger.GetEntries(10)
	if len(entries) != 5 {
		t.Errorf("Expected 5 entries, got %d", len(entries))
	}

	// 测试获取部分条目
	entries = logger.GetEntries(3)
	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}

	// 验证条目顺序 (应该是最新在前)
	if string(entries[0].Command[1]) != "4" {
		t.Errorf("Expected newest entry first, got %s", entries[0].Command[1])
	}
	if string(entries[2].Command[1]) != "2" {
		t.Errorf("Expected oldest of the three to be 2, got %s", entries[2].Command[1])
	}

	// 测试获取0条
	entries = logger.GetEntries(0)
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries when requesting 0, got %d", len(entries))
	}

}

// TestSlowLogger_Len 测试获取日志数量
func TestSlowLogger_Len(t *testing.T) {
	logger := NewSlowLogger(10, 1000)
	if logger.Len() != 0 {
		t.Errorf("New logger should have 0 entries, got %d", logger.Len())
	}

	for i := 0; i < 3; i++ {
		start := time.Now()
		time.Sleep(2 * time.Millisecond)
		logger.Record(start, [][]byte{[]byte("LEN_CMD"), []byte(strconv.Itoa(i))}, "client")
	}

	if logger.Len() != 3 {
		t.Errorf("Expected 3 entries, got %d", logger.Len())
	}
}

func TestSlowLogger_Reset(t *testing.T) {
	logger := NewSlowLogger(10, 1000)

	// 添加条目
	for i := 0; i < 5; i++ {
		start := time.Now()
		time.Sleep(2 * time.Millisecond)
		logger.Record(start, [][]byte{[]byte("RESET_CMD"), []byte(strconv.Itoa(i))}, "client")
	}

	// Before resetting
	if logger.Len() != 5 {
		t.Errorf("Pre-reset should have 5 entries, got %d", logger.Len())
	}

	logger.Reset()

	// After resetting
	if logger.Len() != 0 {
		t.Errorf("Post-reset should have 0 entries, got %d", logger.Len())
	}

	// Verify ID reset
	start := time.Now()
	time.Sleep(2 * time.Millisecond)
	logger.Record(start, [][]byte{[]byte("NEW_CMD"), []byte("after reset")}, "client")
	if logger.GetEntries(1)[0].ID != 1 {
		t.Errorf("After reset, ID should start from 1, got %d", logger.GetEntries(1)[0].ID)
	}
}

func TestSlowLogger_MaxEntries(t *testing.T) {
	logger := NewSlowLogger(3, 1000) // 最大3条

	for i := 0; i < 5; i++ {
		start := time.Now()
		time.Sleep(2 * time.Millisecond)
		logger.Record(start, [][]byte{[]byte("MAX_CMD"), []byte(strconv.Itoa(i))}, "client")
	}

	if logger.Len() != 3 {
		t.Errorf("Expected max 3 entries, got %d", logger.Len())
	}

	entries := logger.GetEntries(5)
	if len(entries) != 3 {
		t.Fatalf("Expected 3 entries, got %d", len(entries))
	}

	expected := []string{"4", "3", "2"}
	for i, entry := range entries {
		if string(entry.Command[1]) != expected[i] {
			t.Errorf("Position %d: expected %s, got %s", i, expected[i], entry.Command[1])
		}
	}
}

func TestSlowLogger_HandleSlowlogCommand(t *testing.T) {
	logger := NewSlowLogger(10, 1000)

	for i := 0; i < 5; i++ {
		start := time.Now()
		time.Sleep(2 * time.Millisecond)
		logger.Record(start, [][]byte{[]byte("CMD"), []byte(strconv.Itoa(i))}, "client")
	}

	// get
	t.Run("GET_Subcommand", func(t *testing.T) {
		args := [][]byte{[]byte("SLOWLOG"), []byte("GET"), []byte("3")}
		reply := logger.HandleSlowlogCommand(args)

		multiReply, ok := reply.(*protocol.MultiRawReply)
		if !ok {
			t.Fatalf("Expected MultiRawReply, got %T", reply)
		}

		if len(multiReply.Replies) != 3 {
			t.Errorf("Expected 3 entries, got %d", len(multiReply.Replies))
		}

		for i, entryReply := range multiReply.Replies {
			entryMulti, ok := entryReply.(*protocol.MultiRawReply)
			if !ok {
				t.Fatalf("Entry %d: Expected MultiRawReply, got %T", i, entryReply)
			}

			if len(entryMulti.Replies) != 5 {
				t.Errorf("Entry %d: Expected 5 fields, got %d", i, len(entryMulti.Replies))
			}

			// Verify that the first field is ID
			idReply, ok := entryMulti.Replies[0].(*protocol.IntReply)
			if !ok {
				t.Errorf("Entry %d: ID field should be IntReply", i)
			}
			if idReply.Code <= 0 {
				t.Errorf("Entry %d: Invalid ID %d", i, idReply.Code)
			}
		}
	})

	// len
	t.Run("LEN_Subcommand", func(t *testing.T) {
		args := [][]byte{[]byte("SLOWLOG"), []byte("LEN")}
		reply := logger.HandleSlowlogCommand(args)

		intReply, ok := reply.(*protocol.IntReply)
		if !ok {
			t.Fatalf("Expected IntReply, got %T", reply)
		}

		if intReply.Code != 5 {
			t.Errorf("Expected 5 entries, got %d", intReply.Code)
		}
	})

	// reset
	t.Run("RESET_Subcommand", func(t *testing.T) {
		args := [][]byte{[]byte("SLOWLOG"), []byte("RESET")}
		reply := logger.HandleSlowlogCommand(args)

		_, ok := reply.(*protocol.OkReply)
		if !ok {
			t.Fatalf("Expected OkReply, got %T", reply)
		}

		// Verify if the log has been reset
		if logger.Len() != 0 {
			t.Errorf("After reset, expected 0 entries, got %d", logger.Len())
		}
	})

	// Invalid sub command
	t.Run("Invalid_Subcommand", func(t *testing.T) {
		args := [][]byte{[]byte("SLOWLOG"), []byte("INVALID")}
		reply := logger.HandleSlowlogCommand(args)

		errReply, ok := reply.(*protocol.StandardErrReply)
		if !ok {
			t.Fatalf("Expected StandardErrReply, got %T", reply)
		}

		expectedErr := "ERR Unknown subcommand or wrong number of arguments for  'invalid'"
		if !strings.Contains(errReply.Error(), expectedErr) {
			t.Errorf("Expected error '%s', got '%s'", expectedErr, errReply.Error())
		}
	})

	// The number of test parameters is incorrect
	t.Run("Invalid_Argument_Count", func(t *testing.T) {
		args := [][]byte{[]byte("SLOWLOG")}
		reply := logger.HandleSlowlogCommand(args)

		errReply, ok := reply.(*protocol.StandardErrReply)
		if !ok {
			t.Fatalf("Expected StandardErrReply, got %T", reply)
		}

		expectedErr := "ERR wrong number of arguments for 'SLOWLOG' command"
		if !strings.Contains(errReply.Error(), expectedErr) {
			t.Errorf("Expected error '%s', got '%s'", expectedErr, errReply.Error())
		}
	})
}
