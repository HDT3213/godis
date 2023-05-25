package database

import (
	"strconv"
	"testing"

	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
	"github.com/hdt3213/godis/redis/protocol/asserts"
)

func TestPush(t *testing.T) {
	testDB.Flush()
	size := 100

	// rpush single
	key := utils.RandString(10)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		value := utils.RandString(10)
		values[i] = []byte(value)
		result := testDB.Exec(nil, utils.ToCmdLine("rpush", key, value))
		if intResult, _ := result.(*protocol.IntReply); intResult.Code != int64(i+1) {
			t.Errorf("expected %d, actually %d", i+1, intResult.Code)
		}
	}
	actual := testDB.Exec(nil, utils.ToCmdLine("lrange", key, "0", "-1"))
	expected := protocol.MakeMultiBulkReply(values)
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("push error")
	}
	testDB.Remove(key)

	// rpush multi
	key = utils.RandString(10)
	args := make([]string, size+1)
	args[0] = key
	values = make([][]byte, size)
	for i := 0; i < size; i++ {
		value := utils.RandString(10)
		values[i] = []byte(value)
		args[i+1] = value
	}
	result := testDB.Exec(nil, utils.ToCmdLine2("rpush", args...))
	if intResult, _ := result.(*protocol.IntReply); intResult.Code != int64(size) {
		t.Errorf("expected %d, actually %d", size, intResult.Code)
	}
	actual = testDB.Exec(nil, utils.ToCmdLine("lrange", key, "0", "-1"))
	expected = protocol.MakeMultiBulkReply(values)
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("push error")
	}
	testDB.Remove(key)

	// left push single
	key = utils.RandString(10)
	values = make([][]byte, size)
	for i := 0; i < size; i++ {
		value := utils.RandString(10)
		values[size-i-1] = []byte(value)
		result = testDB.Exec(nil, utils.ToCmdLine("lpush", key, value))
		if intResult, _ := result.(*protocol.IntReply); intResult.Code != int64(i+1) {
			t.Errorf("expected %d, actually %d", i+1, intResult.Code)
		}
	}
	actual = testDB.Exec(nil, utils.ToCmdLine("lrange", key, "0", "-1"))
	expected = protocol.MakeMultiBulkReply(values)
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("push error")
	}
	testDB.Remove(key)

	// left push multi
	key = utils.RandString(10)
	args = make([]string, size+1)
	args[0] = key
	expectedValues := make([][]byte, size)
	for i := 0; i < size; i++ {
		value := utils.RandString(10)
		args[i+1] = value
		expectedValues[size-i-1] = []byte(value)
	}
	// result = execLPush(testDB, values)
	result = testDB.Exec(nil, utils.ToCmdLine2("lpush", args...))
	if intResult, _ := result.(*protocol.IntReply); intResult.Code != int64(size) {
		t.Errorf("expected %d, actually %d", size, intResult.Code)
	}
	actual = testDB.Exec(nil, utils.ToCmdLine("lrange", key, "0", "-1"))
	expected = protocol.MakeMultiBulkReply(expectedValues)
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("push error")
	}
	testDB.Remove(key)
}

func TestLRange(t *testing.T) {
	// prepare list
	testDB.Flush()
	size := 100
	key := utils.RandString(10)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		value := utils.RandString(10)
		testDB.Exec(nil, utils.ToCmdLine("rpush", key, value))
		values[i] = []byte(value)
	}

	start := "0"
	end := "9"
	actual := testDB.Exec(nil, utils.ToCmdLine("lrange", key, start, end))
	expected := protocol.MakeMultiBulkReply(values[0:10])
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Errorf("range error [%s, %s]", start, end)
	}

	start = "0"
	end = "200"
	actual = testDB.Exec(nil, utils.ToCmdLine("lrange", key, start, end))
	expected = protocol.MakeMultiBulkReply(values)
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Errorf("range error [%s, %s]", start, end)
	}

	start = "0"
	end = "-10"
	actual = testDB.Exec(nil, utils.ToCmdLine("lrange", key, start, end))
	expected = protocol.MakeMultiBulkReply(values[0 : size-10+1])
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Errorf("range error [%s, %s]", start, end)
	}

	start = "0"
	end = "-200"
	actual = testDB.Exec(nil, utils.ToCmdLine("lrange", key, start, end))
	expected = protocol.MakeMultiBulkReply(values[0:0])
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Errorf("range error [%s, %s]", start, end)
	}

	start = "-10"
	end = "-1"
	actual = testDB.Exec(nil, utils.ToCmdLine("lrange", key, start, end))
	expected = protocol.MakeMultiBulkReply(values[90:])
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Errorf("range error [%s, %s]", start, end)
	}
}

func TestLIndex(t *testing.T) {
	// prepare list
	testDB.Flush()
	size := 100
	key := utils.RandString(10)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		value := utils.RandString(10)
		testDB.Exec(nil, utils.ToCmdLine("rpush", key, value))
		values[i] = []byte(value)
	}

	result := testDB.Exec(nil, utils.ToCmdLine("llen", key))
	if intResult, _ := result.(*protocol.IntReply); intResult.Code != int64(size) {
		t.Errorf("expected %d, actually %d", size, intResult.Code)
	}

	for i := 0; i < size; i++ {
		result = testDB.Exec(nil, utils.ToCmdLine("lindex", key, strconv.Itoa(i)))
		expected := protocol.MakeBulkReply(values[i])
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes()))
		}
	}

	for i := 1; i <= size; i++ {
		result = testDB.Exec(nil, utils.ToCmdLine("lindex", key, strconv.Itoa(-i)))
		expected := protocol.MakeBulkReply(values[size-i])
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes()))
		}
	}
}

func TestLRem(t *testing.T) {
	// prepare list
	testDB.Flush()
	key := utils.RandString(10)
	values := []string{key, "a", "b", "a", "a", "c", "a", "a"}
	testDB.Exec(nil, utils.ToCmdLine2("rpush", values...))

	result := testDB.Exec(nil, utils.ToCmdLine("lrem", key, "1", "a"))
	if intResult, _ := result.(*protocol.IntReply); intResult.Code != 1 {
		t.Errorf("expected %d, actually %d", 1, intResult.Code)
	}
	result = testDB.Exec(nil, utils.ToCmdLine("llen", key))
	if intResult, _ := result.(*protocol.IntReply); intResult.Code != 6 {
		t.Errorf("expected %d, actually %d", 6, intResult.Code)
	}

	result = testDB.Exec(nil, utils.ToCmdLine("lrem", key, "-2", "a"))
	if intResult, _ := result.(*protocol.IntReply); intResult.Code != 2 {
		t.Errorf("expected %d, actually %d", 2, intResult.Code)
	}
	result = testDB.Exec(nil, utils.ToCmdLine("llen", key))
	if intResult, _ := result.(*protocol.IntReply); intResult.Code != 4 {
		t.Errorf("expected %d, actually %d", 4, intResult.Code)
	}

	result = testDB.Exec(nil, utils.ToCmdLine("lrem", key, "0", "a"))
	if intResult, _ := result.(*protocol.IntReply); intResult.Code != 2 {
		t.Errorf("expected %d, actually %d", 2, intResult.Code)
	}
	result = testDB.Exec(nil, utils.ToCmdLine("llen", key))
	if intResult, _ := result.(*protocol.IntReply); intResult.Code != 2 {
		t.Errorf("expected %d, actually %d", 2, intResult.Code)
	}
}

func TestLSet(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	values := []string{key, "a", "b", "c", "d", "e", "f"}
	testDB.Exec(nil, utils.ToCmdLine2("rpush", values...))

	// test positive index
	size := len(values) - 1
	for i := 0; i < size; i++ {
		indexStr := strconv.Itoa(i)
		value := utils.RandString(10)
		result := testDB.Exec(nil, utils.ToCmdLine("lset", key, indexStr, value))
		if _, ok := result.(*protocol.OkReply); !ok {
			t.Errorf("expected OK, actually %s", string(result.ToBytes()))
		}
		result = testDB.Exec(nil, utils.ToCmdLine("lindex", key, indexStr))
		expected := protocol.MakeBulkReply([]byte(value))
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes()))
		}
	}
	// test negative index
	for i := 1; i <= size; i++ {
		value := utils.RandString(10)
		result := testDB.Exec(nil, utils.ToCmdLine("lset", key, strconv.Itoa(-i), value))
		if _, ok := result.(*protocol.OkReply); !ok {
			t.Errorf("expected OK, actually %s", string(result.ToBytes()))
		}
		result = testDB.Exec(nil, utils.ToCmdLine("lindex", key, strconv.Itoa(len(values)-i-1)))
		expected := protocol.MakeBulkReply([]byte(value))
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes()))
		}
	}

	// test illegal index
	value := utils.RandString(10)
	result := testDB.Exec(nil, utils.ToCmdLine("lset", key, strconv.Itoa(-len(values)-1), value))
	expected := protocol.MakeErrReply("ERR index out of range")
	if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
		t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes()))
	}
	result = testDB.Exec(nil, utils.ToCmdLine("lset", key, strconv.Itoa(len(values)), value))
	if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
		t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes()))
	}
	result = testDB.Exec(nil, utils.ToCmdLine("lset", key, "a", value))
	expected = protocol.MakeErrReply("ERR value is not an integer or out of range")
	if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
		t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes()))
	}
}

func TestLPop(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	values := []string{key, "a", "b", "c", "d", "e", "f"}
	testDB.Exec(nil, utils.ToCmdLine2("rpush", values...))
	size := len(values) - 1

	for i := 0; i < size; i++ {
		result := testDB.Exec(nil, utils.ToCmdLine("lpop", key))
		expected := protocol.MakeBulkReply([]byte(values[i+1]))
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes()))
		}
	}
	result := testDB.Exec(nil, utils.ToCmdLine("rpop", key))
	expected := &protocol.NullBulkReply{}
	if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
		t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes()))
	}
}

func TestRPop(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	values := []string{key, "a", "b", "c", "d", "e", "f"}
	testDB.Exec(nil, utils.ToCmdLine2("rpush", values...))
	size := len(values) - 1

	for i := 0; i < size; i++ {
		result := testDB.Exec(nil, utils.ToCmdLine("rpop", key))
		expected := protocol.MakeBulkReply([]byte(values[len(values)-i-1]))
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes()))
		}
	}
	result := testDB.Exec(nil, utils.ToCmdLine("rpop", key))
	expected := &protocol.NullBulkReply{}
	if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
		t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes()))
	}
}

func TestRPopLPush(t *testing.T) {
	testDB.Flush()
	key1 := utils.RandString(10)
	key2 := utils.RandString(10)
	values := []string{key1, "a", "b", "c", "d", "e", "f"}
	testDB.Exec(nil, utils.ToCmdLine2("rpush", values...))
	size := len(values) - 1

	for i := 0; i < size; i++ {
		result := testDB.Exec(nil, utils.ToCmdLine("rpoplpush", key1, key2))
		expected := protocol.MakeBulkReply([]byte(values[len(values)-i-1]))
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes()))
		}
		result = testDB.Exec(nil, utils.ToCmdLine("lindex", key2, "0"))
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes()))
		}
	}
	result := testDB.Exec(nil, utils.ToCmdLine("rpop", key1))
	expected := &protocol.NullBulkReply{}
	if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
		t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes()))
	}
}

func TestRPushX(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	result := testDB.Exec(nil, utils.ToCmdLine("rpushx", key, "1"))
	expected := protocol.MakeIntReply(int64(0))
	if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
		t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes()))
	}

	testDB.Exec(nil, utils.ToCmdLine("rpush", key, "1"))
	for i := 0; i < 10; i++ {
		value := utils.RandString(10)
		result = testDB.Exec(nil, utils.ToCmdLine("rpushx", key, value))
		expected := protocol.MakeIntReply(int64(i + 2))
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes()))
		}
		result = testDB.Exec(nil, utils.ToCmdLine("lindex", key, "-1"))
		expected2 := protocol.MakeBulkReply([]byte(value))
		if !utils.BytesEquals(result.ToBytes(), expected2.ToBytes()) {
			t.Errorf("expected %s, actually %s", string(expected2.ToBytes()), string(result.ToBytes()))
		}
	}
}

func TestLPushX(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	result := testDB.Exec(nil, utils.ToCmdLine("rpushx", key, "1"))
	expected := protocol.MakeIntReply(int64(0))
	if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
		t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes()))
	}

	testDB.Exec(nil, utils.ToCmdLine("lpush", key, "1"))
	for i := 0; i < 10; i++ {
		value := utils.RandString(10)
		result = testDB.Exec(nil, utils.ToCmdLine("lpushx", key, value))
		expected := protocol.MakeIntReply(int64(i + 2))
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes()))
		}
		result = testDB.Exec(nil, utils.ToCmdLine("lindex", key, "0"))
		expected2 := protocol.MakeBulkReply([]byte(value))
		if !utils.BytesEquals(result.ToBytes(), expected2.ToBytes()) {
			t.Errorf("expected %s, actually %s", string(expected2.ToBytes()), string(result.ToBytes()))
		}
	}
}

func TestLTrim(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	values := []string{"a", "b", "c", "d", "e", "f"}
	result := testDB.Exec(nil, utils.ToCmdLine("rpush", key, "a", "b", "c", "d", "e", "f"))
	asserts.AssertIntReply(t, result, 6)

	// case1
	result1 := testDB.Exec(nil, utils.ToCmdLine("ltrim", key, "1", "-2"))
	asserts.AssertStatusReply(t, result1, "OK")

	actualValue1 := testDB.Exec(nil, utils.ToCmdLine("lrange", key, "0", "-1"))
	asserts.AssertMultiBulkReply(t, actualValue1, values[1:5])

	// case2
	result2 := testDB.Exec(nil, utils.ToCmdLine("ltrim", key, "-3", "-2"))
	asserts.AssertStatusReply(t, result2, "OK")

	actualValue2 := testDB.Exec(nil, utils.ToCmdLine("lrange", key, "0", "-1"))
	asserts.AssertMultiBulkReply(t, actualValue2, values[2:4])

	// case3
	result3 := testDB.Exec(nil, utils.ToCmdLine("ltrim", key, "1", "0"))
	asserts.AssertStatusReply(t, result3, "OK")

	actualValue3 := testDB.Exec(nil, utils.ToCmdLine("lrange", key, "0", "-1"))
	asserts.AssertMultiBulkReplySize(t, actualValue3, 0)
}

func TestLInsert(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	result := testDB.Exec(nil, utils.ToCmdLine("rpush", key, "a", "b", "c", "d", "e", "f"))
	asserts.AssertIntReply(t, result, 6)

	// case1
	result = testDB.Exec(nil, utils.ToCmdLine("linsert", key, "before", "d", "0"))
	asserts.AssertIntReply(t, result, 7)

	values1 := testDB.Exec(nil, utils.ToCmdLine("lrange", key, "0", "-1"))
	asserts.AssertMultiBulkReply(t, values1, []string{"a", "b", "c", "0", "d", "e", "f"})

	// case2
	result = testDB.Exec(nil, utils.ToCmdLine("linsert", key, "after", "d", "1"))
	asserts.AssertIntReply(t, result, 8)

	values2 := testDB.Exec(nil, utils.ToCmdLine("lrange", key, "0", "-1"))
	asserts.AssertMultiBulkReply(t, values2, []string{"a", "b", "c", "0", "d", "1", "e", "f"})

	// case3
	result3 := testDB.Exec(nil, utils.ToCmdLine("linsert", key, "test", "d", "1"))
	asserts.AssertErrReply(t, result3, "ERR syntax error")

	// case4
	result4 := testDB.Exec(nil, utils.ToCmdLine("linsert", key, "test", "d"))
	asserts.AssertErrReply(t, result4, "ERR wrong number of arguments for 'linsert' command")

	// case5
	result5 := testDB.Exec(nil, utils.ToCmdLine("linsert", key, "before", "z", "2"))
	asserts.AssertIntReply(t, result5, -1)
}

func TestUndoLPush(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	cmdLine := utils.ToCmdLine("lpush", key, value)
	testDB.Exec(nil, cmdLine)
	undoCmdLines := undoLPush(testDB, cmdLine[1:])
	testDB.Exec(nil, cmdLine)
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	result := testDB.Exec(nil, utils.ToCmdLine("llen", key))
	asserts.AssertIntReply(t, result, 1)
}

func TestUndoLPop(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("lpush", key, value, value))
	cmdLine := utils.ToCmdLine("lpop", key)
	undoCmdLines := undoLPop(testDB, cmdLine[1:])
	testDB.Exec(nil, cmdLine)
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	result := testDB.Exec(nil, utils.ToCmdLine("llen", key))
	asserts.AssertIntReply(t, result, 2)
}

func TestUndoLSet(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	value2 := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("lpush", key, value, value))
	cmdLine := utils.ToCmdLine("lset", key, "1", value2)
	undoCmdLines := undoLSet(testDB, cmdLine[1:])
	testDB.Exec(nil, cmdLine)
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	result := testDB.Exec(nil, utils.ToCmdLine("lindex", key, "1"))
	asserts.AssertBulkReply(t, result, value)
}

func TestUndoRPop(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("rpush", key, value, value))
	cmdLine := utils.ToCmdLine("rpop", key)
	undoCmdLines := undoRPop(testDB, cmdLine[1:])
	testDB.Exec(nil, cmdLine)
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	result := testDB.Exec(nil, utils.ToCmdLine("llen", key))
	asserts.AssertIntReply(t, result, 2)
}

func TestUndoRPopLPush(t *testing.T) {
	testDB.Flush()
	key1 := utils.RandString(10)
	key2 := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("lpush", key1, value))

	cmdLine := utils.ToCmdLine("rpoplpush", key1, key2)
	undoCmdLines := undoRPopLPush(testDB, cmdLine[1:])
	testDB.Exec(nil, cmdLine)
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	result := testDB.Exec(nil, utils.ToCmdLine("llen", key1))
	asserts.AssertIntReply(t, result, 1)
	result = testDB.Exec(nil, utils.ToCmdLine("llen", key2))
	asserts.AssertIntReply(t, result, 0)
}
