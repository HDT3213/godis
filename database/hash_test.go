package database

import (
	"fmt"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/reply"
	"github.com/hdt3213/godis/redis/reply/asserts"
	"strconv"
	"testing"
)

func TestHSet(t *testing.T) {
	testDB.Flush()
	size := 100

	// test hset
	key := utils.RandString(10)
	values := make(map[string][]byte, size)
	for i := 0; i < size; i++ {
		value := utils.RandString(10)
		field := strconv.Itoa(i)
		values[field] = []byte(value)
		result := testDB.Exec(nil, utils.ToCmdLine("hset", key, field, value))
		if intResult, _ := result.(*reply.IntReply); intResult.Code != int64(1) {
			t.Error(fmt.Sprintf("expected %d, actually %d", 1, intResult.Code))
		}
	}

	// test hget and hexists
	for field, v := range values {
		actual := testDB.Exec(nil, utils.ToCmdLine("hget", key, field))
		expected := reply.MakeBulkReply(v)
		if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
			t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(actual.ToBytes())))
		}
		actual = testDB.Exec(nil, utils.ToCmdLine("hexists", key, field))
		if intResult, _ := actual.(*reply.IntReply); intResult.Code != int64(1) {
			t.Error(fmt.Sprintf("expected %d, actually %d", 1, intResult.Code))
		}
	}

	// test hlen
	actual := testDB.Exec(nil, utils.ToCmdLine("hlen", key))
	if intResult, _ := actual.(*reply.IntReply); intResult.Code != int64(len(values)) {
		t.Error(fmt.Sprintf("expected %d, actually %d", len(values), intResult.Code))
	}
}

func TestHDel(t *testing.T) {
	testDB.Flush()
	size := 100

	// set values
	key := utils.RandString(10)
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		value := utils.RandString(10)
		field := strconv.Itoa(i)
		fields[i] = field
		testDB.Exec(nil, utils.ToCmdLine("hset", key, field, value))
	}

	// test HDel
	args := []string{key}
	args = append(args, fields...)
	actual := testDB.Exec(nil, utils.ToCmdLine2("hdel", args...))
	if intResult, _ := actual.(*reply.IntReply); intResult.Code != int64(len(fields)) {
		t.Error(fmt.Sprintf("expected %d, actually %d", len(fields), intResult.Code))
	}

	actual = testDB.Exec(nil, utils.ToCmdLine("hlen", key))
	if intResult, _ := actual.(*reply.IntReply); intResult.Code != int64(0) {
		t.Error(fmt.Sprintf("expected %d, actually %d", 0, intResult.Code))
	}
}

func TestHMSet(t *testing.T) {
	testDB.Flush()
	size := 100

	// test hset
	key := utils.RandString(10)
	fields := make([]string, size)
	values := make([]string, size)
	setArgs := []string{key}
	for i := 0; i < size; i++ {
		fields[i] = utils.RandString(10)
		values[i] = utils.RandString(10)
		setArgs = append(setArgs, fields[i], values[i])
	}
	result := testDB.Exec(nil, utils.ToCmdLine2("hmset", setArgs...))
	if _, ok := result.(*reply.OkReply); !ok {
		t.Error(fmt.Sprintf("expected ok, actually %s", string(result.ToBytes())))
	}

	// test HMGet
	getArgs := []string{key}
	getArgs = append(getArgs, fields...)
	actual := testDB.Exec(nil, utils.ToCmdLine2("hmget", getArgs...))
	expected := reply.MakeMultiBulkReply(utils.ToCmdLine(values...))
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(actual.ToBytes())))
	}
}

func TestHGetAll(t *testing.T) {
	testDB.Flush()
	size := 100
	key := utils.RandString(10)
	fields := make([]string, size)
	valueSet := make(map[string]bool, size)
	valueMap := make(map[string]string)
	all := make([]string, 0)
	for i := 0; i < size; i++ {
		fields[i] = utils.RandString(10)
		value := utils.RandString(10)
		all = append(all, fields[i], value)
		valueMap[fields[i]] = value
		valueSet[value] = true
		execHSet(testDB, utils.ToCmdLine(key, fields[i], value))
	}

	// test HGetAll
	result := testDB.Exec(nil, utils.ToCmdLine("hgetall", key))
	multiBulk, ok := result.(*reply.MultiBulkReply)
	if !ok {
		t.Error(fmt.Sprintf("expected MultiBulkReply, actually %s", string(result.ToBytes())))
	}
	if 2*len(fields) != len(multiBulk.Args) {
		t.Error(fmt.Sprintf("expected %d items , actually %d ", 2*len(fields), len(multiBulk.Args)))
	}
	for i := range fields {
		field := string(multiBulk.Args[2*i])
		actual := string(multiBulk.Args[2*i+1])
		expected, ok := valueMap[field]
		if !ok {
			t.Error(fmt.Sprintf("unexpected field %s", field))
			continue
		}
		if actual != expected {
			t.Error(fmt.Sprintf("expected %s, actually %s", expected, actual))
		}
	}

	// test HKeys
	result = testDB.Exec(nil, utils.ToCmdLine("hkeys", key))
	multiBulk, ok = result.(*reply.MultiBulkReply)
	if !ok {
		t.Error(fmt.Sprintf("expected MultiBulkReply, actually %s", string(result.ToBytes())))
	}
	if len(fields) != len(multiBulk.Args) {
		t.Error(fmt.Sprintf("expected %d items , actually %d ", len(fields), len(multiBulk.Args)))
	}
	for _, v := range multiBulk.Args {
		field := string(v)
		if _, ok := valueMap[field]; !ok {
			t.Error(fmt.Sprintf("unexpected field %s", field))
		}
	}

	// test HVals
	result = testDB.Exec(nil, utils.ToCmdLine("hvals", key))
	multiBulk, ok = result.(*reply.MultiBulkReply)
	if !ok {
		t.Error(fmt.Sprintf("expected MultiBulkReply, actually %s", string(result.ToBytes())))
	}
	if len(fields) != len(multiBulk.Args) {
		t.Error(fmt.Sprintf("expected %d items , actually %d ", len(fields), len(multiBulk.Args)))
	}
	for _, v := range multiBulk.Args {
		value := string(v)
		_, ok := valueSet[value]
		if !ok {
			t.Error(fmt.Sprintf("unexpected value %s", value))
		}
	}
}

func TestHIncrBy(t *testing.T) {
	testDB.Flush()

	key := utils.RandString(10)
	result := testDB.Exec(nil, utils.ToCmdLine("hincrby", key, "a", "1"))
	if bulkResult, _ := result.(*reply.BulkReply); string(bulkResult.Arg) != "1" {
		t.Error(fmt.Sprintf("expected %s, actually %s", "1", string(bulkResult.Arg)))
	}
	result = testDB.Exec(nil, utils.ToCmdLine("hincrby", key, "a", "1"))
	if bulkResult, _ := result.(*reply.BulkReply); string(bulkResult.Arg) != "2" {
		t.Error(fmt.Sprintf("expected %s, actually %s", "2", string(bulkResult.Arg)))
	}

	result = testDB.Exec(nil, utils.ToCmdLine("hincrbyfloat", key, "b", "1.2"))
	if bulkResult, _ := result.(*reply.BulkReply); string(bulkResult.Arg) != "1.2" {
		t.Error(fmt.Sprintf("expected %s, actually %s", "1.2", string(bulkResult.Arg)))
	}
	result = testDB.Exec(nil, utils.ToCmdLine("hincrbyfloat", key, "b", "1.2"))
	if bulkResult, _ := result.(*reply.BulkReply); string(bulkResult.Arg) != "2.4" {
		t.Error(fmt.Sprintf("expected %s, actually %s", "2.4", string(bulkResult.Arg)))
	}
}

func TestHSetNX(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	field := utils.RandString(10)
	value := utils.RandString(10)
	result := testDB.Exec(nil, utils.ToCmdLine("hsetnx", key, field, value))
	asserts.AssertIntReply(t, result, 1)
	value2 := utils.RandString(10)
	result = testDB.Exec(nil, utils.ToCmdLine("hsetnx", key, field, value2))
	asserts.AssertIntReply(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("hget", key, field))
	asserts.AssertBulkReply(t, result, value)
}

func TestUndoHDel(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	field := utils.RandString(10)
	value := utils.RandString(10)

	testDB.Exec(nil, utils.ToCmdLine("hset", key, field, value))
	cmdLine := utils.ToCmdLine("hdel", key, field)
	undoCmdLines := undoHDel(testDB, cmdLine[1:])
	testDB.Exec(nil, cmdLine)
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	result := testDB.Exec(nil, utils.ToCmdLine("hget", key, field))
	asserts.AssertBulkReply(t, result, value)
}

func TestUndoHSet(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	field := utils.RandString(10)
	value := utils.RandString(10)
	value2 := utils.RandString(10)

	testDB.Exec(nil, utils.ToCmdLine("hset", key, field, value))
	cmdLine := utils.ToCmdLine("hset", key, field, value2)
	undoCmdLines := undoHSet(testDB, cmdLine[1:])
	testDB.Exec(nil, cmdLine)
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	result := testDB.Exec(nil, utils.ToCmdLine("hget", key, field))
	asserts.AssertBulkReply(t, result, value)
}

func TestUndoHMSet(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	field1 := utils.RandString(10)
	field2 := utils.RandString(10)
	value := utils.RandString(10)
	value2 := utils.RandString(10)

	testDB.Exec(nil, utils.ToCmdLine("hmset", key, field1, value, field2, value))
	cmdLine := utils.ToCmdLine("hmset", key, field1, value2, field2, value2)
	undoCmdLines := undoHMSet(testDB, cmdLine[1:])
	testDB.Exec(nil, cmdLine)
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	result := testDB.Exec(nil, utils.ToCmdLine("hget", key, field1))
	asserts.AssertBulkReply(t, result, value)
	result = testDB.Exec(nil, utils.ToCmdLine("hget", key, field2))
	asserts.AssertBulkReply(t, result, value)
}

func TestUndoHIncr(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	field := utils.RandString(10)

	testDB.Exec(nil, utils.ToCmdLine("hset", key, field, "1"))
	cmdLine := utils.ToCmdLine("hinctby", key, field, "2")
	undoCmdLines := undoHIncr(testDB, cmdLine[1:])
	testDB.Exec(nil, cmdLine)
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	result := testDB.Exec(nil, utils.ToCmdLine("hget", key, field))
	asserts.AssertBulkReply(t, result, "1")
}
