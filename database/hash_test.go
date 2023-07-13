package database

import (
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"math"
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
		if intResult, _ := result.(*protocol.IntReply); intResult.Code != int64(1) {
			t.Errorf("expected %d, actually %d", 1, intResult.Code)
		}
	}

	// test hget, hexists and hstrlen
	for field, v := range values {
		actual := testDB.Exec(nil, utils.ToCmdLine("hget", key, field))
		expected := protocol.MakeBulkReply(v)
		if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
			t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(actual.ToBytes()))
		}
		actual = testDB.Exec(nil, utils.ToCmdLine("hexists", key, field))
		if intResult, _ := actual.(*protocol.IntReply); intResult.Code != int64(1) {
			t.Errorf("expected %d, actually %d", 1, intResult.Code)
		}

		actual = testDB.Exec(nil, utils.ToCmdLine("hstrlen", key, field))
		if intResult, _ := actual.(*protocol.IntReply); intResult.Code != int64(len(v)) {
			t.Errorf("expected %d, actually %d", int64(len(v)), intResult.Code)
		}
	}

	// test hlen
	actual := testDB.Exec(nil, utils.ToCmdLine("hlen", key))
	if intResult, _ := actual.(*protocol.IntReply); intResult.Code != int64(len(values)) {
		t.Errorf("expected %d, actually %d", len(values), intResult.Code)
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
	if intResult, _ := actual.(*protocol.IntReply); intResult.Code != int64(len(fields)) {
		t.Errorf("expected %d, actually %d", len(fields), intResult.Code)
	}

	actual = testDB.Exec(nil, utils.ToCmdLine("hlen", key))
	if intResult, _ := actual.(*protocol.IntReply); intResult.Code != int64(0) {
		t.Errorf("expected %d, actually %d", 0, intResult.Code)
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
	if _, ok := result.(*protocol.OkReply); !ok {
		t.Errorf("expected ok, actually %s", string(result.ToBytes()))
	}

	// test HMGet
	getArgs := []string{key}
	getArgs = append(getArgs, fields...)
	actual := testDB.Exec(nil, utils.ToCmdLine2("hmget", getArgs...))
	expected := protocol.MakeMultiBulkReply(utils.ToCmdLine(values...))
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(actual.ToBytes()))
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
	multiBulk, ok := result.(*protocol.MultiBulkReply)
	if !ok {
		t.Errorf("expected MultiBulkReply, actually %s", string(result.ToBytes()))
	}
	if 2*len(fields) != len(multiBulk.Args) {
		t.Errorf("expected %d items , actually %d ", 2*len(fields), len(multiBulk.Args))
	}
	for i := range fields {
		field := string(multiBulk.Args[2*i])
		actual := string(multiBulk.Args[2*i+1])
		expected, ok := valueMap[field]
		if !ok {
			t.Errorf("unexpected field %s", field)
			continue
		}
		if actual != expected {
			t.Errorf("expected %s, actually %s", expected, actual)
		}
	}

	// test HKeys
	result = testDB.Exec(nil, utils.ToCmdLine("hkeys", key))
	multiBulk, ok = result.(*protocol.MultiBulkReply)
	if !ok {
		t.Errorf("expected MultiBulkReply, actually %s", string(result.ToBytes()))
	}
	if len(fields) != len(multiBulk.Args) {
		t.Errorf("expected %d items , actually %d ", len(fields), len(multiBulk.Args))
	}
	for _, v := range multiBulk.Args {
		field := string(v)
		if _, ok := valueMap[field]; !ok {
			t.Errorf("unexpected field %s", field)
		}
	}

	// test HVals
	result = testDB.Exec(nil, utils.ToCmdLine("hvals", key))
	multiBulk, ok = result.(*protocol.MultiBulkReply)
	if !ok {
		t.Errorf("expected MultiBulkReply, actually %s", string(result.ToBytes()))
	}
	if len(fields) != len(multiBulk.Args) {
		t.Errorf("expected %d items , actually %d ", len(fields), len(multiBulk.Args))
	}
	for _, v := range multiBulk.Args {
		value := string(v)
		_, ok := valueSet[value]
		if !ok {
			t.Errorf("unexpected value %s", value)
		}
	}

	// test HRandField
	// test HRandField count of 0 is handled correctly -- "emptyarray"
	result = testDB.Exec(nil, utils.ToCmdLine("hrandfield", key, "0"))
	multiBulk, ok = result.(*protocol.MultiBulkReply)
	if !ok {
		resultBytes := string(result.ToBytes())
		if resultBytes != "*0\r\n" {
			t.Errorf("expected MultiBulkReply, actually %s", resultBytes)
		}
	}

	// test HRandField count > size
	result = testDB.Exec(nil, utils.ToCmdLine("hrandfield", key, strconv.Itoa(size+100)))
	multiBulk, ok = result.(*protocol.MultiBulkReply)
	if !ok {
		t.Errorf("expected MultiBulkReply, actually %s", string(result.ToBytes()))
	}
	if len(fields) != len(multiBulk.Args) {
		t.Errorf("expected %d items , actually %d ", len(fields), len(multiBulk.Args))
	}

	// test HRandField count > size withvalues
	result = testDB.Exec(nil, utils.ToCmdLine("hrandfield", key, strconv.Itoa(size+100), "withvalues"))
	multiBulk, ok = result.(*protocol.MultiBulkReply)
	if !ok {
		t.Errorf("expected MultiBulkReply, actually %s", string(result.ToBytes()))
	}
	if 2*len(fields) != len(multiBulk.Args) {
		t.Errorf("expected %d items , actually %d ", 2*len(fields), len(multiBulk.Args))
	}

	// test HRandField count < size
	result = testDB.Exec(nil, utils.ToCmdLine("hrandfield", key, strconv.Itoa(-size-10)))
	multiBulk, ok = result.(*protocol.MultiBulkReply)
	if !ok {
		t.Errorf("expected MultiBulkReply, actually %s", string(result.ToBytes()))
	}
	if len(fields)+10 != len(multiBulk.Args) {
		t.Errorf("expected %d items , actually %d ", len(fields)+10, len(multiBulk.Args))
	}

	// test HRandField count < size withvalues
	result = testDB.Exec(nil, utils.ToCmdLine("hrandfield", key, strconv.Itoa(-size-10), "withvalues"))
	multiBulk, ok = result.(*protocol.MultiBulkReply)
	if !ok {
		t.Errorf("expected MultiBulkReply, actually %s", string(result.ToBytes()))
	}
	if 2*(len(fields)+10) != len(multiBulk.Args) {
		t.Errorf("expected %d items , actually %d ", 2*(len(fields)+10), len(multiBulk.Args))
	}
}

func TestHIncrBy(t *testing.T) {
	testDB.Flush()

	key := utils.RandString(10)
	result := testDB.Exec(nil, utils.ToCmdLine("hincrby", key, "a", "1"))
	if bulkResult, _ := result.(*protocol.BulkReply); string(bulkResult.Arg) != "1" {
		t.Errorf("expected %s, actually %s", "1", string(bulkResult.Arg))
	}
	result = testDB.Exec(nil, utils.ToCmdLine("hincrby", key, "a", "1"))
	if bulkResult, _ := result.(*protocol.BulkReply); string(bulkResult.Arg) != "2" {
		t.Errorf("expected %s, actually %s", "2", string(bulkResult.Arg))
	}

	result = testDB.Exec(nil, utils.ToCmdLine("hincrbyfloat", key, "b", "1.2"))
	if bulkResult, ok := result.(*protocol.BulkReply); ok {
		val, _ := strconv.ParseFloat(string(bulkResult.Arg), 10)
		if math.Abs(val-1.2) > 1e-4 {
			t.Errorf("expected %s, actually %s", "1.2", string(bulkResult.Arg))
		}
	}
	result = testDB.Exec(nil, utils.ToCmdLine("hincrbyfloat", key, "b", "1.2"))
	if bulkResult, ok := result.(*protocol.BulkReply); ok {
		val, _ := strconv.ParseFloat(string(bulkResult.Arg), 10)
		if math.Abs(val-2.4) > 1e-4 {
			t.Errorf("expected %s, actually %s", "1.2", string(bulkResult.Arg))
		}
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
