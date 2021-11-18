package database

import (
	"fmt"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/reply"
	"github.com/hdt3213/godis/redis/reply/asserts"
	"strconv"
	"testing"
)

var testDB = makeTestDB()
var testServer = NewStandaloneServer()

func TestSet2(t *testing.T) {
	key := utils.RandString(10)
	value := utils.RandString(10)
	for i := 0; i < 1000; i++ {
		testDB.Exec(nil, utils.ToCmdLine("SET", key, value))
		actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
		expected := reply.MakeBulkReply([]byte(value))
		if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
			t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
		}
	}
}

func TestSet(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)

	// normal set
	testDB.Exec(nil, utils.ToCmdLine("SET", key, value))
	actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
	expected := reply.MakeBulkReply([]byte(value))
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
	}

	// set nx
	actual = testDB.Exec(nil, utils.ToCmdLine("SET", key, value, "NX"))
	if _, ok := actual.(*reply.NullBulkReply); !ok {
		t.Error("expected true actual false")
	}

	testDB.Flush()
	key = utils.RandString(10)
	value = utils.RandString(10)
	actual = testDB.Exec(nil, utils.ToCmdLine("SET", key, value, "NX"))
	actual = testDB.Exec(nil, utils.ToCmdLine("GET", key))
	expected = reply.MakeBulkReply([]byte(value))
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
	}

	// set xx
	testDB.Flush()
	key = utils.RandString(10)
	value = utils.RandString(10)
	actual = testDB.Exec(nil, utils.ToCmdLine("SET", key, value, "XX"))
	if _, ok := actual.(*reply.NullBulkReply); !ok {
		t.Error("expected true actually false ")
	}

	execSet(testDB, utils.ToCmdLine(key, value))
	testDB.Exec(nil, utils.ToCmdLine("SET", key, value))
	actual = testDB.Exec(nil, utils.ToCmdLine("SET", key, value, "XX"))
	actual = testDB.Exec(nil, utils.ToCmdLine("GET", key))
	asserts.AssertBulkReply(t, actual, value)

	// set ex
	testDB.Remove(key)
	ttl := "1000"
	testDB.Exec(nil, utils.ToCmdLine("SET", key, value, "EX", ttl))
	actual = testDB.Exec(nil, utils.ToCmdLine("GET", key))
	asserts.AssertBulkReply(t, actual, value)
	actual = execTTL(testDB, utils.ToCmdLine(key))
	actual = testDB.Exec(nil, utils.ToCmdLine("TTL", key))
	intResult, ok := actual.(*reply.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int reply, actually %s", actual.ToBytes()))
		return
	}
	if intResult.Code <= 0 || intResult.Code > 1000 {
		t.Error(fmt.Sprintf("expected int between [0, 1000], actually %d", intResult.Code))
		return
	}

	// set px
	testDB.Remove(key)
	ttlPx := "1000000"
	testDB.Exec(nil, utils.ToCmdLine("SET", key, value, "PX", ttlPx))
	actual = testDB.Exec(nil, utils.ToCmdLine("GET", key))
	asserts.AssertBulkReply(t, actual, value)
	actual = testDB.Exec(nil, utils.ToCmdLine("TTL", key))
	intResult, ok = actual.(*reply.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int reply, actually %s", actual.ToBytes()))
		return
	}
	if intResult.Code <= 0 || intResult.Code > 1000 {
		t.Error(fmt.Sprintf("expected int between [0, 1000], actually %d", intResult.Code))
		return
	}
}

func TestSetNX(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("SETNX", key, value))
	actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
	expected := reply.MakeBulkReply([]byte(value))
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
	}

	actual = testDB.Exec(nil, utils.ToCmdLine("SETNX", key, value))
	expected2 := reply.MakeIntReply(int64(0))
	if !utils.BytesEquals(actual.ToBytes(), expected2.ToBytes()) {
		t.Error("expected: " + string(expected2.ToBytes()) + ", actual: " + string(actual.ToBytes()))
	}
}

func TestSetEX(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	ttl := "1000"

	testDB.Exec(nil, utils.ToCmdLine("SETEX", key, ttl, value))
	actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
	asserts.AssertBulkReply(t, actual, value)
	actual = testDB.Exec(nil, utils.ToCmdLine("TTL", key))
	intResult, ok := actual.(*reply.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int reply, actually %s", actual.ToBytes()))
		return
	}
	if intResult.Code <= 0 || intResult.Code > 1000 {
		t.Error(fmt.Sprintf("expected int between [0, 1000], actually %d", intResult.Code))
		return
	}
}

func TestPSetEX(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	ttl := "1000000"

	testDB.Exec(nil, utils.ToCmdLine("PSetEx", key, ttl, value))
	actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
	asserts.AssertBulkReply(t, actual, value)
	actual = testDB.Exec(nil, utils.ToCmdLine("PTTL", key))
	intResult, ok := actual.(*reply.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int reply, actually %s", actual.ToBytes()))
		return
	}
	if intResult.Code <= 0 || intResult.Code > 1000000 {
		t.Error(fmt.Sprintf("expected int between [0, 1000], actually %d", intResult.Code))
		return
	}
}

func TestMSet(t *testing.T) {
	testDB.Flush()
	size := 10
	keys := make([]string, size)
	values := make([][]byte, size)
	var args []string
	for i := 0; i < size; i++ {
		keys[i] = utils.RandString(10)
		value := utils.RandString(10)
		values[i] = []byte(value)
		args = append(args, keys[i], value)
	}
	testDB.Exec(nil, utils.ToCmdLine2("MSET", args...))
	actual := testDB.Exec(nil, utils.ToCmdLine2("MGET", keys...))
	expected := reply.MakeMultiBulkReply(values)
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
	}

	// test mget with wrong type
	key1 := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key1, key1))
	key2 := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("LPush", key2, key2))
	actual = testDB.Exec(nil, utils.ToCmdLine2("MGET", key1, key2))
	arr := actual.(*reply.MultiBulkReply)
	if string(arr.Args[0]) != key1 {
		t.Error("expected: " + key1 + ", actual: " + string(arr.Args[1]))
	}
	if len(arr.Args[1]) > 0 {
		t.Error("expect null, actual: " + string(arr.Args[0]))
	}
}

func TestIncr(t *testing.T) {
	testDB.Flush()
	size := 10
	key := utils.RandString(10)
	for i := 0; i < size; i++ {
		testDB.Exec(nil, utils.ToCmdLine("INCR", key))
		actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
		expected := reply.MakeBulkReply([]byte(strconv.FormatInt(int64(i+1), 10)))
		if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
			t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
		}
	}
	for i := 0; i < size; i++ {
		testDB.Exec(nil, utils.ToCmdLine("INCRBY", key, "-1"))
		actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
		expected := reply.MakeBulkReply([]byte(strconv.FormatInt(int64(size-i-1), 10)))
		if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
			t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
		}
	}

	testDB.Flush()
	key = utils.RandString(10)
	for i := 0; i < size; i++ {
		testDB.Exec(nil, utils.ToCmdLine("INCRBY", key, "1"))
		actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
		expected := reply.MakeBulkReply([]byte(strconv.FormatInt(int64(i+1), 10)))
		if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
			t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
		}
	}
	testDB.Remove(key)
	for i := 0; i < size; i++ {
		testDB.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", key, "-1.0"))
		actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
		expected := -i - 1
		bulk, ok := actual.(*reply.BulkReply)
		if !ok {
			t.Error(fmt.Sprintf("expected bulk reply, actually %s", actual.ToBytes()))
			return
		}
		val, err := strconv.ParseFloat(string(bulk.Arg), 10)
		if err != nil {
			t.Error(err)
			return
		}
		if int(val) != expected {
			t.Errorf("expect %d, actual: %d", expected, int(val))
			return
		}
	}
}

func TestDecr(t *testing.T) {
	testDB.Flush()
	size := 10
	key := utils.RandString(10)
	for i := 0; i < size; i++ {
		testDB.Exec(nil, utils.ToCmdLine("DECR", key))
		actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
		asserts.AssertBulkReply(t, actual, strconv.Itoa(-i-1))
	}
	testDB.Remove(key)
	for i := 0; i < size; i++ {
		testDB.Exec(nil, utils.ToCmdLine("DECRBY", key, "1"))
		actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
		expected := -i - 1
		bulk, ok := actual.(*reply.BulkReply)
		if !ok {
			t.Error(fmt.Sprintf("expected bulk reply, actually %s", actual.ToBytes()))
			return
		}
		val, err := strconv.ParseFloat(string(bulk.Arg), 10)
		if err != nil {
			t.Error(err)
			return
		}
		if int(val) != expected {
			t.Errorf("expect %d, actual: %d", expected, int(val))
			return
		}
	}
}

func TestGetSet(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)

	actual := testDB.Exec(nil, utils.ToCmdLine("GETSET", key, value))
	_, ok := actual.(*reply.NullBulkReply)
	if !ok {
		t.Errorf("expect null bulk reply, get: %s", string(actual.ToBytes()))
		return
	}

	value2 := utils.RandString(10)
	actual = testDB.Exec(nil, utils.ToCmdLine("GETSET", key, value2))
	asserts.AssertBulkReply(t, actual, value)
	actual = testDB.Exec(nil, utils.ToCmdLine("GET", key))
	asserts.AssertBulkReply(t, actual, value2)
}

func TestMSetNX(t *testing.T) {
	testDB.Flush()
	size := 10
	args := make([]string, 0, size*2)
	for i := 0; i < size; i++ {
		str := utils.RandString(10)
		args = append(args, str, str)
	}
	result := testDB.Exec(nil, utils.ToCmdLine2("MSETNX", args...))
	asserts.AssertIntReply(t, result, 1)

	result = testDB.Exec(nil, utils.ToCmdLine2("MSETNX", args[0:4]...))
	asserts.AssertIntReply(t, result, 0)
}

func TestStrLen(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("StrLen", key))
	len, ok := actual.(*reply.IntReply)
	if !ok {
		t.Errorf("expect int bulk reply, get: %s", string(actual.ToBytes()))
		return
	}
	asserts.AssertIntReply(t, len, 10)
}

func TestStrLen_KeyNotExist(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)

	actual := testDB.Exec(nil, utils.ToCmdLine("StrLen", key))
	result, ok := actual.(*reply.IntReply)
	if !ok {
		t.Errorf("expect null bulk reply, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertIntReply(t, result, 0)
}

func TestAppend_KeyExist(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	key2 := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("Append", key, key2))
	val, ok := actual.(*reply.IntReply)
	if !ok {
		t.Errorf("expect nil bulk reply, get: %s", string(actual.ToBytes()))
		return
	}
	asserts.AssertIntReply(t, val, len(key)*2)
}

func TestAppend_KeyNotExist(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)

	actual := testDB.Exec(nil, utils.ToCmdLine("Append", key, key))
	val, ok := actual.(*reply.IntReply)
	if !ok {
		t.Errorf("expect nil bulk reply, get: %s", string(actual.ToBytes()))
		return
	}
	asserts.AssertIntReply(t, val, len(key))
}

func TestSetRange_StringExist(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	key2 := utils.RandString(3)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("SetRange", key, fmt.Sprint(0), key2))
	val, ok := actual.(*reply.IntReply)
	if !ok {
		t.Errorf("expect int bulk reply, get: %s", string(actual.ToBytes()))
		return
	}

	result := len(key2 + key[3:])
	asserts.AssertIntReply(t, val, result)
}

func TestSetRange_StringExist_OffsetOutOfLen(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	key2 := utils.RandString(3)
	emptyByteLen := 5
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("SetRange", key, fmt.Sprint(len(key)+emptyByteLen), key2))
	val, ok := actual.(*reply.IntReply)
	if !ok {
		t.Errorf("expect int bulk reply, get: %s", string(actual.ToBytes()))
		return
	}

	result := len(key + string(make([]byte, emptyByteLen)) + key2)
	asserts.AssertIntReply(t, val, result)
}

func TestSetRange_StringNotExist(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)

	actual := testDB.Exec(nil, utils.ToCmdLine("SetRange", key, fmt.Sprint(0), key))
	val, ok := actual.(*reply.IntReply)
	if !ok {
		t.Errorf("expect int bulk reply, get: %s", string(actual.ToBytes()))
		return
	}
	asserts.AssertIntReply(t, val, len(key))
}

func TestGetRange_StringExist(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(0), fmt.Sprint(len(key))))
	val, ok := actual.(*reply.BulkReply)
	if !ok {
		t.Errorf("expect bulk reply, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertBulkReply(t, val, key)
}

func TestGetRange_RangeLargeThenDataLen(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(0), fmt.Sprint(len(key)+2)))
	val, ok := actual.(*reply.BulkReply)
	if !ok {
		t.Errorf("expect bulk reply, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertBulkReply(t, val, key)
}

func TestGetRange_StringNotExist(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(0), fmt.Sprint(len(key))))
	val, ok := actual.(*reply.NullBulkReply)
	if !ok {
		t.Errorf("expect nil bulk reply, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertNullBulk(t, val)
}

func TestGetRange_StringExist_GetPartial(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(0), fmt.Sprint(len(key)/2)))
	val, ok := actual.(*reply.BulkReply)
	if !ok {
		t.Errorf("expect bulk reply, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertBulkReply(t, val, key[:(len(key)/2)+1])
}

func TestGetRange_StringExist_EndIdxOutOfRange(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	emptyByteLen := 2
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(0), fmt.Sprint(len(key)+emptyByteLen)))
	val, ok := actual.(*reply.BulkReply)
	if !ok {
		t.Errorf("expect bulk reply, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertBulkReply(t, val, key)
}

func TestGetRange_StringExist_StartIdxEndIdxAreSame(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	emptyByteLen := 2
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(len(key)+emptyByteLen), fmt.Sprint(len(key)+emptyByteLen)))
	val, ok := actual.(*reply.NullBulkReply)
	if !ok {
		t.Errorf("expect nil bulk reply, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertNullBulk(t, val)
}

func TestGetRange_StringExist_StartIdxGreaterThanEndIdx(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(len(key)+1), fmt.Sprint(len(key))))
	val, ok := actual.(*reply.NullBulkReply)
	if !ok {
		t.Errorf("expect nil bulk reply, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertNullBulk(t, val)
}

func TestGetRange_StringExist_StartIdxEndIdxAreNegative(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(-1*len(key)), fmt.Sprint(-1)))
	val, ok := actual.(*reply.BulkReply)
	if !ok {
		t.Errorf("expect bulk reply, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertBulkReply(t, val, key)
}

func TestGetRange_StringExist_StartIdxNegative(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(-1*len(key)), fmt.Sprint(len(key)/2)))
	val, ok := actual.(*reply.BulkReply)
	if !ok {
		t.Errorf("expect bulk reply, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertBulkReply(t, val, key[0:(len(key)/2)+1])
}

func TestGetRange_StringExist_EndIdxNegative(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(0), fmt.Sprint(-len(key)/2)))
	val, ok := actual.(*reply.BulkReply)
	if !ok {
		t.Errorf("expect bulk reply, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertBulkReply(t, val, key[0:(len(key)/2)+1])
}

func TestGetRange_StringExist_StartIsOutOfRange(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(-len(key)-3), fmt.Sprint(len(key))))
	val, ok := actual.(*reply.NullBulkReply)
	if !ok {
		t.Errorf("expect bulk reply, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertNullBulk(t, val)
}

func TestGetRange_StringExist_EndIdxIsOutOfRange(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(0), fmt.Sprint(-len(key)-3)))
	val, ok := actual.(*reply.NullBulkReply)
	if !ok {
		t.Errorf("expect bulk reply, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertNullBulk(t, val)
}

func TestGetRange_StringExist_StartIdxGreaterThanDataLen(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(len(key)+1), fmt.Sprint(0)))
	val, ok := actual.(*reply.NullBulkReply)
	if !ok {
		t.Errorf("expect bulk reply, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertNullBulk(t, val)
}

func TestGetRange_StringExist_StartIdxIncorrectFormat(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))
	incorrectValue := "incorrect"

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, incorrectValue, fmt.Sprint(0)))
	val, ok := actual.(*reply.StandardErrReply)
	if !ok {
		t.Errorf("expect standart bulk reply, get: %s", string(actual.ToBytes()))
		return
	}

	errorMsg := fmt.Sprintf("strconv.ParseInt: parsing \"%s\": invalid syntax", incorrectValue)
	asserts.AssertErrReply(t, val, errorMsg)
}

func TestGetRange_StringExist_EndIdxIncorrectFormat(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))
	incorrectValue := "incorrect"

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(0), incorrectValue))
	val, ok := actual.(*reply.StandardErrReply)
	if !ok {
		t.Errorf("expect standart bulk reply, get: %s", string(actual.ToBytes()))
		return
	}

	errorMsg := fmt.Sprintf("strconv.ParseInt: parsing \"%s\": invalid syntax", incorrectValue)
	asserts.AssertErrReply(t, val, errorMsg)
}
