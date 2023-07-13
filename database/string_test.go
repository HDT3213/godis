package database

import (
	"fmt"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"math"
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
		expected := protocol.MakeBulkReply([]byte(value))
		if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
			t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
		}
	}
}

func TestSetEmpty(t *testing.T) {
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("SET", key, ""))
	actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
	bulkReply, ok := actual.(*protocol.BulkReply)
	if !ok {
		t.Errorf("expected bulk protocol, actually %s", actual.ToBytes())
		return
	}
	if !(bulkReply.Arg != nil && len(bulkReply.Arg) == 0) {
		t.Error("illegal empty string")
	}
}

func TestSet(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)

	// normal set
	testDB.Exec(nil, utils.ToCmdLine("SET", key, value))
	actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
	expected := protocol.MakeBulkReply([]byte(value))
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
	}

	// set nx
	actual = testDB.Exec(nil, utils.ToCmdLine("SET", key, value, "NX"))
	if _, ok := actual.(*protocol.NullBulkReply); !ok {
		t.Error("expected true actual false")
	}

	testDB.Flush()
	key = utils.RandString(10)
	value = utils.RandString(10)
	actual = testDB.Exec(nil, utils.ToCmdLine("SET", key, value, "NX"))
	actual = testDB.Exec(nil, utils.ToCmdLine("GET", key))
	expected = protocol.MakeBulkReply([]byte(value))
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
	}

	// set xx
	testDB.Flush()
	key = utils.RandString(10)
	value = utils.RandString(10)
	actual = testDB.Exec(nil, utils.ToCmdLine("SET", key, value, "XX"))
	if _, ok := actual.(*protocol.NullBulkReply); !ok {
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
	intResult, ok := actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", actual.ToBytes())
		return
	}
	if intResult.Code <= 0 || intResult.Code > 1000 {
		t.Errorf("expected int between [0, 1000], actually %d", intResult.Code)
		return
	}

	// set px
	testDB.Remove(key)
	ttlPx := "1000000"
	testDB.Exec(nil, utils.ToCmdLine("SET", key, value, "PX", ttlPx))
	actual = testDB.Exec(nil, utils.ToCmdLine("GET", key))
	asserts.AssertBulkReply(t, actual, value)
	actual = testDB.Exec(nil, utils.ToCmdLine("TTL", key))
	intResult, ok = actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", actual.ToBytes())
		return
	}
	if intResult.Code <= 0 || intResult.Code > 1000 {
		t.Errorf("expected int between [0, 1000], actually %d", intResult.Code)
		return
	}
}

func TestSetNX(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("SETNX", key, value))
	actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
	expected := protocol.MakeBulkReply([]byte(value))
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
	}

	actual = testDB.Exec(nil, utils.ToCmdLine("SETNX", key, value))
	expected2 := protocol.MakeIntReply(int64(0))
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
	intResult, ok := actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", actual.ToBytes())
		return
	}
	if intResult.Code <= 0 || intResult.Code > 1000 {
		t.Errorf("expected int between [0, 1000], actually %d", intResult.Code)
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
	intResult, ok := actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", actual.ToBytes())
		return
	}
	if intResult.Code <= 0 || intResult.Code > 1000000 {
		t.Errorf("expected int between [0, 1000], actually %d", intResult.Code)
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
	expected := protocol.MakeMultiBulkReply(values)
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
	}

	// test mget with wrong type
	key1 := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key1, key1))
	key2 := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("LPush", key2, key2))
	actual = testDB.Exec(nil, utils.ToCmdLine2("MGET", key1, key2))
	arr := actual.(*protocol.MultiBulkReply)
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
		expected := protocol.MakeBulkReply([]byte(strconv.FormatInt(int64(i+1), 10)))
		if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
			t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
		}
	}
	for i := 0; i < size; i++ {
		testDB.Exec(nil, utils.ToCmdLine("INCRBY", key, "-1"))
		actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
		expected := protocol.MakeBulkReply([]byte(strconv.FormatInt(int64(size-i-1), 10)))
		if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
			t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
		}
	}

	testDB.Flush()
	key = utils.RandString(10)
	for i := 0; i < size; i++ {
		testDB.Exec(nil, utils.ToCmdLine("INCRBY", key, "1"))
		actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
		expected := protocol.MakeBulkReply([]byte(strconv.FormatInt(int64(i+1), 10)))
		if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
			t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
		}
	}
	testDB.Remove(key)
	for i := 0; i < size; i++ {
		testDB.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", key, "-1.0"))
		actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
		expected := -i - 1
		bulk, ok := actual.(*protocol.BulkReply)
		if !ok {
			t.Errorf("expected bulk protocol, actually %s", actual.ToBytes())
			return
		}
		val, err := strconv.ParseFloat(string(bulk.Arg), 10)
		if err != nil {
			t.Error(err)
			return
		}
		if math.Abs(val-float64(expected)) > 1e-4 {
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
		bulk, ok := actual.(*protocol.BulkReply)
		if !ok {
			t.Errorf("expected bulk protocol, actually %s", actual.ToBytes())
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

func TestGetEX(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	ttl := "1000"

	testDB.Exec(nil, utils.ToCmdLine("SET", key, value))

	// Normal Get
	actual := testDB.Exec(nil, utils.ToCmdLine("GETEX", key))
	asserts.AssertBulkReply(t, actual, value)

	// Test GetEX Key EX Seconds
	actual = testDB.Exec(nil, utils.ToCmdLine("GETEX", key, "EX", ttl))
	asserts.AssertBulkReply(t, actual, value)
	actual = testDB.Exec(nil, utils.ToCmdLine("TTL", key))
	intResult, ok := actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", actual.ToBytes())
		return
	}
	if intResult.Code <= 0 || intResult.Code > 1000 {
		t.Errorf("expected int between [0, 1000], actually %d", intResult.Code)
		return
	}

	// Test GetEX Key Persist
	actual = testDB.Exec(nil, utils.ToCmdLine("GETEX", key, "PERSIST"))
	asserts.AssertBulkReply(t, actual, value)
	actual = testDB.Exec(nil, utils.ToCmdLine("TTL", key))
	intResult, ok = actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", actual.ToBytes())
		return
	}
	if intResult.Code != -1 {
		t.Errorf("expected int equals -1, actually %d", intResult.Code)
		return
	}

	// Test GetEX Key NX Milliseconds
	ttl = "1000000"
	actual = testDB.Exec(nil, utils.ToCmdLine("GETEX", key, "PX", ttl))
	asserts.AssertBulkReply(t, actual, value)
	actual = testDB.Exec(nil, utils.ToCmdLine("TTL", key))
	intResult, ok = actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", actual.ToBytes())
		return
	}
	if intResult.Code <= 0 || intResult.Code > 1000000 {
		t.Errorf("expected int between [0, 1000000], actually %d", intResult.Code)
		return
	}
}

func TestGetSet(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)

	actual := testDB.Exec(nil, utils.ToCmdLine("GETSET", key, value))
	_, ok := actual.(*protocol.NullBulkReply)
	if !ok {
		t.Errorf("expect null bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}

	value2 := utils.RandString(10)
	actual = testDB.Exec(nil, utils.ToCmdLine("GETSET", key, value2))
	asserts.AssertBulkReply(t, actual, value)
	actual = testDB.Exec(nil, utils.ToCmdLine("GET", key))
	asserts.AssertBulkReply(t, actual, value2)

	// Test GetDel
	actual = testDB.Exec(nil, utils.ToCmdLine("GETDEL", key))
	asserts.AssertBulkReply(t, actual, value2)

	actual = testDB.Exec(nil, utils.ToCmdLine("GETDEL", key))
	_, ok = actual.(*protocol.NullBulkReply)
	if !ok {
		t.Errorf("expect null bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}
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
	size, ok := actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expect int bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}
	asserts.AssertIntReply(t, size, 10)
}

func TestStrLen_KeyNotExist(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)

	actual := testDB.Exec(nil, utils.ToCmdLine("StrLen", key))
	result, ok := actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expect null bulk protocol, get: %s", string(actual.ToBytes()))
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
	val, ok := actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expect nil bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}
	asserts.AssertIntReply(t, val, len(key)*2)
}

func TestAppend_KeyNotExist(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)

	actual := testDB.Exec(nil, utils.ToCmdLine("Append", key, key))
	val, ok := actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expect nil bulk protocol, get: %s", string(actual.ToBytes()))
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
	val, ok := actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expect int bulk protocol, get: %s", string(actual.ToBytes()))
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
	val, ok := actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expect int bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}

	result := len(key + string(make([]byte, emptyByteLen)) + key2)
	asserts.AssertIntReply(t, val, result)
}

func TestSetRange_StringNotExist(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)

	actual := testDB.Exec(nil, utils.ToCmdLine("SetRange", key, fmt.Sprint(0), key))
	val, ok := actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expect int bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}
	asserts.AssertIntReply(t, val, len(key))
}

func TestGetRange_StringExist(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(0), fmt.Sprint(len(key))))
	val, ok := actual.(*protocol.BulkReply)
	if !ok {
		t.Errorf("expect bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertBulkReply(t, val, key)
}

func TestGetRange_RangeLargeThenDataLen(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(0), fmt.Sprint(len(key)+2)))
	val, ok := actual.(*protocol.BulkReply)
	if !ok {
		t.Errorf("expect bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertBulkReply(t, val, key)
}

func TestGetRange_StringNotExist(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(0), fmt.Sprint(len(key))))
	val, ok := actual.(*protocol.NullBulkReply)
	if !ok {
		t.Errorf("expect nil bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertNullBulk(t, val)
}

func TestGetRange_StringExist_GetPartial(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(0), fmt.Sprint(len(key)/2)))
	val, ok := actual.(*protocol.BulkReply)
	if !ok {
		t.Errorf("expect bulk protocol, get: %s", string(actual.ToBytes()))
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
	val, ok := actual.(*protocol.BulkReply)
	if !ok {
		t.Errorf("expect bulk protocol, get: %s", string(actual.ToBytes()))
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
	val, ok := actual.(*protocol.NullBulkReply)
	if !ok {
		t.Errorf("expect nil bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertNullBulk(t, val)
}

func TestGetRange_StringExist_StartIdxGreaterThanEndIdx(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(len(key)+1), fmt.Sprint(len(key))))
	val, ok := actual.(*protocol.NullBulkReply)
	if !ok {
		t.Errorf("expect nil bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertNullBulk(t, val)
}

func TestGetRange_StringExist_StartIdxEndIdxAreNegative(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(-1*len(key)), fmt.Sprint(-1)))
	val, ok := actual.(*protocol.BulkReply)
	if !ok {
		t.Errorf("expect bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertBulkReply(t, val, key)
}

func TestGetRange_StringExist_StartIdxNegative(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(-1*len(key)), fmt.Sprint(len(key)/2)))
	val, ok := actual.(*protocol.BulkReply)
	if !ok {
		t.Errorf("expect bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertBulkReply(t, val, key[0:(len(key)/2)+1])
}

func TestGetRange_StringExist_EndIdxNegative(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(0), fmt.Sprint(-len(key)/2)))
	val, ok := actual.(*protocol.BulkReply)
	if !ok {
		t.Errorf("expect bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertBulkReply(t, val, key[0:(len(key)/2)+1])
}

func TestGetRange_StringExist_StartIsOutOfRange(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(-len(key)-3), fmt.Sprint(len(key))))
	val, ok := actual.(*protocol.NullBulkReply)
	if !ok {
		t.Errorf("expect bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertNullBulk(t, val)
}

func TestGetRange_StringExist_EndIdxIsOutOfRange(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(0), fmt.Sprint(-len(key)-3)))
	val, ok := actual.(*protocol.NullBulkReply)
	if !ok {
		t.Errorf("expect bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}

	asserts.AssertNullBulk(t, val)
}

func TestGetRange_StringExist_StartIdxGreaterThanDataLen(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(len(key)+1), fmt.Sprint(0)))
	val, ok := actual.(*protocol.NullBulkReply)
	if !ok {
		t.Errorf("expect bulk protocol, get: %s", string(actual.ToBytes()))
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
	asserts.AssertErrReply(t, actual, "ERR value is not an integer or out of range")
}

func TestGetRange_StringExist_EndIdxIncorrectFormat(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))
	incorrectValue := "incorrect"

	actual := testDB.Exec(nil, utils.ToCmdLine("GetRange", key, fmt.Sprint(0), incorrectValue))
	asserts.AssertErrReply(t, actual, "ERR value is not an integer or out of range")
}

func TestSetBit(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	actual := testDB.Exec(nil, utils.ToCmdLine("SetBit", key, "15", "1"))
	asserts.AssertIntReply(t, actual, 0)
	actual = testDB.Exec(nil, utils.ToCmdLine("SetBit", key, "15", "0"))
	asserts.AssertIntReply(t, actual, 1)
	testDB.Exec(nil, utils.ToCmdLine("SetBit", key, "13", "1"))
	actual = testDB.Exec(nil, utils.ToCmdLine("GetBit", key, "13"))
	asserts.AssertIntReply(t, actual, 1)
	actual = testDB.Exec(nil, utils.ToCmdLine("GetBit", key+"1", "13")) // test not exist key
	asserts.AssertIntReply(t, actual, 0)

	actual = testDB.Exec(nil, utils.ToCmdLine("SetBit", key, "13", "a"))
	asserts.AssertErrReply(t, actual, "ERR bit is not an integer or out of range")
	actual = testDB.Exec(nil, utils.ToCmdLine("SetBit", key, "a", "1"))
	asserts.AssertErrReply(t, actual, "ERR bit offset is not an integer or out of range")
	actual = testDB.Exec(nil, utils.ToCmdLine("GetBit", key, "a"))
	asserts.AssertErrReply(t, actual, "ERR bit offset is not an integer or out of range")

	key2 := utils.RandString(11)
	testDB.Exec(nil, utils.ToCmdLine("rpush", key2, "1"))
	actual = testDB.Exec(nil, utils.ToCmdLine("SetBit", key2, "15", "0"))
	asserts.AssertErrReply(t, actual, "WRONGTYPE Operation against a key holding the wrong kind of value")
	actual = testDB.Exec(nil, utils.ToCmdLine("GetBit", key2, "15"))
	asserts.AssertErrReply(t, actual, "WRONGTYPE Operation against a key holding the wrong kind of value")
}

func TestBitCount(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("SetBit", key, "15", "1"))
	testDB.Exec(nil, utils.ToCmdLine("SetBit", key, "13", "1"))
	actual := testDB.Exec(nil, utils.ToCmdLine("BitCount", key))
	asserts.AssertIntReply(t, actual, 2)
	actual = testDB.Exec(nil, utils.ToCmdLine("BitCount", key, "14", "15", "BIT"))
	asserts.AssertIntReply(t, actual, 1)
	actual = testDB.Exec(nil, utils.ToCmdLine("BitCount", key, "16", "20", "BIT"))
	asserts.AssertIntReply(t, actual, 0)
	actual = testDB.Exec(nil, utils.ToCmdLine("BitCount", key, "1", "1", "BYTE"))
	asserts.AssertIntReply(t, actual, 2)

	key2 := utils.RandString(11)
	testDB.Exec(nil, utils.ToCmdLine("rpush", key2, "1"))
	actual = testDB.Exec(nil, utils.ToCmdLine("BitCount", key2))
	asserts.AssertErrReply(t, actual, "WRONGTYPE Operation against a key holding the wrong kind of value")
	actual = testDB.Exec(nil, utils.ToCmdLine("BitCount", key+"a"))
	asserts.AssertIntReply(t, actual, 0)
	actual = testDB.Exec(nil, utils.ToCmdLine("BitCount", key, "14", "15", "B"))
	asserts.AssertErrReply(t, actual, "ERR syntax error")
	actual = testDB.Exec(nil, utils.ToCmdLine("BitCount", key, "14", "A"))
	asserts.AssertErrReply(t, actual, "ERR value is not an integer or out of range")
	actual = testDB.Exec(nil, utils.ToCmdLine("BitCount", key, "A", "-1"))
	asserts.AssertErrReply(t, actual, "ERR value is not an integer or out of range")
}

func TestBitPos(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("SetBit", key, "15", "1"))
	actual := testDB.Exec(nil, utils.ToCmdLine("BitPos", key, "0"))
	asserts.AssertIntReply(t, actual, 0)
	actual = testDB.Exec(nil, utils.ToCmdLine("BitPos", key, "1"))
	asserts.AssertIntReply(t, actual, 15)
	actual = testDB.Exec(nil, utils.ToCmdLine("BitPos", key, "1", "0", "-1", "BIT"))
	asserts.AssertIntReply(t, actual, 15)
	actual = testDB.Exec(nil, utils.ToCmdLine("BitPos", key, "1", "1", "1", "BYTE"))
	asserts.AssertIntReply(t, actual, 15)
	actual = testDB.Exec(nil, utils.ToCmdLine("BitPos", key, "0", "1", "1", "BYTE"))
	asserts.AssertIntReply(t, actual, 8)

	key2 := utils.RandString(12)
	testDB.Exec(nil, utils.ToCmdLine("rpush", key2, "1"))
	actual = testDB.Exec(nil, utils.ToCmdLine("BitPos", key2, "1"))
	asserts.AssertErrReply(t, actual, "WRONGTYPE Operation against a key holding the wrong kind of value")
	actual = testDB.Exec(nil, utils.ToCmdLine("BitPos", key+"a", "1"))
	asserts.AssertIntReply(t, actual, -1)
	actual = testDB.Exec(nil, utils.ToCmdLine("BitPos", key, "1", "1", "15", "B"))
	asserts.AssertErrReply(t, actual, "ERR syntax error")
	actual = testDB.Exec(nil, utils.ToCmdLine("BitPos", key, "1", "14", "A"))
	asserts.AssertErrReply(t, actual, "ERR value is not an integer or out of range")
	actual = testDB.Exec(nil, utils.ToCmdLine("BitPos", key, "1", "a", "14"))
	asserts.AssertErrReply(t, actual, "ERR value is not an integer or out of range")
	actual = testDB.Exec(nil, utils.ToCmdLine("BitPos", key, "-1"))
	asserts.AssertErrReply(t, actual, "ERR bit is not an integer or out of range")
}

func TestRandomkey(t *testing.T) {
	testDB.Flush()
	for i := 0; i < 10; i++ {
		key := utils.RandString(10)
		testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))
	}
	actual := testDB.Exec(nil, utils.ToCmdLine("Randomkey"))
	asserts.AssertNotError(t, actual)
}
