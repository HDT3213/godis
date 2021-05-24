package godis

import (
	"fmt"
	"github.com/hdt3213/godis/datastruct/utils"
	utils2 "github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/reply"
	"strconv"
	"testing"
)

func TestPush(t *testing.T) {
	execFlushAll(testDB, [][]byte{})
	size := 100

	// rpush single
	key := utils2.RandString(10)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		value := utils2.RandString(10)
		values[i] = []byte(value)
		result := execRPush(testDB, utils2.ToBytesList(key, value))
		if intResult, _ := result.(*reply.IntReply); intResult.Code != int64(i+1) {
			t.Error(fmt.Sprintf("expected %d, actually %d", i+1, intResult.Code))
		}
	}
	actual := execLRange(testDB, utils2.ToBytesList(key, "0", "-1"))
	expected := reply.MakeMultiBulkReply(values)
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("push error")
	}
	testDB.Remove(key)

	// rpush multi
	key = utils2.RandString(10)
	values = make([][]byte, size+1)
	values[0] = []byte(key)
	for i := 0; i < size; i++ {
		value := utils2.RandString(10)
		values[i+1] = []byte(value)
	}
	result := execRPush(testDB, values)
	if intResult, _ := result.(*reply.IntReply); intResult.Code != int64(size) {
		t.Error(fmt.Sprintf("expected %d, actually %d", size, intResult.Code))
	}
	actual = execLRange(testDB, utils2.ToBytesList(key, "0", "-1"))
	expected = reply.MakeMultiBulkReply(values[1:])
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("push error")
	}
	testDB.Remove(key)

	// left push single
	key = utils2.RandString(10)
	values = make([][]byte, size)
	for i := 0; i < size; i++ {
		value := utils2.RandString(10)
		values[size-i-1] = []byte(value)
		result = execLPush(testDB, utils2.ToBytesList(key, value))
		if intResult, _ := result.(*reply.IntReply); intResult.Code != int64(i+1) {
			t.Error(fmt.Sprintf("expected %d, actually %d", i+1, intResult.Code))
		}
	}
	actual = execLRange(testDB, utils2.ToBytesList(key, "0", "-1"))
	expected = reply.MakeMultiBulkReply(values)
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("push error")
	}
	testDB.Remove(key)

	// left push multi
	key = utils2.RandString(10)
	values = make([][]byte, size+1)
	values[0] = []byte(key)
	expectedValues := make([][]byte, size)
	for i := 0; i < size; i++ {
		value := utils2.RandString(10)
		values[i+1] = []byte(value)
		expectedValues[size-i-1] = []byte(value)
	}
	result = execLPush(testDB, values)
	if intResult, _ := result.(*reply.IntReply); intResult.Code != int64(size) {
		t.Error(fmt.Sprintf("expected %d, actually %d", size, intResult.Code))
	}
	actual = execLRange(testDB, utils2.ToBytesList(key, "0", "-1"))
	expected = reply.MakeMultiBulkReply(expectedValues)
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("push error")
	}
	testDB.Remove(key)
}

func TestLRange(t *testing.T) {
	// prepare list
	execFlushAll(testDB, [][]byte{})
	size := 100
	key := utils2.RandString(10)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		value := utils2.RandString(10)
		execRPush(testDB, utils2.ToBytesList(key, value))
		values[i] = []byte(value)
	}

	start := "0"
	end := "9"
	actual := execLRange(testDB, utils2.ToBytesList(key, start, end))
	expected := reply.MakeMultiBulkReply(values[0:10])
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error(fmt.Sprintf("range error [%s, %s]", start, end))
	}

	start = "0"
	end = "200"
	actual = execLRange(testDB, utils2.ToBytesList(key, start, end))
	expected = reply.MakeMultiBulkReply(values)
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error(fmt.Sprintf("range error [%s, %s]", start, end))
	}

	start = "0"
	end = "-10"
	actual = execLRange(testDB, utils2.ToBytesList(key, start, end))
	expected = reply.MakeMultiBulkReply(values[0 : size-10+1])
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error(fmt.Sprintf("range error [%s, %s]", start, end))
	}

	start = "0"
	end = "-200"
	actual = execLRange(testDB, utils2.ToBytesList(key, start, end))
	expected = reply.MakeMultiBulkReply(values[0:0])
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error(fmt.Sprintf("range error [%s, %s]", start, end))
	}

	start = "-10"
	end = "-1"
	actual = execLRange(testDB, utils2.ToBytesList(key, start, end))
	expected = reply.MakeMultiBulkReply(values[90:])
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error(fmt.Sprintf("range error [%s, %s]", start, end))
	}
}

func TestLIndex(t *testing.T) {
	// prepare list
	execFlushAll(testDB, [][]byte{})
	size := 100
	key := utils2.RandString(10)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		value := utils2.RandString(10)
		execRPush(testDB, utils2.ToBytesList(key, value))
		values[i] = []byte(value)
	}

	result := execLLen(testDB, utils2.ToBytesList(key))
	if intResult, _ := result.(*reply.IntReply); intResult.Code != int64(size) {
		t.Error(fmt.Sprintf("expected %d, actually %d", size, intResult.Code))
	}

	for i := 0; i < size; i++ {
		result = execLIndex(testDB, utils2.ToBytesList(key, strconv.Itoa(i)))
		expected := reply.MakeBulkReply(values[i])
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
		}
	}

	for i := 1; i <= size; i++ {
		result = execLIndex(testDB, utils2.ToBytesList(key, strconv.Itoa(-i)))
		expected := reply.MakeBulkReply(values[size-i])
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
		}
	}
}

func TestLRem(t *testing.T) {
	// prepare list
	execFlushAll(testDB, [][]byte{})
	key := utils2.RandString(10)
	values := []string{key, "a", "b", "a", "a", "c", "a", "a"}
	execRPush(testDB, utils2.ToBytesList(values...))

	result := execLRem(testDB, utils2.ToBytesList(key, "1", "a"))
	if intResult, _ := result.(*reply.IntReply); intResult.Code != 1 {
		t.Error(fmt.Sprintf("expected %d, actually %d", 1, intResult.Code))
	}
	result = execLLen(testDB, utils2.ToBytesList(key))
	if intResult, _ := result.(*reply.IntReply); intResult.Code != 6 {
		t.Error(fmt.Sprintf("expected %d, actually %d", 6, intResult.Code))
	}

	result = execLRem(testDB, utils2.ToBytesList(key, "-2", "a"))
	if intResult, _ := result.(*reply.IntReply); intResult.Code != 2 {
		t.Error(fmt.Sprintf("expected %d, actually %d", 2, intResult.Code))
	}
	result = execLLen(testDB, utils2.ToBytesList(key))
	if intResult, _ := result.(*reply.IntReply); intResult.Code != 4 {
		t.Error(fmt.Sprintf("expected %d, actually %d", 4, intResult.Code))
	}

	result = execLRem(testDB, utils2.ToBytesList(key, "0", "a"))
	if intResult, _ := result.(*reply.IntReply); intResult.Code != 2 {
		t.Error(fmt.Sprintf("expected %d, actually %d", 2, intResult.Code))
	}
	result = execLLen(testDB, utils2.ToBytesList(key))
	if intResult, _ := result.(*reply.IntReply); intResult.Code != 2 {
		t.Error(fmt.Sprintf("expected %d, actually %d", 2, intResult.Code))
	}
}

func TestLSet(t *testing.T) {
	execFlushAll(testDB, [][]byte{})
	key := utils2.RandString(10)
	values := []string{key, "a", "b", "c", "d", "e", "f"}
	execRPush(testDB, utils2.ToBytesList(values...))

	// test positive index
	size := len(values) - 1
	for i := 0; i < size; i++ {
		indexStr := strconv.Itoa(i)
		value := utils2.RandString(10)
		result := execLSet(testDB, utils2.ToBytesList(key, indexStr, value))
		if _, ok := result.(*reply.OkReply); !ok {
			t.Error(fmt.Sprintf("expected OK, actually %s", string(result.ToBytes())))
		}
		result = execLIndex(testDB, utils2.ToBytesList(key, indexStr))
		expected := reply.MakeBulkReply([]byte(value))
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
		}
	}
	// test negative index
	for i := 1; i <= size; i++ {
		value := utils2.RandString(10)
		result := execLSet(testDB, utils2.ToBytesList(key, strconv.Itoa(-i), value))
		if _, ok := result.(*reply.OkReply); !ok {
			t.Error(fmt.Sprintf("expected OK, actually %s", string(result.ToBytes())))
		}
		result = execLIndex(testDB, utils2.ToBytesList(key, strconv.Itoa(len(values)-i-1)))
		expected := reply.MakeBulkReply([]byte(value))
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
		}
	}

	// test illegal index
	value := utils2.RandString(10)
	result := execLSet(testDB, utils2.ToBytesList(key, strconv.Itoa(-len(values)-1), value))
	expected := reply.MakeErrReply("ERR index out of range")
	if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
		t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
	}
	result = execLSet(testDB, utils2.ToBytesList(key, strconv.Itoa(len(values)), value))
	if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
		t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
	}
	result = execLSet(testDB, utils2.ToBytesList(key, "a", value))
	expected = reply.MakeErrReply("ERR value is not an integer or out of range")
	if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
		t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
	}
}

func TestLPop(t *testing.T) {
	execFlushAll(testDB, [][]byte{})
	key := utils2.RandString(10)
	values := []string{key, "a", "b", "c", "d", "e", "f"}
	execRPush(testDB, utils2.ToBytesList(values...))
	size := len(values) - 1

	for i := 0; i < size; i++ {
		result := execLPop(testDB, utils2.ToBytesList(key))
		expected := reply.MakeBulkReply([]byte(values[i+1]))
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
		}
	}
	result := execRPop(testDB, utils2.ToBytesList(key))
	expected := &reply.NullBulkReply{}
	if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
		t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
	}
}

func TestRPop(t *testing.T) {
	execFlushAll(testDB, [][]byte{})
	key := utils2.RandString(10)
	values := []string{key, "a", "b", "c", "d", "e", "f"}
	execRPush(testDB, utils2.ToBytesList(values...))
	size := len(values) - 1

	for i := 0; i < size; i++ {
		result := execRPop(testDB, utils2.ToBytesList(key))
		expected := reply.MakeBulkReply([]byte(values[len(values)-i-1]))
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
		}
	}
	result := execRPop(testDB, utils2.ToBytesList(key))
	expected := &reply.NullBulkReply{}
	if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
		t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
	}
}

func TestRPopLPush(t *testing.T) {
	execFlushAll(testDB, [][]byte{})
	key1 := utils2.RandString(10)
	key2 := utils2.RandString(10)
	values := []string{key1, "a", "b", "c", "d", "e", "f"}
	execRPush(testDB, utils2.ToBytesList(values...))
	size := len(values) - 1

	for i := 0; i < size; i++ {
		result := execRPopLPush(testDB, utils2.ToBytesList(key1, key2))
		expected := reply.MakeBulkReply([]byte(values[len(values)-i-1]))
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
		}
		result = execLIndex(testDB, utils2.ToBytesList(key2, "0"))
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
		}
	}
	result := execRPop(testDB, utils2.ToBytesList(key1))
	expected := &reply.NullBulkReply{}
	if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
		t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
	}
}

func TestRPushX(t *testing.T) {
	execFlushAll(testDB, [][]byte{})
	key := utils2.RandString(10)
	result := execRPushX(testDB, utils2.ToBytesList(key, "1"))
	expected := reply.MakeIntReply(int64(0))
	if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
		t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
	}

	execRPush(testDB, utils2.ToBytesList(key, "1"))
	for i := 0; i < 10; i++ {
		value := utils2.RandString(10)
		result := execRPushX(testDB, utils2.ToBytesList(key, value))
		expected := reply.MakeIntReply(int64(i + 2))
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
		}
		result = execLIndex(testDB, utils2.ToBytesList(key, "-1"))
		expected2 := reply.MakeBulkReply([]byte(value))
		if !utils.BytesEquals(result.ToBytes(), expected2.ToBytes()) {
			t.Error(fmt.Sprintf("expected %s, actually %s", string(expected2.ToBytes()), string(result.ToBytes())))
		}
	}
}

func TestLPushX(t *testing.T) {
	execFlushAll(testDB, [][]byte{})
	key := utils2.RandString(10)
	result := execRPushX(testDB, utils2.ToBytesList(key, "1"))
	expected := reply.MakeIntReply(int64(0))
	if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
		t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
	}

	execLPush(testDB, utils2.ToBytesList(key, "1"))
	for i := 0; i < 10; i++ {
		value := utils2.RandString(10)
		result := execLPushX(testDB, utils2.ToBytesList(key, value))
		expected := reply.MakeIntReply(int64(i + 2))
		if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
			t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
		}
		result = execLIndex(testDB, utils2.ToBytesList(key, "0"))
		expected2 := reply.MakeBulkReply([]byte(value))
		if !utils.BytesEquals(result.ToBytes(), expected2.ToBytes()) {
			t.Error(fmt.Sprintf("expected %s, actually %s", string(expected2.ToBytes()), string(result.ToBytes())))
		}
	}

}
