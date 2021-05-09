package godis

import (
	"fmt"
	"github.com/hdt3213/godis/datastruct/utils"
	utils2 "github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/reply"
	"github.com/hdt3213/godis/redis/reply/asserts"
	"strconv"
	"testing"
)

var testDB = makeTestDB()

func TestSet(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	key := utils2.RandString(10)
	value := utils2.RandString(10)

	// normal set
	Set(testDB, utils2.ToBytesList(key, value))
	actual := Get(testDB, utils2.ToBytesList(key))
	expected := reply.MakeBulkReply([]byte(value))
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
	}

	// set nx
	actual = Set(testDB, utils2.ToBytesList(key, value, "NX"))
	if _, ok := actual.(*reply.NullBulkReply); !ok {
		t.Error("expected true actual false")
	}

	FlushAll(testDB, [][]byte{})
	key = utils2.RandString(10)
	value = utils2.RandString(10)
	Set(testDB, utils2.ToBytesList(key, value, "NX"))
	actual = Get(testDB, utils2.ToBytesList(key))
	expected = reply.MakeBulkReply([]byte(value))
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
	}

	// set xx
	FlushAll(testDB, [][]byte{})
	key = utils2.RandString(10)
	value = utils2.RandString(10)
	actual = Set(testDB, utils2.ToBytesList(key, value, "XX"))
	if _, ok := actual.(*reply.NullBulkReply); !ok {
		t.Error("expected true actually false ")
	}

	Set(testDB, utils2.ToBytesList(key, value))
	Set(testDB, utils2.ToBytesList(key, value, "XX"))
	actual = Get(testDB, utils2.ToBytesList(key))
	asserts.AssertBulkReply(t, actual, value)

	// set ex
	Del(testDB, utils2.ToBytesList(key))
	ttl := "1000"
	Set(testDB, utils2.ToBytesList(key, value, "EX", ttl))
	actual = Get(testDB, utils2.ToBytesList(key))
	asserts.AssertBulkReply(t, actual, value)
	actual = TTL(testDB, utils2.ToBytesList(key))
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
	Del(testDB, utils2.ToBytesList(key))
	ttlPx := "1000000"
	Set(testDB, utils2.ToBytesList(key, value, "PX", ttlPx))
	actual = Get(testDB, utils2.ToBytesList(key))
	asserts.AssertBulkReply(t, actual, value)
	actual = TTL(testDB, utils2.ToBytesList(key))
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
	FlushAll(testDB, [][]byte{})
	key := utils2.RandString(10)
	value := utils2.RandString(10)
	SetNX(testDB, utils2.ToBytesList(key, value))
	actual := Get(testDB, utils2.ToBytesList(key))
	expected := reply.MakeBulkReply([]byte(value))
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
	}

	actual = SetNX(testDB, utils2.ToBytesList(key, value))
	expected2 := reply.MakeIntReply(int64(0))
	if !utils.BytesEquals(actual.ToBytes(), expected2.ToBytes()) {
		t.Error("expected: " + string(expected2.ToBytes()) + ", actual: " + string(actual.ToBytes()))
	}
}

func TestSetEX(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	key := utils2.RandString(10)
	value := utils2.RandString(10)
	ttl := "1000"

	SetEX(testDB, utils2.ToBytesList(key, ttl, value))
	actual := Get(testDB, utils2.ToBytesList(key))
	asserts.AssertBulkReply(t, actual, value)
	actual = TTL(testDB, utils2.ToBytesList(key))
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
	FlushAll(testDB, [][]byte{})
	key := utils2.RandString(10)
	value := utils2.RandString(10)
	ttl := "1000000"

	PSetEX(testDB, utils2.ToBytesList(key, ttl, value))
	actual := Get(testDB, utils2.ToBytesList(key))
	asserts.AssertBulkReply(t, actual, value)
	actual = PTTL(testDB, utils2.ToBytesList(key))
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
	FlushAll(testDB, [][]byte{})
	size := 10
	keys := make([]string, size)
	values := make([][]byte, size)
	args := make([]string, 0, size*2)
	for i := 0; i < size; i++ {
		keys[i] = utils2.RandString(10)
		value := utils2.RandString(10)
		values[i] = []byte(value)
		args = append(args, keys[i], value)
	}
	MSet(testDB, utils2.ToBytesList(args...))
	actual := MGet(testDB, utils2.ToBytesList(keys...))
	expected := reply.MakeMultiBulkReply(values)
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
	}
}

func TestIncr(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	size := 10
	key := utils2.RandString(10)
	for i := 0; i < size; i++ {
		Incr(testDB, utils2.ToBytesList(key))
		actual := Get(testDB, utils2.ToBytesList(key))
		expected := reply.MakeBulkReply([]byte(strconv.FormatInt(int64(i+1), 10)))
		if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
			t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
		}
	}
	for i := 0; i < size; i++ {
		IncrBy(testDB, utils2.ToBytesList(key, "-1"))
		actual := Get(testDB, utils2.ToBytesList(key))
		expected := reply.MakeBulkReply([]byte(strconv.FormatInt(int64(size-i-1), 10)))
		if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
			t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
		}
	}

	FlushAll(testDB, [][]byte{})
	key = utils2.RandString(10)
	for i := 0; i < size; i++ {
		IncrBy(testDB, utils2.ToBytesList(key, "1"))
		actual := Get(testDB, utils2.ToBytesList(key))
		expected := reply.MakeBulkReply([]byte(strconv.FormatInt(int64(i+1), 10)))
		if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
			t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
		}
	}
	Del(testDB, utils2.ToBytesList(key))
	for i := 0; i < size; i++ {
		IncrByFloat(testDB, utils2.ToBytesList(key, "-1.0"))
		actual := Get(testDB, utils2.ToBytesList(key))
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
	FlushAll(testDB, [][]byte{})
	size := 10
	key := utils2.RandString(10)
	for i := 0; i < size; i++ {
		Decr(testDB, utils2.ToBytesList(key))
		actual := Get(testDB, utils2.ToBytesList(key))
		asserts.AssertBulkReply(t, actual, strconv.Itoa(-i-1))
	}
	Del(testDB, utils2.ToBytesList(key))
	for i := 0; i < size; i++ {
		DecrBy(testDB, utils2.ToBytesList(key, "1"))
		actual := Get(testDB, utils2.ToBytesList(key))
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
	FlushAll(testDB, [][]byte{})
	key := utils2.RandString(10)
	value := utils2.RandString(10)

	result := GetSet(testDB, utils2.ToBytesList(key, value))
	_, ok := result.(*reply.NullBulkReply)
	if !ok {
		t.Errorf("expect null bulk reply, get: %s", string(result.ToBytes()))
		return
	}

	value2 := utils2.RandString(10)
	result = GetSet(testDB, utils2.ToBytesList(key, value2))
	asserts.AssertBulkReply(t, result, value)
	result = Get(testDB, utils2.ToBytesList(key))
	asserts.AssertBulkReply(t, result, value2)
}

func TestMSetNX(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	size := 10
	args := make([]string, 0, size*2)
	for i := 0; i < size; i++ {
		str := utils2.RandString(10)
		args = append(args, str, str)
	}
	result := MSetNX(testDB, utils2.ToBytesList(args...))
	asserts.AssertIntReply(t, result, 1)

	result = MSetNX(testDB, utils2.ToBytesList(args[0:4]...))
	asserts.AssertIntReply(t, result, 0)
}
