package db

import (
	"fmt"
	"github.com/hdt3213/godis/datastruct/utils"
	utils2 "github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/reply"
	"github.com/hdt3213/godis/redis/reply/asserts"
	"strconv"
	"testing"
)

func TestHSet(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	size := 100

	// test hset
	key := utils2.RandString(10)
	values := make(map[string][]byte, size)
	for i := 0; i < size; i++ {
		value := utils2.RandString(10)
		field := strconv.Itoa(i)
		values[field] = []byte(value)
		result := HSet(testDB, utils2.ToBytesList(key, field, value))
		if intResult, _ := result.(*reply.IntReply); intResult.Code != int64(1) {
			t.Error(fmt.Sprintf("expected %d, actually %d", 1, intResult.Code))
		}
	}

	// test hget and hexists
	for field, v := range values {
		actual := HGet(testDB, utils2.ToBytesList(key, field))
		expected := reply.MakeBulkReply(v)
		if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
			t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(actual.ToBytes())))
		}
		actual = HExists(testDB, utils2.ToBytesList(key, field))
		if intResult, _ := actual.(*reply.IntReply); intResult.Code != int64(1) {
			t.Error(fmt.Sprintf("expected %d, actually %d", 1, intResult.Code))
		}
	}

	// test hlen
	actual := HLen(testDB, utils2.ToBytesList(key))
	if intResult, _ := actual.(*reply.IntReply); intResult.Code != int64(len(values)) {
		t.Error(fmt.Sprintf("expected %d, actually %d", len(values), intResult.Code))
	}
}

func TestHDel(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	size := 100

	// set values
	key := utils2.RandString(10)
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		value := utils2.RandString(10)
		field := strconv.Itoa(i)
		fields[i] = field
		HSet(testDB, utils2.ToBytesList(key, field, value))
	}

	// test HDel
	args := []string{key}
	args = append(args, fields...)
	actual := HDel(testDB, utils2.ToBytesList(args...))
	if intResult, _ := actual.(*reply.IntReply); intResult.Code != int64(len(fields)) {
		t.Error(fmt.Sprintf("expected %d, actually %d", len(fields), intResult.Code))
	}

	actual = HLen(testDB, utils2.ToBytesList(key))
	if intResult, _ := actual.(*reply.IntReply); intResult.Code != int64(0) {
		t.Error(fmt.Sprintf("expected %d, actually %d", 0, intResult.Code))
	}
}

func TestHMSet(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	size := 100

	// test hset
	key := utils2.RandString(10)
	fields := make([]string, size)
	values := make([]string, size)
	setArgs := []string{key}
	for i := 0; i < size; i++ {
		fields[i] = utils2.RandString(10)
		values[i] = utils2.RandString(10)
		setArgs = append(setArgs, fields[i], values[i])
	}
	result := HMSet(testDB, utils2.ToBytesList(setArgs...))
	if _, ok := result.(*reply.OkReply); !ok {
		t.Error(fmt.Sprintf("expected ok, actually %s", string(result.ToBytes())))
	}

	// test HMGet
	getArgs := []string{key}
	getArgs = append(getArgs, fields...)
	actual := HMGet(testDB, utils2.ToBytesList(getArgs...))
	expected := reply.MakeMultiBulkReply(utils2.ToBytesList(values...))
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(actual.ToBytes())))
	}
}

func TestHGetAll(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	size := 100
	key := utils2.RandString(10)
	fields := make([]string, size)
	valueSet := make(map[string]bool, size)
	valueMap := make(map[string]string)
	all := make([]string, 0)
	for i := 0; i < size; i++ {
		fields[i] = utils2.RandString(10)
		value := utils2.RandString(10)
		all = append(all, fields[i], value)
		valueMap[fields[i]] = value
		valueSet[value] = true
		HSet(testDB, utils2.ToBytesList(key, fields[i], value))
	}

	// test HGetAll
	result := HGetAll(testDB, utils2.ToBytesList(key))
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
	result = HKeys(testDB, utils2.ToBytesList(key))
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
	result = HVals(testDB, utils2.ToBytesList(key))
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
	FlushAll(testDB, [][]byte{})

	key := utils2.RandString(10)
	result := HIncrBy(testDB, utils2.ToBytesList(key, "a", "1"))
	if bulkResult, _ := result.(*reply.BulkReply); string(bulkResult.Arg) != "1" {
		t.Error(fmt.Sprintf("expected %s, actually %s", "1", string(bulkResult.Arg)))
	}
	result = HIncrBy(testDB, utils2.ToBytesList(key, "a", "1"))
	if bulkResult, _ := result.(*reply.BulkReply); string(bulkResult.Arg) != "2" {
		t.Error(fmt.Sprintf("expected %s, actually %s", "2", string(bulkResult.Arg)))
	}

	result = HIncrByFloat(testDB, utils2.ToBytesList(key, "b", "1.2"))
	if bulkResult, _ := result.(*reply.BulkReply); string(bulkResult.Arg) != "1.2" {
		t.Error(fmt.Sprintf("expected %s, actually %s", "1.2", string(bulkResult.Arg)))
	}
	result = HIncrByFloat(testDB, utils2.ToBytesList(key, "b", "1.2"))
	if bulkResult, _ := result.(*reply.BulkReply); string(bulkResult.Arg) != "2.4" {
		t.Error(fmt.Sprintf("expected %s, actually %s", "2.4", string(bulkResult.Arg)))
	}
}

func TestHSetNX(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	key := utils2.RandString(10)
	field := utils2.RandString(10)
	value := utils2.RandString(10)
	result := HSetNX(testDB, utils2.ToBytesList(key, field, value))
	asserts.AssertIntReply(t, result, 1)
	value2 := utils2.RandString(10)
	result = HSetNX(testDB, utils2.ToBytesList(key, field, value2))
	asserts.AssertIntReply(t, result, 0)
	result = HGet(testDB, utils2.ToBytesList(key, field))
	asserts.AssertBulkReply(t, result, value)

}
