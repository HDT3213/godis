package db

import (
    "fmt"
    "github.com/HDT3213/godis/src/datastruct/utils"
    "github.com/HDT3213/godis/src/redis/reply"
    "math/rand"
    "strconv"
    "testing"
)

func TestPush(t *testing.T) {
    FlushAll(testDB, [][]byte{})
    size := 100

    // rpush single
    key := strconv.FormatInt(int64(rand.Int()), 10)
    values := make([][]byte, size)
    for i := 0; i < size; i++ {
        value := strconv.FormatInt(int64(rand.Int()), 10)
        values[i] = []byte(value)
        result := RPush(testDB, toArgs(key, value))
        if intResult, _ := result.(*reply.IntReply); intResult.Code != int64(i+1) {
            t.Error(fmt.Sprintf("expected %d, actually %d", i+1, intResult.Code))
        }
    }
    actual := LRange(testDB, toArgs(key, "0", "-1"))
    expected := reply.MakeMultiBulkReply(values)
    if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
        t.Error("push error")
    }
    Del(testDB, toArgs(key))

    // rpush multi
    key = strconv.FormatInt(int64(rand.Int()), 10)
    values = make([][]byte, size+1)
    values[0] = []byte(key)
    for i := 0; i < size; i++ {
        value := strconv.FormatInt(int64(rand.Int()), 10)
        values[i+1] = []byte(value)
    }
    result := RPush(testDB, values)
    if intResult, _ := result.(*reply.IntReply); intResult.Code != int64(size) {
        t.Error(fmt.Sprintf("expected %d, actually %d", size, intResult.Code))
    }
    actual = LRange(testDB, toArgs(key, "0", "-1"))
    expected = reply.MakeMultiBulkReply(values[1:])
    if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
        t.Error("push error")
    }
    Del(testDB, toArgs(key))

    // left push single
    key = strconv.FormatInt(int64(rand.Int()), 10)
    values = make([][]byte, size)
    for i := 0; i < size; i++ {
        value := strconv.FormatInt(int64(rand.Int()), 10)
        values[size-i-1] = []byte(value)
        result = LPush(testDB, toArgs(key, value))
        if intResult, _ := result.(*reply.IntReply); intResult.Code != int64(i+1) {
            t.Error(fmt.Sprintf("expected %d, actually %d", i+1, intResult.Code))
        }
    }
    actual = LRange(testDB, toArgs(key, "0", "-1"))
    expected = reply.MakeMultiBulkReply(values)
    if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
        t.Error("push error")
    }
    Del(testDB, toArgs(key))

    // left push multi
    key = strconv.FormatInt(int64(rand.Int()), 10)
    values = make([][]byte, size+1)
    values[0] = []byte(key)
    expectedValues := make([][]byte, size)
    for i := 0; i < size; i++ {
        value := strconv.FormatInt(int64(rand.Int()), 10)
        values[i+1] = []byte(value)
        expectedValues[size-i-1] = []byte(value)
    }
    result = LPush(testDB, values)
    if intResult, _ := result.(*reply.IntReply); intResult.Code != int64(size) {
        t.Error(fmt.Sprintf("expected %d, actually %d", size, intResult.Code))
    }
    actual = LRange(testDB, toArgs(key, "0", "-1"))
    expected = reply.MakeMultiBulkReply(expectedValues)
    if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
        t.Error("push error")
    }
    Del(testDB, toArgs(key))
}

func TestLRange(t *testing.T) {
    // prepare list
    FlushAll(testDB, [][]byte{})
    size := 100
    key := strconv.FormatInt(int64(rand.Int()), 10)
    values := make([][]byte, size)
    for i := 0; i < size; i++ {
        value := strconv.FormatInt(int64(rand.Int()), 10)
        RPush(testDB, toArgs(key, value))
        values[i] = []byte(value)
    }

    start := "0"
    end := "9"
    actual := LRange(testDB, toArgs(key, start, end))
    expected := reply.MakeMultiBulkReply(values[0:10])
    if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
        t.Error(fmt.Sprintf("range error [%s, %s]", start, end))
    }

    start = "0"
    end = "200"
    actual = LRange(testDB, toArgs(key, start, end))
    expected = reply.MakeMultiBulkReply(values)
    if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
        t.Error(fmt.Sprintf("range error [%s, %s]", start, end))
    }

    start = "0"
    end = "-10"
    actual = LRange(testDB, toArgs(key, start, end))
    expected = reply.MakeMultiBulkReply(values[0 : size-10+1])
    if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
        t.Error(fmt.Sprintf("range error [%s, %s]", start, end))
    }

    start = "0"
    end = "-200"
    actual = LRange(testDB, toArgs(key, start, end))
    expected = reply.MakeMultiBulkReply(values[0:0])
    if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
        t.Error(fmt.Sprintf("range error [%s, %s]", start, end))
    }

    start = "-10"
    end = "-1"
    actual = LRange(testDB, toArgs(key, start, end))
    expected = reply.MakeMultiBulkReply(values[90:])
    if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
        t.Error(fmt.Sprintf("range error [%s, %s]", start, end))
    }
}

func TestLIndex(t *testing.T) {
    // prepare list
    FlushAll(testDB, [][]byte{})
    size := 100
    key := strconv.FormatInt(int64(rand.Int()), 10)
    values := make([][]byte, size)
    for i := 0; i < size; i++ {
        value := strconv.FormatInt(int64(rand.Int()), 10)
        RPush(testDB, toArgs(key, value))
        values[i] = []byte(value)
    }

    result := LLen(testDB, toArgs(key))
    if intResult, _ := result.(*reply.IntReply); intResult.Code != int64(size) {
        t.Error(fmt.Sprintf("expected %d, actually %d", size, intResult.Code))
    }

    for i := 0; i < size; i++ {
        result = LIndex(testDB, toArgs(key, strconv.Itoa(i)))
        expected := reply.MakeBulkReply(values[i])
        if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
            t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
        }
    }

    for i := 1; i <= size; i++ {
        result = LIndex(testDB, toArgs(key, strconv.Itoa(-i)))
        expected := reply.MakeBulkReply(values[size-i])
        if !utils.BytesEquals(result.ToBytes(), expected.ToBytes()) {
            t.Error(fmt.Sprintf("expected %s, actually %s", string(expected.ToBytes()), string(result.ToBytes())))
        }
    }
}

func TestLRem(t *testing.T) {
    // prepare list
    FlushAll(testDB, [][]byte{})
    key := strconv.FormatInt(int64(rand.Int()), 10)
    values := []string{key, "a", "b", "a", "a", "c", "a", "a"}
    RPush(testDB, toArgs(values...))

    result := LRem(testDB, toArgs(key, "1", "a"))
    if intResult, _ := result.(*reply.IntReply); intResult.Code != 1 {
        t.Error(fmt.Sprintf("expected %d, actually %d", 1, intResult.Code))
    }
    result = LLen(testDB, toArgs(key))
    if intResult, _ := result.(*reply.IntReply); intResult.Code != 6 {
        t.Error(fmt.Sprintf("expected %d, actually %d", 6, intResult.Code))
    }

    result = LRem(testDB, toArgs(key, "-2", "a"))
    if intResult, _ := result.(*reply.IntReply); intResult.Code != 2 {
        t.Error(fmt.Sprintf("expected %d, actually %d", 2, intResult.Code))
    }
    result = LLen(testDB, toArgs(key))
    if intResult, _ := result.(*reply.IntReply); intResult.Code != 4 {
        t.Error(fmt.Sprintf("expected %d, actually %d", 4, intResult.Code))
    }

    result = LRem(testDB, toArgs(key, "0", "a"))
    if intResult, _ := result.(*reply.IntReply); intResult.Code != 2 {
        t.Error(fmt.Sprintf("expected %d, actually %d", 2, intResult.Code))
    }
    result = LLen(testDB, toArgs(key))
    if intResult, _ := result.(*reply.IntReply); intResult.Code != 2 {
        t.Error(fmt.Sprintf("expected %d, actually %d", 2, intResult.Code))
    }
}
