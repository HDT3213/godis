package db

import (
    "github.com/HDT3213/godis/src/datastruct/utils"
    "github.com/HDT3213/godis/src/redis/reply"
    "math/rand"
    "strconv"
    "testing"
)

var testDB = makeTestDB()

func TestSet(t *testing.T) {
    FlushAll(testDB, [][]byte{})
    key := strconv.FormatInt(int64(rand.Int()), 10)
    value := strconv.FormatInt(int64(rand.Int()), 10)

    // normal set
    Set(testDB, toArgs(key, value))
    actual := Get(testDB, toArgs(key))
    expected := reply.MakeBulkReply([]byte(value))
    if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
        t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
    }

    // set nx
    actual = Set(testDB, toArgs(key, value, "NX"))
    if _, ok := actual.(*reply.NullBulkReply); !ok {
        t.Error("expected true actual false")
    }

    FlushAll(testDB, [][]byte{})
    key = strconv.FormatInt(int64(rand.Int()), 10)
    value = strconv.FormatInt(int64(rand.Int()), 10)
    Set(testDB, toArgs(key, value, "NX"))
    actual = Get(testDB, toArgs(key))
    expected = reply.MakeBulkReply([]byte(value))
    if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
        t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
    }

    // set xx
    FlushAll(testDB, [][]byte{})
    key = strconv.FormatInt(int64(rand.Int()), 10)
    value = strconv.FormatInt(int64(rand.Int()), 10)
    actual = Set(testDB, toArgs(key, value, "XX"))
    if _, ok := actual.(*reply.NullBulkReply); !ok {
        t.Error("expected true actually false ")
    }

    Set(testDB, toArgs(key, value))
    Set(testDB, toArgs(key, value, "XX"))
    actual = Get(testDB, toArgs(key))
    expected = reply.MakeBulkReply([]byte(value))
    if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
        t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
    }

}

func TestSetNX(t *testing.T) {
    FlushAll(testDB, [][]byte{})
    key := strconv.FormatInt(int64(rand.Int()), 10)
    value := strconv.FormatInt(int64(rand.Int()), 10)
    SetNX(testDB, toArgs(key, value))
    actual := Get(testDB, toArgs(key))
    expected := reply.MakeBulkReply([]byte(value))
    if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
        t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
    }

    actual = SetNX(testDB, toArgs(key, value))
    expected2 := reply.MakeIntReply(int64(0))
    if !utils.BytesEquals(actual.ToBytes(), expected2.ToBytes()) {
        t.Error("expected: " + string(expected2.ToBytes()) + ", actual: " + string(actual.ToBytes()))
    }
}

func TestSetEX(t *testing.T) {
    FlushAll(testDB, [][]byte{})
    key := strconv.FormatInt(int64(rand.Int()), 10)
    value := strconv.FormatInt(int64(rand.Int()), 10)
    ttl := "1000"

    SetEX(testDB, toArgs(key, ttl, value))
    actual := Get(testDB, toArgs(key))
    expected2 := reply.MakeBulkReply([]byte(value))
    if !utils.BytesEquals(actual.ToBytes(), expected2.ToBytes()) {
        t.Error("expected: " + string(expected2.ToBytes()) + ", actual: " + string(actual.ToBytes()))
    }
}

func TestMSet(t *testing.T) {
    FlushAll(testDB, [][]byte{})
    size := 10
    keys := make([]string, size)
    values := make([][]byte, size)
    args := make([]string, size*2)[0:0]
    for i := 0; i < size; i++ {
        keys[i] = strconv.FormatInt(int64(rand.Int()), 10)
        value := strconv.FormatInt(int64(rand.Int()), 10)
        values[i] = []byte(value)
        args = append(args, keys[i], value)
    }
    MSet(testDB, toArgs(args...))
    actual := MGet(testDB, toArgs(keys...))
    expected := reply.MakeMultiBulkReply(values)
    if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
        t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
    }
}

func TestIncr(t *testing.T) {
    FlushAll(testDB, [][]byte{})
    size := 10
    key := strconv.FormatInt(int64(rand.Int()), 10)
    for i := 0; i < size; i++ {
        Incr(testDB, toArgs(key))
        actual := Get(testDB, toArgs(key))
        expected := reply.MakeBulkReply([]byte(strconv.FormatInt(int64(i+1), 10)))
        if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
            t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
        }
    }
    for i := 0; i < size; i++ {
        IncrBy(testDB, toArgs(key, "-1"))
        actual := Get(testDB, toArgs(key))
        expected := reply.MakeBulkReply([]byte(strconv.FormatInt(int64(size-i-1), 10)))
        if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
            t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
        }
    }

    FlushAll(testDB, [][]byte{})
    key = strconv.FormatInt(int64(rand.Int()), 10)
    for i := 0; i < size; i++ {
        IncrBy(testDB, toArgs(key, "1"))
        actual := Get(testDB, toArgs(key))
        expected := reply.MakeBulkReply([]byte(strconv.FormatInt(int64(i+1), 10)))
        if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
            t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
        }
    }
    for i := 0; i < size; i++ {
        IncrByFloat(testDB, toArgs(key, "-1.0"))
        actual := Get(testDB, toArgs(key))
        expected := reply.MakeBulkReply([]byte(strconv.FormatInt(int64(size-i-1), 10)))
        if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
            t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
        }
    }
}