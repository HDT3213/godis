package db

import (
    "github.com/HDT3213/godis/src/datastruct/utils"
    "github.com/HDT3213/godis/src/redis/reply"
    "math/rand"
    "strconv"
    "testing"
)

var db = makeTestDB()

func TestSet(t *testing.T) {
    FlushAll(db, [][]byte{})
    key := strconv.FormatInt(int64(rand.Int()), 10)
    value := strconv.FormatInt(int64(rand.Int()), 10)

    // normal set
    Set(db, toArgs(key, value))
    actual, _ := Get(db, toArgs(key))
    expected := reply.MakeBulkReply([]byte(value))
    if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
        t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
    }

    // set nx
    actual, _ = Set(db, toArgs(key, value, "NX"))
    if _, ok := actual.(*reply.NullBulkReply); !ok {
        t.Error("expected true actual false")
    }

    FlushAll(db, [][]byte{})
    key = strconv.FormatInt(int64(rand.Int()), 10)
    value = strconv.FormatInt(int64(rand.Int()), 10)
    Set(db, toArgs(key, value, "NX"))
    actual, _ = Get(db, toArgs(key))
    expected = reply.MakeBulkReply([]byte(value))
    if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
        t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
    }

    // set xx
    FlushAll(db, [][]byte{})
    key = strconv.FormatInt(int64(rand.Int()), 10)
    value = strconv.FormatInt(int64(rand.Int()), 10)
    actual, _ = Set(db, toArgs(key, value, "XX"))
    if _, ok := actual.(*reply.NullBulkReply); !ok {
        t.Error("expected true actually false ")
    }

    Set(db, toArgs(key, value))
    Set(db, toArgs(key, value, "XX"))
    actual, _ = Get(db, toArgs(key))
    expected = reply.MakeBulkReply([]byte(value))
    if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
        t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
    }

}

func TestSetNX(t *testing.T) {
    FlushAll(db, [][]byte{})
    key := strconv.FormatInt(int64(rand.Int()), 10)
    value := strconv.FormatInt(int64(rand.Int()), 10)
    SetNX(db, toArgs(key, value))
    actual, _ := Get(db, toArgs(key))
    expected := reply.MakeBulkReply([]byte(value))
    if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
        t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
    }

    actual, _ = SetNX(db, toArgs(key, value))
    expected2 := reply.MakeIntReply(int64(0))
    if !utils.BytesEquals(actual.ToBytes(), expected2.ToBytes()) {
        t.Error("expected: " + string(expected2.ToBytes()) + ", actual: " + string(actual.ToBytes()))
    }
}

func TestSetEX(t *testing.T) {
    FlushAll(db, [][]byte{})
    key := strconv.FormatInt(int64(rand.Int()), 10)
    value := strconv.FormatInt(int64(rand.Int()), 10)
    ttl := "1000"

    SetEX(db, toArgs(key, ttl, value))
    actual, _ := Get(db, toArgs(key))
    expected2 := reply.MakeBulkReply([]byte(value))
    if !utils.BytesEquals(actual.ToBytes(), expected2.ToBytes()) {
        t.Error("expected: " + string(expected2.ToBytes()) + ", actual: " + string(actual.ToBytes()))
    }
}

func TestMSet(t *testing.T) {
    FlushAll(db, [][]byte{})
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
    MSet(db, toArgs(args...))
    actual, _ := MGet(db, toArgs(keys...))
    expected := reply.MakeMultiBulkReply(values)
    if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
        t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
    }
}

func TestIncr(t *testing.T) {
    FlushAll(db, [][]byte{})
    size := 10
    key := strconv.FormatInt(int64(rand.Int()), 10)
    for i := 0; i < size; i++ {
        Incr(db, toArgs(key))
        actual, _ := Get(db, toArgs(key))
        expected := reply.MakeBulkReply([]byte(strconv.FormatInt(int64(i+1), 10)))
        if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
            t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
        }
    }
    for i := 0; i < size; i++ {
        IncrBy(db, toArgs(key, "-1"))
        actual, _ := Get(db, toArgs(key))
        expected := reply.MakeBulkReply([]byte(strconv.FormatInt(int64(size-i-1), 10)))
        if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
            t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
        }
    }

    FlushAll(db, [][]byte{})
    key = strconv.FormatInt(int64(rand.Int()), 10)
    for i := 0; i < size; i++ {
        IncrBy(db, toArgs(key, "1"))
        actual, _ := Get(db, toArgs(key))
        expected := reply.MakeBulkReply([]byte(strconv.FormatInt(int64(i+1), 10)))
        if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
            t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
        }
    }
    for i := 0; i < size; i++ {
        IncrByFloat(db, toArgs(key, "-1.0"))
        actual, _ := Get(db, toArgs(key))
        expected := reply.MakeBulkReply([]byte(strconv.FormatInt(int64(size-i-1), 10)))
        if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
            t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
        }
    }
}