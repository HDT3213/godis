package godis

import (
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/datastruct/utils"
	utils2 "github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/reply"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"
	"time"
)

func TestAof(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "godis")
	if err != nil {
		t.Error(err)
		return
	}
	aofFilename := path.Join(tmpDir, "a.aof")
	defer func() {
		_ = os.Remove(aofFilename)
	}()
	config.Properties = &config.ServerProperties{
		AppendOnly:     true,
		AppendFilename: aofFilename,
	}
	aofWriteDB := MakeDB()
	size := 10
	keys := make([]string, 0)
	cursor := 0
	for i := 0; i < size; i++ {
		key := strconv.Itoa(cursor)
		cursor++
		Set(aofWriteDB, utils2.ToBytesList(key, utils2.RandString(8), "EX", "10000"))
		keys = append(keys, key)
	}
	for i := 0; i < size; i++ {
		key := strconv.Itoa(cursor)
		cursor++
		RPush(aofWriteDB, utils2.ToBytesList(key, utils2.RandString(8)))
		keys = append(keys, key)
	}
	for i := 0; i < size; i++ {
		key := strconv.Itoa(cursor)
		cursor++
		HSet(aofWriteDB, utils2.ToBytesList(key, utils2.RandString(8), utils2.RandString(8)))
		keys = append(keys, key)
	}
	for i := 0; i < size; i++ {
		key := strconv.Itoa(cursor)
		cursor++
		SAdd(aofWriteDB, utils2.ToBytesList(key, utils2.RandString(8)))
		keys = append(keys, key)
	}
	for i := 0; i < size; i++ {
		key := strconv.Itoa(cursor)
		cursor++
		ZAdd(aofWriteDB, utils2.ToBytesList(key, "10", utils2.RandString(8)))
		keys = append(keys, key)
	}
	aofWriteDB.Close()    // wait for aof finished
	aofReadDB := MakeDB() // start new db and read aof file
	for _, key := range keys {
		expect, ok := aofWriteDB.GetEntity(key)
		if !ok {
			t.Errorf("key not found in origin: %s", key)
			continue
		}
		actual, ok := aofReadDB.GetEntity(key)
		if !ok {
			t.Errorf("key not found: %s", key)
			continue
		}
		expectData := EntityToCmd(key, expect).ToBytes()
		actualData := EntityToCmd(key, actual).ToBytes()
		if !utils.BytesEquals(expectData, actualData) {
			t.Errorf("wrong value of key: %s", key)
		}
	}
	aofReadDB.Close()
}

func TestRewriteAOF(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "godis")
	if err != nil {
		t.Error(err)
		return
	}
	aofFilename := path.Join(tmpDir, "a.aof")
	defer func() {
		_ = os.Remove(aofFilename)
	}()
	config.Properties = &config.ServerProperties{
		AppendOnly:     true,
		AppendFilename: aofFilename,
	}
	aofWriteDB := MakeDB()
	size := 1
	keys := make([]string, 0)
	ttlKeys := make([]string, 0)
	cursor := 0
	for i := 0; i < size; i++ {
		key := "str" + strconv.Itoa(cursor)
		cursor++
		Set(aofWriteDB, utils2.ToBytesList(key, utils2.RandString(8)))
		Set(aofWriteDB, utils2.ToBytesList(key, utils2.RandString(8)))
		keys = append(keys, key)
	}
	// test ttl
	for i := 0; i < size; i++ {
		key := "str" + strconv.Itoa(cursor)
		cursor++
		Set(aofWriteDB, utils2.ToBytesList(key, utils2.RandString(8), "EX", "1000"))
		ttlKeys = append(ttlKeys, key)
	}
	for i := 0; i < size; i++ {
		key := "list" + strconv.Itoa(cursor)
		cursor++
		RPush(aofWriteDB, utils2.ToBytesList(key, utils2.RandString(8)))
		RPush(aofWriteDB, utils2.ToBytesList(key, utils2.RandString(8)))
		keys = append(keys, key)
	}
	for i := 0; i < size; i++ {
		key := "hash" + strconv.Itoa(cursor)
		cursor++
		field := utils2.RandString(8)
		HSet(aofWriteDB, utils2.ToBytesList(key, field, utils2.RandString(8)))
		HSet(aofWriteDB, utils2.ToBytesList(key, field, utils2.RandString(8)))
		keys = append(keys, key)
	}
	for i := 0; i < size; i++ {
		key := "set" + strconv.Itoa(cursor)
		cursor++
		member := utils2.RandString(8)
		SAdd(aofWriteDB, utils2.ToBytesList(key, member))
		SAdd(aofWriteDB, utils2.ToBytesList(key, member))
		keys = append(keys, key)
	}
	for i := 0; i < size; i++ {
		key := "zset" + strconv.Itoa(cursor)
		cursor++
		ZAdd(aofWriteDB, utils2.ToBytesList(key, "10", utils2.RandString(8)))
		keys = append(keys, key)
	}
	time.Sleep(time.Second) // wait for async goroutine finish its job
	aofWriteDB.aofRewrite()
	aofWriteDB.Close()    // wait for aof finished
	aofReadDB := MakeDB() // start new db and read aof file
	for _, key := range keys {
		expect, ok := aofWriteDB.GetEntity(key)
		if !ok {
			t.Errorf("key not found in origin: %s", key)
			continue
		}
		actual, ok := aofReadDB.GetEntity(key)
		if !ok {
			t.Errorf("key not found: %s", key)
			continue
		}
		expectData := EntityToCmd(key, expect).ToBytes()
		actualData := EntityToCmd(key, actual).ToBytes()
		if !utils.BytesEquals(expectData, actualData) {
			t.Errorf("wrong value of key: %s", key)
		}
	}
	for _, key := range ttlKeys {
		ret := TTL(aofReadDB, utils2.ToBytesList(key))
		intResult, ok := ret.(*reply.IntReply)
		if !ok {
			t.Errorf("expected int reply, actually %s", ret.ToBytes())
			return
		}
		if intResult.Code <= 0 {
			t.Errorf("expect a positive integer, actual: %d", intResult.Code)
		}
	}
	aofReadDB.Close()
}
