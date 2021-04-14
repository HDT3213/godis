package db

import (
	"github.com/hdt3213/godis/src/config"
	"github.com/hdt3213/godis/src/datastruct/utils"
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
	config.Properties = &config.PropertyHolder{
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
		Set(aofWriteDB, toArgs(key, RandString(8), "EX", "10000"))
		keys = append(keys, key)
	}
	for i := 0; i < size; i++ {
		key := strconv.Itoa(cursor)
		cursor++
		RPush(aofWriteDB, toArgs(key, RandString(8)))
		keys = append(keys, key)
	}
	for i := 0; i < size; i++ {
		key := strconv.Itoa(cursor)
		cursor++
		HSet(aofWriteDB, toArgs(key, RandString(8), RandString(8)))
		keys = append(keys, key)
	}
	for i := 0; i < size; i++ {
		key := strconv.Itoa(cursor)
		cursor++
		SAdd(aofWriteDB, toArgs(key, RandString(8)))
		keys = append(keys, key)
	}
	for i := 0; i < size; i++ {
		key := strconv.Itoa(cursor)
		cursor++
		ZAdd(aofWriteDB, toArgs(key, "10", RandString(8)))
		keys = append(keys, key)
	}
	aofWriteDB.Close()    // wait for aof finished
	aofReadDB := MakeDB() // start new db and read aof file
	for _, key := range keys {
		expect, ok := aofWriteDB.Get(key)
		if !ok {
			t.Errorf("key not found in origin: %s", key)
			continue
		}
		actual, ok := aofReadDB.Get(key)
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
	config.Properties = &config.PropertyHolder{
		AppendOnly:     true,
		AppendFilename: aofFilename,
	}
	aofWriteDB := MakeDB()
	size := 1
	keys := make([]string, 0)
	cursor := 0
	for i := 0; i < size; i++ {
		key := "str" + strconv.Itoa(cursor)
		cursor++
		Set(aofWriteDB, toArgs(key, RandString(8)))
		Set(aofWriteDB, toArgs(key, RandString(8)))
		keys = append(keys, key)
	}
	for i := 0; i < size; i++ {
		key := "list" + strconv.Itoa(cursor)
		cursor++
		RPush(aofWriteDB, toArgs(key, RandString(8)))
		RPush(aofWriteDB, toArgs(key, RandString(8)))
		keys = append(keys, key)
	}
	for i := 0; i < size; i++ {
		key := "hash" + strconv.Itoa(cursor)
		cursor++
		field := RandString(8)
		HSet(aofWriteDB, toArgs(key, field, RandString(8)))
		HSet(aofWriteDB, toArgs(key, field, RandString(8)))
		keys = append(keys, key)
	}
	for i := 0; i < size; i++ {
		key := "set" + strconv.Itoa(cursor)
		cursor++
		member := RandString(8)
		SAdd(aofWriteDB, toArgs(key, member))
		SAdd(aofWriteDB, toArgs(key, member))
		keys = append(keys, key)
	}
	for i := 0; i < size; i++ {
		key := "zset" + strconv.Itoa(cursor)
		cursor++
		ZAdd(aofWriteDB, toArgs(key, "10", RandString(8)))
		keys = append(keys, key)
	}
	time.Sleep(time.Second) // wait for async goroutine finish its job
	aofWriteDB.aofRewrite()
	aofWriteDB.Close()    // wait for aof finished
	aofReadDB := MakeDB() // start new db and read aof file
	for _, key := range keys {
		expect, ok := aofWriteDB.Get(key)
		if !ok {
			t.Errorf("key not found in origin: %s", key)
			continue
		}
		actual, ok := aofReadDB.Get(key)
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
