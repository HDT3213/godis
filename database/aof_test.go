package database

import (
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"
	"time"
)

func makeTestData(db database.DB, dbIndex int, prefix string, size int) {
	conn := connection.NewFakeConn()
	conn.SelectDB(dbIndex)
	db.Exec(conn, utils.ToCmdLine("FlushDB"))
	cursor := 0
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		db.Exec(conn, utils.ToCmdLine("SET", key, key, "EX", "10000"))
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		db.Exec(conn, utils.ToCmdLine("RPUSH", key, key))
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		db.Exec(conn, utils.ToCmdLine("HSET", key, key, key))
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		db.Exec(conn, utils.ToCmdLine("SADD", key, key))
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		db.Exec(conn, utils.ToCmdLine("ZADD", key, "10", key))
	}
}

func validateTestData(t *testing.T, db database.DB, dbIndex int, prefix string, size int) {
	conn := connection.NewFakeConn()
	conn.SelectDB(dbIndex)
	cursor := 0
	var ret redis.Reply
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		ret = db.Exec(conn, utils.ToCmdLine("GET", key))
		asserts.AssertBulkReply(t, ret, key)
		ret = db.Exec(conn, utils.ToCmdLine("TTL", key))
		intResult, ok := ret.(*protocol.IntReply)
		if !ok {
			t.Errorf("expected int protocol, actually %s", ret.ToBytes())
			return
		}
		if intResult.Code <= 0 || intResult.Code > 10000 {
			t.Error("wrong ttl")
		}
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		ret = db.Exec(conn, utils.ToCmdLine("LRANGE", key, "0", "-1"))
		asserts.AssertMultiBulkReply(t, ret, []string{key})
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		ret = db.Exec(conn, utils.ToCmdLine("HGET", key, key))
		asserts.AssertBulkReply(t, ret, key)
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		ret = db.Exec(conn, utils.ToCmdLine("SIsMember", key, key))
		asserts.AssertIntReply(t, ret, 1)
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		ret = db.Exec(conn, utils.ToCmdLine("ZRANGE", key, "0", "-1"))
		asserts.AssertMultiBulkReply(t, ret, []string{key})
	}
}

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
	dbNum := 4
	size := 10
	var prefixes []string
	aofWriteDB := NewStandaloneServer()
	for i := 0; i < dbNum; i++ {
		prefix := utils.RandString(8)
		prefixes = append(prefixes, prefix)
		makeTestData(aofWriteDB, i, prefix, size)
	}
	aofWriteDB.Close()                 // wait for aof finished
	aofReadDB := NewStandaloneServer() // start new db and read aof file
	for i := 0; i < dbNum; i++ {
		prefix := prefixes[i]
		validateTestData(t, aofReadDB, i, prefix, size)
	}
	aofReadDB.Close()
}

func TestRDB(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "godis")
	if err != nil {
		t.Error(err)
		return
	}
	aofFilename := path.Join(tmpDir, "a.aof")
	rdbFilename := path.Join(tmpDir, "dump.rdb")
	defer func() {
		_ = os.Remove(aofFilename)
		_ = os.Remove(rdbFilename)
	}()
	config.Properties = &config.ServerProperties{
		AppendOnly:     true,
		AppendFilename: aofFilename,
		RDBFilename:    rdbFilename,
	}
	dbNum := 4
	size := 10
	var prefixes []string
	conn := connection.NewFakeConn()
	writeDB := NewStandaloneServer()
	for i := 0; i < dbNum; i++ {
		prefix := utils.RandString(8)
		prefixes = append(prefixes, prefix)
		makeTestData(writeDB, i, prefix, size)
	}
	time.Sleep(time.Second) // wait for aof finished
	writeDB.Exec(conn, utils.ToCmdLine("save"))
	writeDB.Close()
	readDB := NewStandaloneServer() // start new db and read aof file
	for i := 0; i < dbNum; i++ {
		prefix := prefixes[i]
		validateTestData(t, readDB, i, prefix, size)
	}
	readDB.Close()
}

func TestRewriteAOF(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "*.aof")
	if err != nil {
		t.Error(err)
		return
	}
	aofFilename := tmpFile.Name()
	defer func() {
		_ = os.Remove(aofFilename)
	}()
	config.Properties = &config.ServerProperties{
		AppendOnly:     true,
		AppendFilename: aofFilename,
	}
	aofWriteDB := NewStandaloneServer()
	size := 1
	dbNum := 4
	var prefixes []string
	for i := 0; i < dbNum; i++ {
		prefix := "" // utils.RandString(8)
		prefixes = append(prefixes, prefix)
		makeTestData(aofWriteDB, i, prefix, size)
	}
	//time.Sleep(2 * time.Second)
	aofWriteDB.Exec(nil, utils.ToCmdLine("rewriteaof"))
	time.Sleep(2 * time.Second)        // wait for async goroutine finish its job
	aofWriteDB.Close()                 // wait for aof finished
	aofReadDB := NewStandaloneServer() // start new db and read aof file
	for i := 0; i < dbNum; i++ {
		prefix := prefixes[i]
		validateTestData(t, aofReadDB, i, prefix, size)
	}
	aofReadDB.Close()
}

// TestRewriteAOF2 tests execute commands during rewrite procedure
func TestRewriteAOF2(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "*.aof")
	if err != nil {
		t.Error(err)
		return
	}
	aofFilename := tmpFile.Name()
	defer func() {
		_ = os.Remove(aofFilename)
	}()
	config.Properties = &config.ServerProperties{
		AppendOnly:     true,
		AppendFilename: aofFilename,
	}
	aofWriteDB := NewStandaloneServer()
	dbNum := 4
	conn := connection.NewFakeConn()
	for i := 0; i < dbNum; i++ {
		conn.SelectDB(i)
		key := strconv.Itoa(i)
		aofWriteDB.Exec(conn, utils.ToCmdLine("SET", key, key))
	}

	ctx, err := aofWriteDB.persister.StartRewrite()
	if err != nil {
		t.Error(err)
		return
	}
	// add data during rewrite
	for i := 0; i < dbNum; i++ {
		conn.SelectDB(i)
		key := "a" + strconv.Itoa(i)
		aofWriteDB.Exec(conn, utils.ToCmdLine("SET", key, key))
	}
	aofWriteDB.persister.DoRewrite(ctx)
	aofWriteDB.persister.FinishRewrite(ctx)

	aofWriteDB.Close()                 // wait for aof finished
	aofReadDB := NewStandaloneServer() // start new db and read aof file
	for i := 0; i < dbNum; i++ {
		conn.SelectDB(i)
		key := strconv.Itoa(i)
		ret := aofReadDB.Exec(conn, utils.ToCmdLine("GET", key))
		asserts.AssertBulkReply(t, ret, key)

		key = "a" + strconv.Itoa(i)
		ret = aofReadDB.Exec(conn, utils.ToCmdLine("GET", key))
		asserts.AssertBulkReply(t, ret, key)
	}
	aofReadDB.Close()
}
