package database

import (
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"path/filepath"
	"runtime"
	"testing"
)

func TestLoadRDB(t *testing.T) {
	_, b, _, _ := runtime.Caller(0)
	projectRoot := filepath.Dir(filepath.Dir(b))
	config.Properties = &config.ServerProperties{
		AppendOnly:  false,
		RDBFilename: filepath.Join(projectRoot, "test.rdb"), // set working directory to project root
	}
	conn := &connection.FakeConn{}
	rdbDB := NewStandaloneServer()
	result := rdbDB.Exec(conn, utils.ToCmdLine("Get", "str"))
	asserts.AssertBulkReply(t, result, "str")
	result = rdbDB.Exec(conn, utils.ToCmdLine("TTL", "str"))
	asserts.AssertIntReplyGreaterThan(t, result, 0)
	result = rdbDB.Exec(conn, utils.ToCmdLine("LRange", "list", "0", "-1"))
	asserts.AssertMultiBulkReply(t, result, []string{"1", "2", "3", "4"})
	result = rdbDB.Exec(conn, utils.ToCmdLine("HGetAll", "hash"))
	asserts.AssertMultiBulkReply(t, result, []string{"1", "1"})
	result = rdbDB.Exec(conn, utils.ToCmdLine("ZRange", "zset", "0", "1", "WITHSCORES"))
	asserts.AssertMultiBulkReply(t, result, []string{"1", "1"})

	config.Properties = &config.ServerProperties{
		AppendOnly:  false,
		RDBFilename: filepath.Join(projectRoot, "none", "test.rdb"), // set working directory to project root
	}
	rdbDB = NewStandaloneServer()
	result = rdbDB.Exec(conn, utils.ToCmdLine("Get", "str"))
	asserts.AssertNullBulk(t, result)
}
