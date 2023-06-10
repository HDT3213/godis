package database

import (
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	_ "github.com/hdt3213/godis/redis/protocol/asserts"
	"testing"
)

//func init() {
//	config.Properties = &config.ServerProperties{
//		AppendOnly:        true,
//		AppendFilename:    "appendonly.aof",
//		AofUseRdbPreamble: false,
//		AppendFsync:       aof.FsyncEverySec,
//		MaxClients:        128,
//	}
//}

func TestConfigGet(t *testing.T) {
	testDB.Flush()
	testMDB := NewStandaloneServer()

	result := testMDB.Exec(nil, utils.ToCmdLine("config", "get", "maxclients"))
	asserts.AssertMultiBulkReply(t, result, []string{"maxclients", "0"})
	result = testMDB.Exec(nil, utils.ToCmdLine("config", "get", "maxcli*"))
	asserts.AssertMultiBulkReply(t, result, []string{"maxclients", "0"})
	result = testMDB.Exec(nil, utils.ToCmdLine("config", "get", "none"))
	asserts.AssertMultiBulkReply(t, result, []string{})
	result = testMDB.Exec(nil, utils.ToCmdLine("config", "get", "maxclients", "appendonly"))
	asserts.AssertMultiBulkReply(t, result, []string{"maxclients", "0", "appendonly", "yes"})
}
func TestConfigSet(t *testing.T) {
	testDB.Flush()
	testMDB := NewStandaloneServer()
	result := testMDB.Exec(nil, utils.ToCmdLine("config", "set", "appendfsync", "no"))
	asserts.AssertOkReply(t, result)
	result = testMDB.Exec(nil, utils.ToCmdLine("config", "set", "appendfsync", "no", "maxclients", "110"))
	asserts.AssertOkReply(t, result)
	result = testMDB.Exec(nil, utils.ToCmdLine("config", "set", "appendonly", "no"))
	asserts.AssertOkReply(t, result)
	result = testMDB.Exec(nil, utils.ToCmdLine("config", "set", "appendfsync", "no", "maxclients", "panic"))
	asserts.AssertErrReply(t, result, "ERR CONFIG SET failed (possibly related to argument 'maxclients') - argument couldn't be parsed into an integer")
	result = testMDB.Exec(nil, utils.ToCmdLine("config", "set", "appendfsync", "no", "errorConfig", "110"))
	asserts.AssertErrReply(t, result, "ERR Unknown option or number of arguments for CONFIG SET - 'errorConfig'")
	result = testMDB.Exec(nil, utils.ToCmdLine("config", "set", "appendfsync", "no", "maxclients"))
	asserts.AssertErrReply(t, result, "ERR wrong number of arguments for 'config|set' command")
	result = testMDB.Exec(nil, utils.ToCmdLine("config", "set", "appendfsync", "no", "appendfsync", "yes"))
	asserts.AssertErrReply(t, result, "ERR CONFIG SET failed (possibly related to argument 'appendfsync') - duplicate parameter")
}
