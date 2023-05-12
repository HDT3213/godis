package database

import (
	"github.com/hdt3213/godis/aof"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	_ "github.com/hdt3213/godis/redis/protocol/asserts"
	"testing"
)

func init() {
	config.Properties = &config.ServerProperties{
		AppendOnly:        true,
		AppendFilename:    "appendonly.aof",
		AofUseRdbPreamble: false,
		AppendFsync:       aof.FsyncEverySec,
		MaxClients:        128,
	}
	config.PropertiesMap = map[string]string{
		"appendonly":           "yes",
		"appendfilename":       "appendonly.aof",
		"aof-use-rdb-preamble": "no",
		"appendfsync":          "everysec",
		"maxclients":           "128",
	}
}

func TestConfigGet(t *testing.T) {
	testDB.Flush()
	testMDB := NewStandaloneServer()

	result := testMDB.Exec(nil, utils.ToCmdLine("config", "get", "maxclients"))
	asserts.AssertMultiRawReply(t, result, []string{"$10\r\nmaxclients\r\n", "$3\r\n128\r\n"})
	result = testMDB.Exec(nil, utils.ToCmdLine("config", "get", "maxcli*"))
	asserts.AssertMultiRawReply(t, result, []string{"$10\r\nmaxclients\r\n", "$3\r\n128\r\n"})
	result = testMDB.Exec(nil, utils.ToCmdLine("config", "get", "none"))
	asserts.AssertMultiRawReply(t, result, []string{})
	result = testMDB.Exec(nil, utils.ToCmdLine("config", "get", "maxclients", "appendonly"))
	asserts.AssertMultiRawReply(t, result, []string{"$10\r\nmaxclients\r\n", "$3\r\n128\r\n", "$10\r\nappendonly\r\n", "$3\r\nyes\r\n"})
}
func TestConfigSet(t *testing.T) {
	testDB.Flush()
	testMDB := NewStandaloneServer()
	result := testMDB.Exec(nil, utils.ToCmdLine("config", "set", "appendfsync", "no"))
	asserts.AssertOkReply(t, result)
	result = testMDB.Exec(nil, utils.ToCmdLine("config", "set", "appendfsync", "no", "maxclients", "110"))
	asserts.AssertOkReply(t, result)
	result = testMDB.Exec(nil, utils.ToCmdLine("config", "set", "appendonly", "no"))
	asserts.AssertErrReply(t, result, "ERR CONFIG SET failed (possibly related to argument 'appendonly') - can't set immutable config")
	result = testMDB.Exec(nil, utils.ToCmdLine("config", "set", "appendfsync", "no", "maxclients", "panic"))
	asserts.AssertErrReply(t, result, "ERR CONFIG SET failed (possibly related to argument 'maxclients') - argument couldn't be parsed into an integer")
	result = testMDB.Exec(nil, utils.ToCmdLine("config", "set", "appendfsync", "no", "errorConfig", "110"))
	asserts.AssertErrReply(t, result, "ERR Unknown option or number of arguments for CONFIG SET - 'errorConfig'")
	result = testMDB.Exec(nil, utils.ToCmdLine("config", "set", "appendfsync", "no", "maxclients"))
	asserts.AssertErrReply(t, result, "ERR wrong number of arguments for 'config|set' command")
	result = testMDB.Exec(nil, utils.ToCmdLine("config", "set", "appendfsync", "no", "appendfsync", "yes"))
	asserts.AssertErrReply(t, result, "ERR CONFIG SET failed (possibly related to argument 'appendfsync') - duplicate parameter")
}
