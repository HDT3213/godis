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
	config.PropertiesMap = map[string]interface{}{
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
}
func TestConfigSet(t *testing.T) {
	testDB.Flush()
	testMDB := NewStandaloneServer()
	result := testMDB.Exec(nil, utils.ToCmdLine("config", "set", "appendfsync", "no"))
	asserts.AssertOkReply(t, result)
}
