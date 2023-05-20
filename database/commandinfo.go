package database

import (
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
	"strings"
)

const (
	redisFlagWrite         = "write"
	redisFlagReadonly      = "readonly"
	redisFlagDenyOOM       = "denyoom"
	redisFlagAdmin         = "admin"
	redisFlagPubSub        = "pubsub"
	redisFlagNoScript      = "noscript"
	redisFlagRandom        = "random"
	redisFlagSortForScript = "sortforscript"
	redisFlagLoading       = "loading"
	redisFlagStale         = "stale"
	redisFlagSkipMonitor   = "skip_monitor"
	redisFlagAsking        = "asking"
	redisFlagFast          = "fast"
	redisFlagMovableKeys   = "movablekeys"
)

func execCommand(args [][]byte) redis.Reply {
	if len(args) == 0 {
		return getAllGodisCommandReply()
	}
	subCommand := strings.ToLower(string(args[0]))
	if subCommand == "info" {
		return getCommands(args[1:])
	} else if subCommand == "count" {
		return protocol.MakeIntReply(int64(len(cmdTable)))
	} else if subCommand == "getkeys" {
		if len(args) < 2 {
			return protocol.MakeErrReply("wrong number of arguments for 'command|" + subCommand + "'")
		}
		return getKeys(args[1:])
	} else {
		return protocol.MakeErrReply("Unknown subcommand '" + subCommand + "'")
	}
}

func getKeys(args [][]byte) redis.Reply {
	cmdName := string(args[0])
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("Invalid command specified")
	}
	if !validateArity(cmd.arity, args[1:]) {
		return protocol.MakeArgNumErrReply(cmdName)
	}

	if cmd.prepare == nil {
		return protocol.MakeErrReply("The command has no key arguments")
	}
	writeKeys, readKeys := cmd.prepare(args[1:])
	keys := append(writeKeys, readKeys...)
	resp := make([][]byte, len(keys))
	for i, key := range keys {
		resp[i] = []byte(key)
	}
	return protocol.MakeMultiBulkReply(resp)
}

func getCommands(args [][]byte) redis.Reply {
	replies := make([]redis.Reply, len(args))
	for i, v := range args {
		cmd, ok := cmdTable[string(v)]
		if ok {
			replies[i] = cmd.toDescReply()
		} else {
			replies[i] = protocol.MakeNullBulkReply()
		}
	}
	return protocol.MakeMultiRawReply(replies)
}

func getAllGodisCommandReply() redis.Reply {
	replies := make([]redis.Reply, 0, len(cmdTable))
	for _, v := range cmdTable {
		replies = append(replies, v.toDescReply())
	}
	return protocol.MakeMultiRawReply(replies)
}

func init() {
	registerSpecialCommand("Command", 0, 0).
		attachCommandExtra([]string{redisFlagRandom, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	registerSpecialCommand("Keys", 2, 0).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 0, 0, 0)
	registerSpecialCommand("Auth", 2, 0).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagLoading, redisFlagStale, redisFlagSkipMonitor, redisFlagFast}, 0, 0, 0)
	registerSpecialCommand("Info", -1, 0).
		attachCommandExtra([]string{redisFlagRandom, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	registerSpecialCommand("SlaveOf", 3, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript, redisFlagStale}, 0, 0, 0)
	registerSpecialCommand("Subscribe", -2, 0).
		attachCommandExtra([]string{redisFlagPubSub, redisFlagNoScript, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	registerSpecialCommand("Publish", 3, 0).
		attachCommandExtra([]string{redisFlagPubSub, redisFlagNoScript, redisFlagLoading, redisFlagFast}, 0, 0, 0)
	registerSpecialCommand("FlushAll", -1, 0).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerSpecialCommand("FlushDB", -1, 0).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerSpecialCommand("Save", -1, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript}, 0, 0, 0)
	registerSpecialCommand("BgSave", 1, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript}, 0, 0, 0)
	registerSpecialCommand("Select", 2, 0).
		attachCommandExtra([]string{redisFlagLoading, redisFlagFast}, 0, 0, 0)
	registerSpecialCommand("ReplConf", -1, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	//attachCommandExtra("ReplConf", 3, []string{redisFlagReadonly, redisFlagAdmin, redisFlagNoScript}, 0, 0, 0, nil)

	// transaction command
	registerSpecialCommand("Multi", 1, 0).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagFast}, 0, 0, 0)
	registerSpecialCommand("Discard", 1, 0).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagFast}, 0, 0, 0)
	registerSpecialCommand("Exec", 1, 0).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagSkipMonitor}, 0, 0, 0)
	registerSpecialCommand("Watch", 1, 0).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagFast}, 1, -1, 1)
}
