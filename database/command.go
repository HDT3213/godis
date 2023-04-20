package database

import (
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
	"strings"
)

func init() {

}

var commandTable = make(map[string]*godisCommand)

const (
	Write         = "write"
	Readonly      = "readonly"
	Denyoom       = "denyoom"
	Admin         = "admin"
	Pubsub        = "pubsub"
	Noscript      = "noscript"
	Random        = "random"
	SortForScript = "sortforscript"
	Loading       = "loading"
	Stale         = "stale"
	SkipMonitor   = "skip_monitor"
	Asking        = "asking"
	Fast          = "fast"
	Movablekeys   = "movablekeys"
)

type godisCommand struct {
	name       string
	arity      int
	signs      []string
	firstKey   int
	lastKey    int
	stepNumber int
	prepare    PreFunc
}

func RegisterGodisCommand(name string, arity int, signs []string, firstKey int, lastKey int, stepNumber int, prepare PreFunc) {
	name = strings.ToLower(name)
	commandTable[name] = &godisCommand{
		name:       name,
		arity:      arity,
		signs:      signs,
		firstKey:   firstKey,
		lastKey:    lastKey,
		stepNumber: stepNumber,
		prepare:    prepare,
	}
}

func execCommand(args [][]byte) redis.Reply {
	n := len(args)
	if n > 1 {
		subCommand := strings.ToUpper(string(args[1]))
		if subCommand == "INFO" {
			return getCommands(args[2:])
		} else if subCommand == "COUNT" {
			return protocol.MakeIntReply(int64(len(commandTable)))
		} else if subCommand == "GETKEYS" {
			if n < 2 {
				return protocol.MakeErrReply("Unknown subcommand or wrong number of arguments for '" + subCommand + "'")
			}
			return getKeys(args[2:])
		} else {
			return protocol.MakeErrReply("Unknow subcomand or wrong number of arguments for '" + subCommand + "'")
		}
	} else {
		return getAllGodisCommandReply()
	}
}

func getKeys(args [][]byte) redis.Reply {
	key := string(args[0])
	command, ok := commandTable[key]
	if !ok {
		return protocol.MakeErrReply("Invalid command specified")
	}
	arity := command.arity
	if arity > 0 {
		if len(args) != arity {
			return protocol.MakeErrReply("Invalid number of arguments specified for command")
		}
	} else {
		if len(args) < -arity {
			return protocol.MakeErrReply("Invalid number of arguments specified for command")
		}
	}

	prepare := command.prepare
	if prepare == nil {
		return protocol.MakeErrReply("The command has no key arguments")
	}
	writeKeys, readKeys := prepare(args[1:])

	keys := append(writeKeys, readKeys...)
	replies := make([]redis.Reply, len(keys))
	for i, key := range keys {
		replies[i] = protocol.MakeBulkReply([]byte(key))
	}
	return protocol.MakeMultiRawReply(replies)
}

func getCommands(args [][]byte) redis.Reply {
	replies := make([]redis.Reply, len(args))

	for i, v := range args {
		reply, ok := commandTable[string(v)]
		if ok {
			replies[i] = reply.ToReply()
		} else {
			replies[i] = protocol.MakeNullBulkReply()
		}
	}

	return protocol.MakeMultiRawReply(replies)
}

func getAllGodisCommandReply() redis.Reply {
	replies := make([]redis.Reply, len(commandTable))
	i := 0
	for _, v := range commandTable {
		replies[i] = v.ToReply()
		i++
	}
	return protocol.MakeMultiRawReply(replies)
}

func (g *godisCommand) ToReply() redis.Reply {
	args := make([]redis.Reply, 6)
	args[0] = protocol.MakeBulkReply([]byte(g.name))
	args[1] = protocol.MakeIntReply(int64(g.arity))
	signs := make([]redis.Reply, len(g.signs))
	for i, v := range g.signs {
		signs[i] = protocol.MakeStatusReply(v)
	}
	args[2] = protocol.MakeMultiRawReply(signs)
	args[3] = protocol.MakeIntReply(int64(g.firstKey))
	args[4] = protocol.MakeIntReply(int64(g.lastKey))
	args[5] = protocol.MakeIntReply(int64(g.stepNumber))

	return protocol.MakeMultiRawReply(args)
}

func init() {
	RegisterGodisCommand("Command", 0, []string{Random, Loading, Stale}, 0, 0, 0, nil)

	RegisterGodisCommand("Keys", 2, []string{Readonly, SortForScript}, 0, 0, 0, nil)
	RegisterGodisCommand("Auth", 2, []string{Noscript, Loading, Stale, SkipMonitor, Fast}, 0, 0, 0, nil)
	RegisterGodisCommand("Info", -1, []string{Random, Loading, Stale}, 0, 0, 0, nil)
	RegisterGodisCommand("Slaveof", 3, []string{Admin, Noscript, Stale}, 0, 0, 0, nil)
	RegisterGodisCommand("Subscribe", -2, []string{Pubsub, Noscript, Loading, Stale}, 0, 0, 0, nil)
	RegisterGodisCommand("Publish", 3, []string{Pubsub, Noscript, Loading, Fast}, 0, 0, 0, nil)
	RegisterGodisCommand("FlushAll", -1, []string{Write}, 0, 0, 0, nil)
	RegisterGodisCommand("FlushDb", -1, []string{Write}, 0, 0, 0, nil)
	RegisterGodisCommand("Save", 1, []string{Admin, Noscript}, 0, 0, 0, nil)
	RegisterGodisCommand("BgSave", 1, []string{Admin, Noscript}, 0, 0, 0, nil)
	RegisterGodisCommand("Select", 2, []string{Loading, Fast}, 0, 0, 0, nil)
	RegisterGodisCommand("Replconf", -1, []string{Admin, Noscript, Loading, Stale}, 0, 0, 0, nil)
	RegisterGodisCommand("Replconf", 3, []string{Readonly, Admin, Noscript}, 0, 0, 0, nil)

	// transaction command
	RegisterGodisCommand("Multi", 1, []string{Noscript, Fast}, 0, 0, 0, nil)
	RegisterGodisCommand("Discard", 1, []string{Noscript, Fast}, 0, 0, 0, nil)
	RegisterGodisCommand("Exec", 1, []string{Noscript, SkipMonitor}, 0, 0, 0, nil)
	RegisterGodisCommand("Watch", 1, []string{Noscript, Fast}, 1, -1, 1, readAllKeys)

}
