package database

import (
	"fmt"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
)

// Ping the server
func Ping(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return &protocol.PongReply{}
	} else if len(args) == 1 {
		return protocol.MakeStatusReply(string(args[0]))
	} else {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}

// GenGodisInfoString Create the string returned by the INFO command
func GenGodisInfoString(c redis.Connection, args [][]byte) redis.Reply {
	key := string(args[0])
	value := string(args[1])
	if key != "info" {
		return &protocol.SyntaxErrReply{}
	}
	if value == "" {
		value = "all"
	}
	switch value {
	case "all":
		allInfo := serverInfo()
		return protocol.MakeBulkReply(allInfo)
	case "server":

		reply := serverInfo()
		return protocol.MakeBulkReply(reply)
	}
	return &protocol.NullBulkReply{}
}

// Auth validate client's password
func Auth(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'auth' command")
	}
	if config.Properties.RequirePass == "" {
		return protocol.MakeErrReply("ERR Client sent AUTH, but no password is set")
	}
	passwd := string(args[0])
	c.SetPassword(passwd)
	if config.Properties.RequirePass != passwd {
		return protocol.MakeErrReply("ERR invalid password")
	}
	return &protocol.OkReply{}
}

func isAuthenticated(c redis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}

func serverInfo() []byte {
	s := fmt.Sprintf("# Server\r\n"+
		"redis_version:%s\r\n"+
		"redis_git_sha1:%s\r\n"+
		"redis_git_dirty:%d\r\n"+
		"redis_build_id:%s\r\n"+
		"redis_mode:%s\r\n"+
		"os:%s %s %s\r\n"+
		"arch_bits:%d\r\n"+
		"multiplexing_api:%s\r\n"+
		"gcc_version:%d.%d.%d\r\n"+
		"process_id:%d\r\n"+
		"run_id:%s\r\n"+
		"tcp_port:%d\r\n"+
		"uptime_in_seconds:%d\r\n"+
		"uptime_in_days:%d\r\n"+
		"hz:%d\r\n"+
		"lru_clock:%d\r\n"+
		"config_file:%s\r\n",
		"1.0.1",
		"asdfsadf",
		1212,
		"asdfbabab",
		"cluster",
		"s", "b", "c",
		64,
		"1.1",
		1, 1, 1,
		1212,
		"asdf",
		6399,
		3333,
		60,
		1111,
		212121,
		"/u01/aoo/")
	return []byte(s)
}
