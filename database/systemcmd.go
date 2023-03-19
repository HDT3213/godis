package database

import (
	"fmt"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
	"os"
	"runtime"
	"strings"
	"time"
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

// Info the information of the godis server returned by the INFO command
func Info(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) == 1 {
		return protocol.MakeBulkReply(GenGodisInfoString())
	} else if len(args) == 2 {
		section := strings.ToLower(string(args[1]))
		switch section {
		case "server":
			reply := GenGodisInfoString()
			return protocol.MakeBulkReply(reply)
		default:
			return protocol.MakeNullBulkReply()
		}
	} else {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'info' command")
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

func GenGodisInfoString() []byte {
	startUpTimeFromNow := getGodisRunningTime()
	s := fmt.Sprintf("# Server\r\n"+
		"godis_version:%s\r\n"+
		//"godis_git_sha1:%s\r\n"+
		//"godis_git_dirty:%d\r\n"+
		//"godis_build_id:%s\r\n"+
		"godis_mode:%s\r\n"+
		"os:%s %s\r\n"+
		"arch_bits:%d\r\n"+
		//"multiplexing_api:%s\r\n"+
		"go_version:%s\r\n"+
		"process_id:%d\r\n"+
		"run_id:%s\r\n"+
		"tcp_port:%d\r\n"+
		"uptime_in_seconds:%d\r\n"+
		"uptime_in_days:%d\r\n"+
		//"hz:%d\r\n"+
		//"lru_clock:%d\r\n"+
		"config_file:%s\r\n",
		godisVersion,
		//TODO,
		//TODO,
		//TODO,
		getGodisRunningMode(),
		runtime.GOOS, runtime.GOARCH,
		32<<(^uint(0)>>63),
		//TODO,
		runtime.Version(),
		os.Getpid(),
		config.Properties.RunID,
		config.Properties.Port,
		startUpTimeFromNow,
		startUpTimeFromNow/time.Duration(3600*24),
		//TODO,
		//TODO,
		config.Properties.CfPath)
	return []byte(s)
}

// getGodisRunningMode return godis running mode
func getGodisRunningMode() string {
	if config.Properties.ClusterEnabled == "yes" {
		return config.ClusterMode
	} else {
		return config.StandaloneMode
	}
}

// getGodisRunningTime return the running time of godis
func getGodisRunningTime() time.Duration {
	return time.Since(config.EachTimeServerInfo.StartUpTime) / time.Second
}
