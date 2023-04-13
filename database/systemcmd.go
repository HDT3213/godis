package database

import (
	"fmt"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
	"github.com/hdt3213/godis/tcp"
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
func Info(c redis.Connection, args [][]byte, server interface{}) redis.Reply {
	if len(args) == 1 {
		infoCommandList := [...]string{"server", "client", "cluster", "keyspace"}
		var allSection []byte
		for _, s := range infoCommandList {
			allSection = append(allSection, GenGodisInfoString(s, c, server)...)
		}

		return protocol.MakeBulkReply(allSection)
	} else if len(args) == 2 {
		section := strings.ToLower(string(args[1]))
		switch section {
		case "server":
			reply := GenGodisInfoString("server", c, server)
			return protocol.MakeBulkReply(reply)
		case "client":
			return protocol.MakeBulkReply(GenGodisInfoString("client", c, server))
		case "cluster":
			return protocol.MakeBulkReply(GenGodisInfoString("cluster", c, server))
		case "keyspace":
			return protocol.MakeBulkReply(GenGodisInfoString("keyspace", c, server))

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

func GenGodisInfoString(section string, c redis.Connection, server interface{}) []byte {
	startUpTimeFromNow := getGodisRuninngTime()
	switch section {
	case "server":
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
	case "client":
		s := fmt.Sprintf("# Clients\r\n"+
			"connected_clients:%d\r\n",
			//"client_recent_max_input_buffer:%d\r\n"+
			//"client_recent_max_output_buffer:%d\r\n"+
			//"blocked_clients:%d\n",
			tcp.ClientCounter,
			//TODO,
			//TODO,
			//TODO,
		)
		return []byte(s)
	case "cluster":
		if getGodisRunningMode() == config.ClusterMode {
			s := fmt.Sprintf("# Cluster\r\n"+
				"cluster_enabled:%s\r\n",
				"1",
			)
			return []byte(s)
		} else {
			s := fmt.Sprintf("# Cluster\r\n"+
				"cluster_enabled:%s\r\n",
				"0",
			)
			return []byte(s)
		}
	case "keyspace":
		dbCount := config.Properties.Databases
		var serv []byte
		if ser, ok := server.(*Server); ok {
			for i := 0; i < dbCount; i++ {
				keys, expiresKeys := ser.GetDBSize(i)
				if keys != 0 {
					ttlSampleAverage := getRandomKeyCountingTTL(ser, i, keys, 20)
					serv = append(serv, getDbSize(i, keys, expiresKeys, ttlSampleAverage)...)
				}
			}
			prefix := []byte("# Keyspace\r\n")
			keyspaceInfo := append(prefix, serv...)
			return keyspaceInfo
		}
	}

	return []byte("")
}

// getGodisRunningMode return godis running mode
func getGodisRunningMode() string {
	if config.Properties.ClusterEnabled == "yes" {
		return config.ClusterMode
	} else {
		return config.StandaloneMode
	}
}

// getGodisRuninngTime return the running time of godis
func getGodisRuninngTime() time.Duration {
	return time.Since(config.EachTimeServerInfo.StartUpTime) / time.Second
}

func getDbSize(dbIndex, keys, expiresKeys int, ttl int64) []byte {
	s := fmt.Sprintf("db%d:keys=%d,expires=%d,avg_ttl=%d\r\n",
		dbIndex, keys, expiresKeys, ttl)
	return []byte(s)
}

// getRandomKeyCountingTTL Calculate the average expiration time of keys
// containing expiration time by randomly sampling all keys
func getRandomKeyCountingTTL(s *Server, dbIndex, keys, sample int) int64 {
	var ttlCount int64
	// if there is no data in the database, avg_ttl is 0
	if keys == 0 {
		return 0
	}
	if sample == 0 && keys > activeExpireCycleLookupsPreLoop {
		sample = activeExpireCycleLookupsPreLoop
	} else {
		sample = keys
	}
	db, err := s.selectDB(dbIndex)
	if err != nil {
		return 0
	}
	key := db.data.RandomKeys(sample)
	for _, k := range key {
		t := time.Now()
		rawExpireTime, ok := db.ttlMap.Get(k)
		if !ok {
			continue
		}
		expireTime, _ := rawExpireTime.(time.Time)
		// if the key has already reached its expiration time during calculation, ignore it
		if expireTime.Sub(t).Microseconds() > 0 {
			ttlCount += expireTime.Sub(t).Microseconds()
		}
	}
	return ttlCount / int64(sample)
}
