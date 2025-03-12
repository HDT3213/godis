package core

import (
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/redis/protocol"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// CmdFunc represents the handler of a redis command
type CmdFunc func(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply

var commands = make(map[string]CmdFunc)

// RegisterCmd add command handler into cluster
func RegisterCmd(name string, cmd CmdFunc) {
	name = strings.ToLower(name)
	commands[name] = cmd
}

// Exec executes command on cluster
func (cluster *Cluster) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "auth" {
		return database.Auth(c, cmdLine[1:])
	}
	if !isAuthenticated(c) {
		return protocol.MakeErrReply("NOAUTH Authentication required")
	}
	cmdFunc, ok := commands[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}
	return cmdFunc(cluster, c, cmdLine)
}

func isAuthenticated(c redis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}

func RegisterDefaultCmd(name string) {
	RegisterCmd(name, DefaultFunc)
}

// relay command to responsible peer, and return its protocol to client
func DefaultFunc(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	key := string(args[1])
	slotId := cluster.GetSlot(key)
	peer := cluster.PickNode(slotId)
	if peer == cluster.SelfID() {
		// to self db
		//return cluster.db.Exec(c, cmdLine)
		return cluster.db.Exec(c, args)
	}
	return cluster.Relay(peer, c, args)
}
