package godis

import (
	"fmt"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/pubsub"
	"github.com/hdt3213/godis/redis/reply"
	"runtime/debug"
	"strings"
)

// Exec executes command
// parameter `cmdArgs` contains command and its arguments, for example: "set key value"
func (db *DB) Exec(c redis.Connection, cmdArgs [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(cmdArgs[0]))
	// authenticate
	if cmdName == "auth" {
		return Auth(db, c, cmdArgs[1:])
	}
	if !isAuthenticated(c) {
		return reply.MakeErrReply("NOAUTH Authentication required")
	}

	// special commands
	if cmdName == "subscribe" {
		if len(cmdArgs) < 2 {
			return reply.MakeArgNumErrReply("subscribe")
		}
		return pubsub.Subscribe(db.hub, c, cmdArgs[1:])
	} else if cmdName == "publish" {
		return pubsub.Publish(db.hub, cmdArgs[1:])
	} else if cmdName == "unsubscribe" {
		return pubsub.UnSubscribe(db.hub, c, cmdArgs[1:])
	} else if cmdName == "bgrewriteaof" {
		// aof.go imports router.go, router.go cannot import BGRewriteAOF from aof.go
		return BGRewriteAOF(db, cmdArgs[1:])
	}

	// normal commands
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdArgs) {
		return reply.MakeArgNumErrReply(cmdName)
	}

	fun := cmd.executor
	if len(cmdArgs) > 1 {
		result = fun(db, cmdArgs[1:])
	} else {
		result = fun(db, [][]byte{})
	}
	return
}
