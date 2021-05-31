package godis

import (
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/reply"
	"strings"
)

func startMulti(db *DB, conn redis.Connection) redis.Reply {
	if conn.InMultiState() {
		return reply.MakeErrReply("ERR MULTI calls can not be nested")
	}
	conn.SetMultiState(true)
	return reply.MakeOkReply()
}

func enqueueCmd(db *DB, conn redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if cmd.prepare == nil {
		return reply.MakeErrReply("ERR command '" + cmdName + "' cannot be used in MULTI")
	}
	if !validateArity(cmd.arity, cmdLine) {
		// difference with redis: we won't enqueue command line with wrong arity
		return reply.MakeArgNumErrReply(cmdName)
	}
	conn.EnqueueCmd(cmdLine)
	return reply.MakeQueuedReply()
}

func execMulti(db *DB, conn redis.Connection) redis.Reply {
	if !conn.InMultiState() {
		return reply.MakeErrReply("ERR EXEC without MULTI")
	}
	defer conn.SetMultiState(false)
	cmdLines := conn.GetQueuedCmdLine()

	// prepare
	writeKeys := make([]string, 0) // may contains duplicate
	readKeys := make([]string, 0)
	for _, cmdLine := range cmdLines {
		cmdName := strings.ToLower(string(cmdLine[0]))
		cmd := cmdTable[cmdName]
		prepare := cmd.prepare
		write, read := prepare(cmdLine[1:])
		writeKeys = append(writeKeys, write...)
		readKeys = append(readKeys, read...)
	}
	db.RWLocks(writeKeys, readKeys)
	defer db.RWUnLocks(writeKeys, readKeys)

	// execute
	results := make([][]byte, 0, len(cmdLines))
	aborted := false
	undoCmdLines := make([][]CmdLine, 0, len(cmdLines))
	for _, cmdLine := range cmdLines {
		undoCmdLines = append(undoCmdLines, db.GetUndoLogs(cmdLine))
		result := db.ExecWithLock(cmdLine)
		if reply.IsErrorReply(result) {
			aborted = true
			// don't rollback failed commands
			undoCmdLines = undoCmdLines[:len(undoCmdLines)-1]
			break
		}
		results = append(results, result.ToBytes())
	}
	if !aborted {
		return reply.MakeMultiRawReply(results)
	}
	// undo if aborted
	size := len(undoCmdLines)
	for i := size - 1; i >= 0; i-- {
		curCmdLines := undoCmdLines[i]
		if len(curCmdLines) == 0 {
			continue
		}
		for _, cmdLine := range curCmdLines {
			db.ExecWithLock(cmdLine)
		}
	}
	return reply.MakeErrReply("EXECABORT Transaction discarded because of previous errors.")
}

func discardMulti(db *DB, conn redis.Connection) redis.Reply {
	if !conn.InMultiState() {
		return reply.MakeErrReply("ERR DISCARD without MULTI")
	}
	conn.ClearQueuedCmds()
	conn.SetMultiState(false)
	return reply.MakeQueuedReply()
}
