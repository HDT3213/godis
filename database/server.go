package database

import (
	"fmt"
	"github.com/hdt3213/godis/aof"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/pubsub"
	"github.com/hdt3213/godis/redis/protocol"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Server is a redis-server with full capabilities including multiple database, rdb loader, replication
type Server struct {
	dbSet []*atomic.Value // *DB

	// handle publish/subscribe
	hub *pubsub.Hub
	// handle aof persistence
	persister *aof.Persister

	// for replication
	role         int32
	slaveStatus  *slaveStatus
	masterStatus *masterStatus
}

// NewStandaloneServer creates a standalone redis server, with multi database and all other funtions
func NewStandaloneServer() *Server {
	server := &Server{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	server.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range server.dbSet {
		singleDB := makeDB()
		singleDB.index = i
		holder := &atomic.Value{}
		holder.Store(singleDB)
		server.dbSet[i] = holder
	}
	server.hub = pubsub.MakeHub()
	validAof := false
	if config.Properties.AppendOnly {
		aofHandler, err := NewPersister(server,
			config.Properties.AppendFilename, true, config.Properties.AppendFsync)
		if err != nil {
			panic(err)
		}
		server.bindPersister(aofHandler)
		validAof = true
	}
	if config.Properties.RDBFilename != "" && !validAof {
		// load rdb
		err := server.loadRdbFile()
		if err != nil {
			logger.Error(err)
		}
	}
	server.slaveStatus = initReplSlaveStatus()
	server.initMaster()
	server.startReplCron()
	server.role = masterRole // The initialization process does not require atomicity
	return server
}

// Exec executes command
// parameter `cmdLine` contains command and its arguments, for example: "set key value"
func (server *Server) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	// ping
	if cmdName == "ping" {
		return Ping(c, cmdLine[1:])
	}
	// authenticate
	if cmdName == "auth" {
		return Auth(c, cmdLine[1:])
	}
	if !isAuthenticated(c) {
		return protocol.MakeErrReply("NOAUTH Authentication required")
	}
	if cmdName == "slaveof" {
		if c != nil && c.InMultiState() {
			return protocol.MakeErrReply("cannot use slave of database within multi")
		}
		if len(cmdLine) != 3 {
			return protocol.MakeArgNumErrReply("SLAVEOF")
		}
		return server.execSlaveOf(c, cmdLine[1:])
	}

	// read only slave
	role := atomic.LoadInt32(&server.role)
	if role == slaveRole && !c.IsMaster() {
		// only allow read only command, forbid all special commands except `auth` and `slaveof`
		if !isReadOnlyCommand(cmdName) {
			return protocol.MakeErrReply("READONLY You can't write against a read only slave.")
		}
	}

	// special commands which cannot execute within transaction
	if cmdName == "subscribe" {
		if len(cmdLine) < 2 {
			return protocol.MakeArgNumErrReply("subscribe")
		}
		return pubsub.Subscribe(server.hub, c, cmdLine[1:])
	} else if cmdName == "publish" {
		return pubsub.Publish(server.hub, cmdLine[1:])
	} else if cmdName == "unsubscribe" {
		return pubsub.UnSubscribe(server.hub, c, cmdLine[1:])
	} else if cmdName == "bgrewriteaof" {
		// aof.go imports router.go, router.go cannot import BGRewriteAOF from aof.go
		return BGRewriteAOF(server, cmdLine[1:])
	} else if cmdName == "rewriteaof" {
		return RewriteAOF(server, cmdLine[1:])
	} else if cmdName == "flushall" {
		return server.flushAll()
	} else if cmdName == "flushdb" {
		if !validateArity(1, cmdLine) {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		if c.InMultiState() {
			return protocol.MakeErrReply("ERR command 'FlushDB' cannot be used in MULTI")
		}
		return server.flushDB(c.GetDBIndex())
	} else if cmdName == "save" {
		return SaveRDB(server, cmdLine[1:])
	} else if cmdName == "bgsave" {
		return BGSaveRDB(server, cmdLine[1:])
	} else if cmdName == "select" {
		if c != nil && c.InMultiState() {
			return protocol.MakeErrReply("cannot select database within multi")
		}
		if len(cmdLine) != 2 {
			return protocol.MakeArgNumErrReply("select")
		}
		return execSelect(c, server, cmdLine[1:])
	} else if cmdName == "copy" {
		if len(cmdLine) < 3 {
			return protocol.MakeArgNumErrReply("copy")
		}
		return execCopy(server, c, cmdLine[1:])
	} else if cmdName == "replconf" {
		return server.execReplConf(c, cmdLine[1:])
	} else if cmdName == "psync" {
		return server.execPSync(c, cmdLine[1:])
	}
	// todo: support multi database transaction

	// normal commands
	dbIndex := c.GetDBIndex()
	selectedDB, errReply := server.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}
	return selectedDB.Exec(c, cmdLine)
}

// AfterClientClose does some clean after client close connection
func (server *Server) AfterClientClose(c redis.Connection) {
	pubsub.UnsubscribeAll(server.hub, c)
}

// Close graceful shutdown database
func (server *Server) Close() {
	// stop slaveStatus first
	server.slaveStatus.close()
	if server.persister != nil {
		server.persister.Close()
	}
	server.stopMaster()
}

func execSelect(c redis.Connection, mdb *Server, args [][]byte) redis.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(mdb.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return protocol.MakeOkReply()
}

func (server *Server) flushDB(dbIndex int) redis.Reply {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	newDB := makeDB()
	server.loadDB(dbIndex, newDB)
	return &protocol.OkReply{}
}

func (server *Server) loadDB(dbIndex int, newDB *DB) redis.Reply {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	oldDB := server.mustSelectDB(dbIndex)
	newDB.index = dbIndex
	newDB.addAof = oldDB.addAof // inherit oldDB
	server.dbSet[dbIndex].Store(newDB)
	return &protocol.OkReply{}
}

func (server *Server) flushAll() redis.Reply {
	for i := range server.dbSet {
		server.flushDB(i)
	}
	if server.persister != nil {
		server.persister.SaveCmdLine(0, utils.ToCmdLine("FlushAll"))
	}
	return &protocol.OkReply{}
}

func (server *Server) selectDB(dbIndex int) (*DB, *protocol.StandardErrReply) {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return nil, protocol.MakeErrReply("ERR DB index is out of range")
	}
	return server.dbSet[dbIndex].Load().(*DB), nil
}

func (server *Server) mustSelectDB(dbIndex int) *DB {
	selectedDB, err := server.selectDB(dbIndex)
	if err != nil {
		panic(err)
	}
	return selectedDB
}

// ForEach traverses all the keys in the given database
func (server *Server) ForEach(dbIndex int, cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	server.mustSelectDB(dbIndex).ForEach(cb)
}

// ExecMulti executes multi commands transaction Atomically and Isolated
func (server *Server) ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	selectedDB, errReply := server.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return selectedDB.ExecMulti(conn, watching, cmdLines)
}

// RWLocks lock keys for writing and reading
func (server *Server) RWLocks(dbIndex int, writeKeys []string, readKeys []string) {
	server.mustSelectDB(dbIndex).RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
func (server *Server) RWUnLocks(dbIndex int, writeKeys []string, readKeys []string) {
	server.mustSelectDB(dbIndex).RWUnLocks(writeKeys, readKeys)
}

// GetUndoLogs return rollback commands
func (server *Server) GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine {
	return server.mustSelectDB(dbIndex).GetUndoLogs(cmdLine)
}

// ExecWithLock executes normal commands, invoker should provide locks
func (server *Server) ExecWithLock(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	db, errReply := server.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return db.execWithLock(cmdLine)
}

// BGRewriteAOF asynchronously rewrites Append-Only-File
func BGRewriteAOF(db *Server, args [][]byte) redis.Reply {
	go db.persister.Rewrite()
	return protocol.MakeStatusReply("Background append only file rewriting started")
}

// RewriteAOF start Append-Only-File rewriting and blocked until it finished
func RewriteAOF(db *Server, args [][]byte) redis.Reply {
	err := db.persister.Rewrite()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeOkReply()
}

// SaveRDB start RDB writing and blocked until it finished
func SaveRDB(db *Server, args [][]byte) redis.Reply {
	if db.persister == nil {
		return protocol.MakeErrReply("please enable aof before using save")
	}
	rdbFilename := config.Properties.RDBFilename
	if rdbFilename == "" {
		rdbFilename = "dump.rdb"
	}
	err := db.persister.Rewrite2RDB(rdbFilename)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeOkReply()
}

// BGSaveRDB asynchronously save RDB
func BGSaveRDB(db *Server, args [][]byte) redis.Reply {
	if db.persister == nil {
		return protocol.MakeErrReply("please enable aof before using save")
	}
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Error(err)
			}
		}()
		rdbFilename := config.Properties.RDBFilename
		if rdbFilename == "" {
			rdbFilename = "dump.rdb"
		}
		err := db.persister.Rewrite2RDB(rdbFilename)
		if err != nil {
			logger.Error(err)
		}
	}()
	return protocol.MakeStatusReply("Background saving started")
}

// GetDBSize returns keys count and ttl key count
func (server *Server) GetDBSize(dbIndex int) (int, int) {
	db := server.mustSelectDB(dbIndex)
	return db.data.Len(), db.ttlMap.Len()
}

func (server *Server) startReplCron() {
	go func(mdb *Server) {
		ticker := time.Tick(time.Second * 10)
		for range ticker {
			mdb.slaveCron()
			mdb.masterCron()
		}
	}(server)
}
