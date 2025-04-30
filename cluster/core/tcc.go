package core

import (
	"strings"
	"sync"
	"time"

	"github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/timewheel"
	"github.com/hdt3213/godis/redis/protocol"
)

// transaction info will be deleted after transactionTTL since commit
const transactionTTL = time.Minute

type TransactionManager struct {
	txs map[string]*TCC
	mu  sync.RWMutex
}

type TCC struct {
	realCmdLine CmdLine
	undoLogs    []CmdLine
	writeKeys   []string
	readKeys    []string
	hasLock     bool
}

func newTransactionManager() *TransactionManager {
	return &TransactionManager{
		txs: make(map[string]*TCC),
	}
}

func init() {
	RegisterCmd("prepare", execPrepare)
	RegisterCmd("commit", execCommit)
	RegisterCmd("rollback", execRollback)

}

// execPrepare executes prepare command
// commandline: prepare txid realCmd realArgs...
// execPrepare will check transaction preconditions, lock related keys and prepare undo logs
func execPrepare(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeArgNumErrReply("prepare")
	}
	txId := string(cmdLine[1])
	realCmdLine := cmdLine[2:]

	// create transaction
	cluster.transactions.mu.Lock()
	tx := cluster.transactions.txs[txId]
	if tx != nil {
		cluster.transactions.mu.Unlock()
		return protocol.MakeErrReply("transaction existed")
	}
	tx = &TCC{}
	cluster.transactions.txs[txId] = tx
	cluster.transactions.mu.Unlock()

	// prepare lock and undo locks
	tx.writeKeys, tx.readKeys = database.GetRelatedKeys(realCmdLine)
	cluster.db.RWLocks(0, tx.writeKeys, tx.readKeys)
	tx.undoLogs = cluster.db.GetUndoLogs(0, realCmdLine)
	tx.realCmdLine = realCmdLine
	tx.hasLock = true

	// execute prepare func
	prepareFunc := prepareFuncs[strings.ToLower(string(realCmdLine[0]))]
	var result redis.Reply
	if prepareFunc != nil {
		result = prepareFunc(cluster, c, realCmdLine)
	} else {
		result = protocol.MakeOkReply()
	}
	if protocol.IsErrorReply(result) {
		// prepare for rollback
		cluster.db.RWUnLocks(0, tx.writeKeys, tx.readKeys)
		tx.hasLock = false
	}
	return result
}

func execCommit(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeArgNumErrReply("commit")
	}
	txId := string(cmdLine[1])

	cluster.transactions.mu.Lock()
	tx := cluster.transactions.txs[txId]
	cluster.transactions.mu.Unlock()
	if tx == nil {
		return protocol.MakeErrReply("transaction not found")
	}

	resp := cluster.db.ExecWithLock(c, tx.realCmdLine)

	// unlock regardless of result
	cluster.db.RWUnLocks(0, tx.writeKeys, tx.readKeys)
	tx.hasLock = false

	if protocol.IsErrorReply(resp) {
		// do not delete transaction, waiting rollback
		return resp
	}

	// delete transaction after deadline
	timewheel.At(time.Now().Add(transactionTTL), txId, func() {
		cluster.transactions.mu.Lock()
		delete(cluster.transactions.txs, txId)
		cluster.transactions.mu.Unlock()
	})
	return resp
}

func execRollback(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeArgNumErrReply("rollback")
	}
	txId := string(cmdLine[1])

	// get transaction
	cluster.transactions.mu.Lock()
	tx := cluster.transactions.txs[txId]
	cluster.transactions.mu.Unlock()
	if tx == nil {
		return protocol.MakeErrReply("transaction not found")
	}

	// rollback
	if !tx.hasLock {
		cluster.db.RWLocks(0, tx.writeKeys, tx.readKeys)
		tx.hasLock = true
	}
	for i := len(tx.undoLogs) - 1; i >= 0; i-- {
		cmdline := tx.undoLogs[i]
		cluster.db.ExecWithLock(c, cmdline)
	}
	cluster.db.RWUnLocks(0, tx.writeKeys, tx.readKeys)

	// delete transaction
	cluster.transactions.mu.Lock()
	delete(cluster.transactions.txs, txId)
	cluster.transactions.mu.Unlock()

	return protocol.MakeOkReply()
}

var prepareFuncs = make(map[string]CmdFunc)

// RegisterCmd add tcc preparing validator
// prepareFunc will be executed within lock
// If prepareFunc returns an error reply, the transaction should be rolledback-
func RegisterPrepareFunc(name string, fn CmdFunc) {
	name = strings.ToLower(name)
	prepareFuncs[name] = fn
}
