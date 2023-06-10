package cluster

import (
	"fmt"
	"github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/timewheel"
	"github.com/hdt3213/godis/redis/protocol"
	"strconv"
	"strings"
	"sync"
	"time"
)

// prepareFunc executed after related key locked, and use additional logic to determine whether the transaction can be committed
// For example, prepareMSetNX  will return error to prevent MSetNx transaction from committing if any related key already exists
var prepareFuncMap = make(map[string]CmdFunc)

func registerPrepareFunc(cmdName string, fn CmdFunc) {
	prepareFuncMap[strings.ToLower(cmdName)] = fn
}

// Transaction stores state and data for a try-commit-catch distributed transaction
type Transaction struct {
	id      string   // transaction id
	cmdLine [][]byte // cmd cmdLine
	cluster *Cluster
	conn    redis.Connection
	dbIndex int

	writeKeys  []string
	readKeys   []string
	keysLocked bool
	undoLog    []CmdLine

	status int8
	mu     *sync.Mutex
}

const (
	maxLockTime       = 3 * time.Second
	waitBeforeCleanTx = 2 * maxLockTime

	createdStatus    = 0
	preparedStatus   = 1
	committedStatus  = 2
	rolledBackStatus = 3
)

func genTaskKey(txID string) string {
	return "tx:" + txID
}

// NewTransaction creates a try-commit-catch distributed transaction
func NewTransaction(cluster *Cluster, c redis.Connection, id string, cmdLine [][]byte) *Transaction {
	return &Transaction{
		id:      id,
		cmdLine: cmdLine,
		cluster: cluster,
		conn:    c,
		dbIndex: c.GetDBIndex(),
		status:  createdStatus,
		mu:      new(sync.Mutex),
	}
}

// Reentrant
// invoker should hold tx.mu
func (tx *Transaction) lockKeys() {
	if !tx.keysLocked {
		tx.cluster.db.RWLocks(tx.dbIndex, tx.writeKeys, tx.readKeys)
		tx.keysLocked = true
	}
}

func (tx *Transaction) unLockKeys() {
	if tx.keysLocked {
		tx.cluster.db.RWUnLocks(tx.dbIndex, tx.writeKeys, tx.readKeys)
		tx.keysLocked = false
	}
}

// t should contain Keys and ID field
func (tx *Transaction) prepare() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.writeKeys, tx.readKeys = database.GetRelatedKeys(tx.cmdLine)
	// lock writeKeys
	tx.lockKeys()

	for _, key := range tx.writeKeys {
		err := tx.cluster.ensureKey(key)
		if err != nil {
			return err
		}
	}
	for _, key := range tx.readKeys {
		err := tx.cluster.ensureKey(key)
		if err != nil {
			return err
		}
	}
	// build undoLog
	tx.undoLog = tx.cluster.db.GetUndoLogs(tx.dbIndex, tx.cmdLine)
	tx.status = preparedStatus
	taskKey := genTaskKey(tx.id)
	timewheel.Delay(maxLockTime, taskKey, func() {
		if tx.status == preparedStatus { // rollback transaction uncommitted until expire
			logger.Info("abort transaction: " + tx.id)
			tx.mu.Lock()
			defer tx.mu.Unlock()
			_ = tx.rollbackWithLock()
		}
	})
	return nil
}

func (tx *Transaction) rollbackWithLock() error {
	curStatus := tx.status

	if tx.status != curStatus { // ensure status not changed by other goroutine
		return fmt.Errorf("tx %s status changed", tx.id)
	}
	if tx.status == rolledBackStatus { // no need to rollback a rolled-back transaction
		return nil
	}
	tx.lockKeys()
	for _, cmdLine := range tx.undoLog {
		tx.cluster.db.ExecWithLock(tx.conn, cmdLine)
	}
	tx.unLockKeys()
	tx.status = rolledBackStatus
	return nil
}

// cmdLine: Prepare id cmdName args...
func execPrepare(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'prepare' command")
	}
	txID := string(cmdLine[1])
	cmdName := strings.ToLower(string(cmdLine[2]))
	tx := NewTransaction(cluster, c, txID, cmdLine[2:])
	cluster.transactionMu.Lock()
	cluster.transactions.Put(txID, tx)
	cluster.transactionMu.Unlock()
	err := tx.prepare()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	prepareFunc, ok := prepareFuncMap[cmdName]
	if ok {
		return prepareFunc(cluster, c, cmdLine[2:])
	}
	return &protocol.OkReply{}
}

// execRollback rollbacks local transaction
func execRollback(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rollback' command")
	}
	txID := string(cmdLine[1])
	cluster.transactionMu.RLock()
	raw, ok := cluster.transactions.Get(txID)
	cluster.transactionMu.RUnlock()
	if !ok {
		return protocol.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)

	tx.mu.Lock()
	defer tx.mu.Unlock()
	err := tx.rollbackWithLock()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	// clean transaction
	timewheel.Delay(waitBeforeCleanTx, "", func() {
		cluster.transactionMu.Lock()
		cluster.transactions.Remove(tx.id)
		cluster.transactionMu.Unlock()
	})
	return protocol.MakeIntReply(1)
}

// execCommit commits local transaction as a worker when receive execCommit command from coordinator
func execCommit(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'commit' command")
	}
	txID := string(cmdLine[1])
	cluster.transactionMu.RLock()
	raw, ok := cluster.transactions.Get(txID)
	cluster.transactionMu.RUnlock()
	if !ok {
		return protocol.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)

	tx.mu.Lock()
	defer tx.mu.Unlock()

	result := cluster.db.ExecWithLock(c, tx.cmdLine)

	if protocol.IsErrorReply(result) {
		// failed
		err2 := tx.rollbackWithLock()
		return protocol.MakeErrReply(fmt.Sprintf("err occurs when rollback: %v, origin err: %s", err2, result))
	}
	// after committed
	tx.unLockKeys()
	tx.status = committedStatus
	// clean finished transaction
	// do not clean immediately, in case rollback
	timewheel.Delay(waitBeforeCleanTx, "", func() {
		cluster.transactionMu.Lock()
		cluster.transactions.Remove(tx.id)
		cluster.transactionMu.Unlock()
	})
	return result
}

// requestCommit commands all node to commit transaction as coordinator
func requestCommit(cluster *Cluster, c redis.Connection, txID int64, groupMap map[string][]string) ([]redis.Reply, protocol.ErrorReply) {
	var errReply protocol.ErrorReply
	txIDStr := strconv.FormatInt(txID, 10)
	respList := make([]redis.Reply, 0, len(groupMap))
	for node := range groupMap {
		resp := cluster.relay(node, c, makeArgs("commit", txIDStr))
		if protocol.IsErrorReply(resp) {
			errReply = resp.(protocol.ErrorReply)
			break
		}
		respList = append(respList, resp)
	}
	if errReply != nil {
		requestRollback(cluster, c, txID, groupMap)
		return nil, errReply
	}
	return respList, nil
}

// requestRollback requests all node rollback transaction as coordinator
// groupMap: node -> keys
func requestRollback(cluster *Cluster, c redis.Connection, txID int64, groupMap map[string][]string) {
	txIDStr := strconv.FormatInt(txID, 10)
	for node := range groupMap {
		cluster.relay(node, c, makeArgs("rollback", txIDStr))
	}
}
