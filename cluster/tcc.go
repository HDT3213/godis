package cluster

import (
	"fmt"
	"github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/timewheel"
	"github.com/hdt3213/godis/redis/reply"
	"strconv"
	"sync"
	"time"
)

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

// t should contains Keys and Id field
func (tx *Transaction) prepare() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.writeKeys, tx.readKeys = database.GetRelatedKeys(tx.cmdLine)
	// lock writeKeys
	tx.lockKeys()

	// build undoLog
	tx.undoLog = tx.cluster.db.GetUndoLogs(tx.dbIndex, tx.cmdLine)
	tx.status = preparedStatus
	taskKey := genTaskKey(tx.id)
	timewheel.Delay(maxLockTime, taskKey, func() {
		if tx.status == preparedStatus { // rollback transaction uncommitted until expire
			logger.Info("abort transaction: " + tx.id)
			_ = tx.rollback()
		}
	})
	return nil
}

func (tx *Transaction) rollback() error {
	curStatus := tx.status
	tx.mu.Lock()
	defer tx.mu.Unlock()

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
		return reply.MakeErrReply("ERR wrong number of arguments for 'preparedel' command")
	}
	txID := string(cmdLine[1])
	tx := NewTransaction(cluster, c, txID, cmdLine[2:])
	cluster.transactions.Put(txID, tx)
	err := tx.prepare()
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	return &reply.OkReply{}
}

// execRollback rollbacks local transaction
func execRollback(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rollback' command")
	}
	txID := string(cmdLine[1])
	raw, ok := cluster.transactions.Get(txID)
	if !ok {
		return reply.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)
	err := tx.rollback()
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	// clean transaction
	timewheel.Delay(waitBeforeCleanTx, "", func() {
		cluster.transactions.Remove(tx.id)
	})
	return reply.MakeIntReply(1)
}

// execCommit commits local transaction as a worker when receive execCommit command from coordinator
func execCommit(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'commit' command")
	}
	txID := string(cmdLine[1])
	raw, ok := cluster.transactions.Get(txID)
	if !ok {
		return reply.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)

	tx.mu.Lock()
	defer tx.mu.Unlock()

	result := cluster.db.ExecWithLock(c, tx.cmdLine)

	if reply.IsErrorReply(result) {
		// failed
		err2 := tx.rollback()
		return reply.MakeErrReply(fmt.Sprintf("err occurs when rollback: %v, origin err: %s", err2, result))
	}
	// after committed
	tx.unLockKeys()
	tx.status = committedStatus
	// clean finished transaction
	// do not clean immediately, in case rollback
	timewheel.Delay(waitBeforeCleanTx, "", func() {
		cluster.transactions.Remove(tx.id)
	})
	return result
}

// requestCommit commands all node to commit transaction as coordinator
func requestCommit(cluster *Cluster, c redis.Connection, txID int64, peers map[string][]string) ([]redis.Reply, reply.ErrorReply) {
	var errReply reply.ErrorReply
	txIDStr := strconv.FormatInt(txID, 10)
	respList := make([]redis.Reply, 0, len(peers))
	for peer := range peers {
		var resp redis.Reply
		if peer == cluster.self {
			resp = execCommit(cluster, c, makeArgs("commit", txIDStr))
		} else {
			resp = cluster.relay(peer, c, makeArgs("commit", txIDStr))
		}
		if reply.IsErrorReply(resp) {
			errReply = resp.(reply.ErrorReply)
			break
		}
		respList = append(respList, resp)
	}
	if errReply != nil {
		requestRollback(cluster, c, txID, peers)
		return nil, errReply
	}
	return respList, nil
}

// requestRollback requests all node rollback transaction as coordinator
func requestRollback(cluster *Cluster, c redis.Connection, txID int64, peers map[string][]string) {
	txIDStr := strconv.FormatInt(txID, 10)
	for peer := range peers {
		if peer == cluster.self {
			execRollback(cluster, c, makeArgs("rollback", txIDStr))
		} else {
			cluster.relay(peer, c, makeArgs("rollback", txIDStr))
		}
	}
}
