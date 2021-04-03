package cluster

import (
	"context"
	"fmt"
	"github.com/HDT3213/godis/src/db"
	"github.com/HDT3213/godis/src/interface/redis"
	"github.com/HDT3213/godis/src/lib/marshal/gob"
	"github.com/HDT3213/godis/src/redis/reply"
	"strconv"
	"strings"
	"time"
)

type Transaction struct {
	id      string   // transaction id
	args    [][]byte // cmd args
	cluster *Cluster
	conn    redis.Connection

	keys    []string          // related keys
	undoLog map[string][]byte // store data for undoLog

	lockUntil time.Time
	ctx       context.Context
	cancel    context.CancelFunc
	status    int8
}

const (
	maxLockTime = 3 * time.Second

	CreatedStatus    = 0
	PreparedStatus   = 1
	CommitedStatus   = 2
	RollbackedStatus = 3
)

func NewTransaction(cluster *Cluster, c redis.Connection, id string, args [][]byte, keys []string) *Transaction {
	return &Transaction{
		id:      id,
		args:    args,
		cluster: cluster,
		conn:    c,
		keys:    keys,
		status:  CreatedStatus,
	}
}

// t should contains Keys field
func (tx *Transaction) prepare() error {
	// lock keys
	tx.cluster.db.Locks(tx.keys...)

	// use context to manage
	//tx.lockUntil = time.Now().Add(maxLockTime)
	//ctx, cancel := context.WithDeadline(context.Background(), tx.lockUntil)
	//tx.ctx = ctx
	//tx.cancel = cancel

	// build undoLog
	tx.undoLog = make(map[string][]byte)
	for _, key := range tx.keys {
		entity, ok := tx.cluster.db.Get(key)
		if ok {
			blob, err := gob.Marshal(entity)
			if err != nil {
				return err
			}
			tx.undoLog[key] = blob
		} else {
			tx.undoLog[key] = []byte{} // entity was nil, should be removed while rollback
		}
	}
	tx.status = PreparedStatus
	return nil
}

func (tx *Transaction) rollback() error {
	for key, blob := range tx.undoLog {
		if len(blob) > 0 {
			entity := &db.DataEntity{}
			err := gob.UnMarshal(blob, entity)
			if err != nil {
				return err
			}
			tx.cluster.db.Put(key, entity)
		} else {
			tx.cluster.db.Remove(key)
		}
	}
	if tx.status != CommitedStatus {
		tx.cluster.db.UnLocks(tx.keys...)
	}
	tx.status = RollbackedStatus
	return nil
}

// rollback local transaction
func Rollback(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rollback' command")
	}
	txId := string(args[1])
	raw, ok := cluster.transactions.Get(txId)
	if !ok {
		return reply.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)
	err := tx.rollback()
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	return reply.MakeIntReply(1)
}

// commit local transaction as a worker
func Commit(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'commit' command")
	}
	txId := string(args[1])
	raw, ok := cluster.transactions.Get(txId)
	if !ok {
		return reply.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)

	// finish transaction
	defer func() {
		cluster.db.UnLocks(tx.keys...)
		tx.status = CommitedStatus
		//cluster.transactions.Remove(tx.id) // cannot remove, may rollback after commit
	}()

	cmd := strings.ToLower(string(tx.args[0]))
	var result redis.Reply
	if cmd == "del" {
		result = CommitDel(cluster, c, tx)
	} else if cmd == "mset" {
		result = CommitMSet(cluster, c, tx)
	}

	if reply.IsErrorReply(result) {
		// failed
		err2 := tx.rollback()
		return reply.MakeErrReply(fmt.Sprintf("err occurs when rollback:  %v, origin err: %s", err2, result))
	}

	return result
}

// request all node commit transaction as leader
func RequestCommit(cluster *Cluster, c redis.Connection, txId int64, peers map[string][]string) ([]redis.Reply, reply.ErrorReply) {
	var errReply reply.ErrorReply
	txIdStr := strconv.FormatInt(txId, 10)
	respList := make([]redis.Reply, 0, len(peers))
	for peer := range peers {
		var resp redis.Reply
		if peer == cluster.self {
			resp = Commit(cluster, c, makeArgs("commit", txIdStr))
		} else {
			resp = cluster.Relay(peer, c, makeArgs("commit", txIdStr))
		}
		if reply.IsErrorReply(resp) {
			errReply = resp.(reply.ErrorReply)
			break
		}
		respList = append(respList, resp)
	}
	if errReply != nil {
		RequestRollback(cluster, c, txId, peers)
		return nil, errReply
	}
	return respList, nil
}

// request all node rollback transaction as leader
func RequestRollback(cluster *Cluster, c redis.Connection, txId int64, peers map[string][]string) {
	txIdStr := strconv.FormatInt(txId, 10)
	for peer := range peers {
		if peer == cluster.self {
			Rollback(cluster, c, makeArgs("rollback", txIdStr))
		} else {
			cluster.Relay(peer, c, makeArgs("rollback", txIdStr))
		}
	}
}
