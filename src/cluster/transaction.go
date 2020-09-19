package cluster

import (
    "context"
    "github.com/HDT3213/godis/src/db"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/lib/marshal/gob"
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
    tx.cluster.db.UnLocks(tx.keys...)
    tx.status = RollbackedStatus
    return nil
}

//func (tx *Transaction) commit(cmd CmdFunc) error {
//    finished := make(chan int)
//    go func() {
//        cmd(tx.cluster, tx.conn, tx.args)
//        finished <- 1
//    }()
//    select {
//    case <- tx.ctx.Done():
//        return tx.rollback()
//    case <- finished:
//        tx.cluster.db.UnLocks(tx.keys...)
//    }
//}
