package database

import (
	"github.com/hdt3213/godis/aof"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
)

// execExistIn returns existing key in given keys
// example: ExistIn key1 key2 key3..., returns [key1, key2]
// custom command for MSetNX tcc transaction
func execExistIn(db *DB, args [][]byte) redis.Reply {
	var result [][]byte
	for _, arg := range args {
		key := string(arg)
		_, exists := db.GetEntity(key)
		if exists {
			result = append(result, []byte(key))
		}
	}
	if len(result) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	return protocol.MakeMultiBulkReply(result)
}

// execDumpKey returns redis serialization protocol data of given key (see aof.EntityToCmd)
func execDumpKey(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	entity, ok := db.GetEntity(key)
	if !ok {
		return protocol.MakeEmptyMultiBulkReply()
	}
	dumpCmd := aof.EntityToCmd(key, entity)
	ttlCmd := toTTLCmd(db, key)
	resp := protocol.MakeMultiBulkReply([][]byte{
		dumpCmd.ToBytes(),
		ttlCmd.ToBytes(),
	})
	return resp
}

// execRenameFrom is exactly same as execDel, used for cluster.Rename
func execRenameFrom(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	db.Remove(key)
	return protocol.MakeOkReply()
}

// execRenameTo accepts result of execDumpKey and load the dumped key
// args format: key dumpCmd ttlCmd
// execRenameTo may be partially successful, do not use it without transaction
func execRenameTo(db *DB, args [][]byte) redis.Reply {
	key := args[0]
	dumpRawCmd, err := parser.ParseOne(args[1])
	if err != nil {
		return protocol.MakeErrReply("illegal dump cmd: " + err.Error())
	}
	dumpCmd, ok := dumpRawCmd.(*protocol.MultiBulkReply)
	if !ok {
		return protocol.MakeErrReply("dump cmd is not multi bulk reply")
	}
	dumpCmd.Args[1] = key // change key
	ttlRawCmd, err := parser.ParseOne(args[2])
	if err != nil {
		return protocol.MakeErrReply("illegal ttl cmd: " + err.Error())
	}
	ttlCmd, ok := ttlRawCmd.(*protocol.MultiBulkReply)
	if !ok {
		return protocol.MakeErrReply("ttl cmd is not multi bulk reply")
	}
	ttlCmd.Args[1] = key
	db.Remove(string(key))
	dumpResult := db.execWithLock(dumpCmd.Args)
	if protocol.IsErrorReply(dumpResult) {
		return dumpResult
	}
	ttlResult := db.execWithLock(ttlCmd.Args)
	if protocol.IsErrorReply(ttlResult) {
		return ttlResult
	}
	return protocol.MakeOkReply()
}

// execRenameNxTo is exactly same as execRenameTo, used for cluster.RenameNx, not exists check in cluster.prepareRenameNxTo
func execRenameNxTo(db *DB, args [][]byte) redis.Reply {
	return execRenameTo(db, args)
}

// execCopyFrom just reply "OK" message, used for cluster.Copy
func execCopyFrom(db *DB, args [][]byte) redis.Reply {
	return protocol.MakeOkReply()
}

// execCopyTo accepts result of execDumpKey and load the dumped key
// args format: key dumpCmd ttlCmd
// execCopyTo may be partially successful, do not use it without transaction
func execCopyTo(db *DB, args [][]byte) redis.Reply {
	key := args[0]
	dumpRawCmd, err := parser.ParseOne(args[1])
	if err != nil {
		return protocol.MakeErrReply("illegal dump cmd: " + err.Error())
	}
	dumpCmd, ok := dumpRawCmd.(*protocol.MultiBulkReply)
	if !ok {
		return protocol.MakeErrReply("dump cmd is not multi bulk reply")
	}
	dumpCmd.Args[1] = key // change key
	ttlRawCmd, err := parser.ParseOne(args[2])
	if err != nil {
		return protocol.MakeErrReply("illegal ttl cmd: " + err.Error())
	}
	ttlCmd, ok := ttlRawCmd.(*protocol.MultiBulkReply)
	if !ok {
		return protocol.MakeErrReply("ttl cmd is not multi bulk reply")
	}
	ttlCmd.Args[1] = key
	db.Remove(string(key))
	dumpResult := db.execWithLock(dumpCmd.Args)
	if protocol.IsErrorReply(dumpResult) {
		return dumpResult
	}
	ttlResult := db.execWithLock(ttlCmd.Args)
	if protocol.IsErrorReply(ttlResult) {
		return ttlResult
	}
	return protocol.MakeOkReply()
}

func init() {
	registerCommand("DumpKey", execDumpKey, writeAllKeys, undoDel, 2, flagReadOnly)
	registerCommand("ExistIn", execExistIn, readAllKeys, nil, -1, flagReadOnly)
	registerCommand("RenameFrom", execRenameFrom, readFirstKey, nil, 2, flagWrite)
	registerCommand("RenameTo", execRenameTo, writeFirstKey, rollbackFirstKey, 4, flagWrite)
	registerCommand("RenameNxTo", execRenameTo, writeFirstKey, rollbackFirstKey, 4, flagWrite)
	registerCommand("CopyFrom", execCopyFrom, readFirstKey, nil, 2, flagReadOnly)
	registerCommand("CopyTo", execCopyTo, writeFirstKey, rollbackFirstKey, 5, flagWrite)
}
