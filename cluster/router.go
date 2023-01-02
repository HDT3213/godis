package cluster

import (
	"github.com/hdt3213/godis/interface/redis"
	"strings"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

var router = make(map[string]CmdFunc)

func registerCmd(name string, cmd CmdFunc) {
	name = strings.ToLower(name)
	router[name] = cmd
}

func registerDefaultCmd(name string) {
	registerCmd(name, defaultFunc)
}

// relay command to responsible peer, and return its protocol to client
func defaultFunc(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	key := string(args[1])
	slotId := getSlot(key)
	peer := cluster.pickNode(slotId)
	if peer.ID == cluster.self {
		err := cluster.ensureKeyWithoutLock(key)
		if err != nil {
			return err
		}
		// to self db
		//return cluster.db.Exec(c, cmdLine)
		return cluster.db.Exec(c, args)
	}
	return cluster.relay(peer.ID, c, args)
}

func init() {
	registerCmd("Ping", ping)
	registerCmd("Prepare", execPrepare)
	registerCmd("Commit", execCommit)
	registerCmd("Rollback", execRollback)
	registerCmd("Del", Del)
	registerCmd("Rename", Rename)
	registerCmd("RenameNx", RenameNx)
	registerCmd("Copy", Copy)
	registerCmd("MSet", MSet)
	registerCmd("MGet", MGet)
	registerCmd("MSetNx", MSetNX)
	registerCmd("Publish", Publish)
	registerCmd("Subscribe", Subscribe)
	registerCmd("Unsubscribe", UnSubscribe)
	registerCmd("FlushDB", FlushDB)
	registerCmd("FlushAll", FlushAll)
	registerCmd(relayMulti, execRelayedMulti)
	registerCmd("Watch", execWatch)
	registerCmd("FlushDB_", genPenetratingExecutor("FlushDB"))
	registerCmd("Copy_", genPenetratingExecutor("Copy"))
	registerCmd("Watch_", genPenetratingExecutor("Watch"))
	registerCmd(relayPublish, genPenetratingExecutor("Publish"))
	registerCmd("Del_", genPenetratingExecutor("Del"))
	registerCmd("MSet_", genPenetratingExecutor("MSet"))
	registerCmd("MSetNx_", genPenetratingExecutor("MSetNx"))
	registerCmd("MGet_", genPenetratingExecutor("MGet"))
	registerCmd("Rename_", genPenetratingExecutor("Rename"))
	registerCmd("RenameNx_", genPenetratingExecutor("RenameNx"))
	registerCmd("DumpKey_", genPenetratingExecutor("DumpKey"))

	defaultCmds := []string{
		"expire",
		"expireAt",
		"pExpire",
		"pExpireAt",
		"ttl",
		"PTtl",
		"persist",
		"exists",
		"type",
		"set",
		"setNx",
		"setEx",
		"pSetEx",
		"get",
		"getEx",
		"getSet",
		"getDel",
		"incr",
		"incrBy",
		"incrByFloat",
		"decr",
		"decrBy",
		"lPush",
		"lPushX",
		"rPush",
		"rPushX",
		"LPop",
		"RPop",
		"LRem",
		"LLen",
		"LIndex",
		"LSet",
		"LRange",
		"HSet",
		"HSetNx",
		"HGet",
		"HExists",
		"HDel",
		"HLen",
		"HStrLen",
		"HMGet",
		"HMSet",
		"HKeys",
		"HVals",
		"HGetAll",
		"HIncrBy",
		"HIncrByFloat",
		"HRandField",
		"SAdd",
		"SIsMember",
		"SRem",
		"SPop",
		"SCard",
		"SMembers",
		"SInter",
		"SInterStore",
		"SUnion",
		"SUnionStore",
		"SDiff",
		"SDiffStore",
		"SRandMember",
		"ZAdd",
		"ZScore",
		"ZIncrBy",
		"ZRank",
		"ZCount",
		"ZRevRank",
		"ZCard",
		"ZRange",
		"ZRevRange",
		"ZRangeByScore",
		"ZRevRangeByScore",
		"ZRem",
		"ZRemRangeByScore",
		"ZRemRangeByRank",
		"GeoAdd",
		"GeoPos",
		"GeoDist",
		"GeoHash",
		"GeoRadius",
		"GeoRadiusByMember",
		"GetVer",
		"DumpKey",
	}
	for _, name := range defaultCmds {
		registerDefaultCmd(name)
	}

}

// genPenetratingExecutor generates an executor that can reach directly to the database layer
func genPenetratingExecutor(realCmd string) CmdFunc {
	return func(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
		return cluster.db.Exec(c, modifyCmd(cmdLine, realCmd))
	}
}
