package aof

import (
	"strconv"
	"time"

	"github.com/hdt3213/godis/datastruct/dict"
	List "github.com/hdt3213/godis/datastruct/list"
	"github.com/hdt3213/godis/datastruct/set"
	SortedSet "github.com/hdt3213/godis/datastruct/sortedset"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/redis/protocol"
)

// EntityToCmd serialize data entity to redis command
func EntityToCmd(key string, entity *database.DataEntity) *protocol.MultiBulkReply {
	if entity == nil {
		return nil
	}
	var cmd *protocol.MultiBulkReply
	switch val := entity.Data.(type) {
	case []byte:
		cmd = stringToCmd(key, val)
	case List.List:
		cmd = listToCmd(key, val)
	case *set.Set:
		cmd = setToCmd(key, val)
	case dict.Dict:
		cmd = hashToCmd(key, val)
	case *SortedSet.SortedSet:
		cmd = zSetToCmd(key, val)
	}
	return cmd
}

var setCmd = []byte("SET")

func stringToCmd(key string, bytes []byte) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = bytes
	return protocol.MakeMultiBulkReply(args)
}

var rPushAllCmd = []byte("RPUSH")

func listToCmd(key string, list List.List) *protocol.MultiBulkReply {
	args := make([][]byte, 2+list.Len())
	args[0] = rPushAllCmd
	args[1] = []byte(key)
	list.ForEach(func(i int, val interface{}) bool {
		bytes, _ := val.([]byte)
		args[2+i] = bytes
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

var sAddCmd = []byte("SADD")

func setToCmd(key string, set *set.Set) *protocol.MultiBulkReply {
	args := make([][]byte, 2+set.Len())
	args[0] = sAddCmd
	args[1] = []byte(key)
	i := 0
	set.ForEach(func(val string) bool {
		args[2+i] = []byte(val)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

var hMSetCmd = []byte("HMSET")

func hashToCmd(key string, hash dict.Dict) *protocol.MultiBulkReply {
	args := make([][]byte, 2+hash.Len()*2)
	args[0] = hMSetCmd
	args[1] = []byte(key)
	i := 0
	hash.ForEach(func(field string, val interface{}) bool {
		bytes, _ := val.([]byte)
		args[2+i*2] = []byte(field)
		args[3+i*2] = bytes
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

var zAddCmd = []byte("ZADD")

func zSetToCmd(key string, zset *SortedSet.SortedSet) *protocol.MultiBulkReply {
	args := make([][]byte, 2+zset.Len()*2)
	args[0] = zAddCmd
	args[1] = []byte(key)
	i := 0
	zset.ForEachByRank(int64(0), int64(zset.Len()), true, func(element *SortedSet.Element) bool {
		value := strconv.FormatFloat(element.Score, 'f', -1, 64)
		args[2+i*2] = []byte(value)
		args[3+i*2] = []byte(element.Member)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

var pExpireAtBytes = []byte("PEXPIREAT")

// MakeExpireCmd generates command line to set expiration for the given key
func MakeExpireCmd(key string, expireAt time.Time) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = pExpireAtBytes
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))
	return protocol.MakeMultiBulkReply(args)
}
