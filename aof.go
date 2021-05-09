package godis

import (
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/datastruct/dict"
	List "github.com/hdt3213/godis/datastruct/list"
	"github.com/hdt3213/godis/datastruct/lock"
	"github.com/hdt3213/godis/datastruct/set"
	SortedSet "github.com/hdt3213/godis/datastruct/sortedset"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/reply"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

var pExpireAtCmd = []byte("PEXPIREAT")

func makeExpireCmd(key string, expireAt time.Time) *reply.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = pExpireAtCmd
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))
	return reply.MakeMultiBulkReply(args)
}

func makeAofCmd(cmd string, args [][]byte) *reply.MultiBulkReply {
	params := make([][]byte, len(args)+1)
	copy(params[1:], args)
	params[0] = []byte(cmd)
	return reply.MakeMultiBulkReply(params)
}

// AddAof send command to aof goroutine through channel
func (db *DB) AddAof(args *reply.MultiBulkReply) {
	// aofChan == nil when loadAof
	if config.Properties.AppendOnly && db.aofChan != nil {
		db.aofChan <- args
	}
}

// handleAof listen aof channel and write into file
func (db *DB) handleAof() {
	for cmd := range db.aofChan {
		// todo: use switch and channels instead of mutex
		db.pausingAof.RLock() // prevent other goroutines from pausing aof
		if db.aofRewriteBuffer != nil {
			// replica during rewrite
			db.aofRewriteBuffer <- cmd
		}
		_, err := db.aofFile.Write(cmd.ToBytes())
		if err != nil {
			logger.Warn(err)
		}
		db.pausingAof.RUnlock()
	}
	db.aofFinished <- struct{}{}
}

// loadAof read aof file
func (db *DB) loadAof(maxBytes int) {
	// delete aofChan to prevent write again
	aofChan := db.aofChan
	db.aofChan = nil
	defer func(aofChan chan *reply.MultiBulkReply) {
		db.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(db.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	reader := utils.NewLimitedReader(file, maxBytes)
	ch := parser.ParseStream(reader)
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		cmd := strings.ToLower(string(r.Args[0]))
		cmdFunc, ok := router[cmd]
		if ok {
			cmdFunc(db, r.Args[1:])
		}
	}
}

/*-- aof rewrite --*/
func (db *DB) aofRewrite() {
	file, fileSize, err := db.startRewrite()
	if err != nil {
		logger.Warn(err)
		return
	}

	// load aof file
	tmpDB := &DB{
		data:   dict.MakeSimple(),
		ttlMap: dict.MakeSimple(),
		locker: lock.Make(lockerSize),

		aofFilename: db.aofFilename,
	}
	tmpDB.loadAof(int(fileSize))

	// rewrite aof file
	tmpDB.data.ForEach(func(key string, raw interface{}) bool {
		entity, _ := raw.(*DataEntity)
		cmd := EntityToCmd(key, entity)
		if cmd != nil {
			_, _ = file.Write(cmd.ToBytes())
		}
		return true
	})
	tmpDB.ttlMap.ForEach(func(key string, raw interface{}) bool {
		expireTime, _ := raw.(time.Time)
		cmd := makeExpireCmd(key, expireTime)
		if cmd != nil {
			_, _ = file.Write(cmd.ToBytes())
		}
		return true
	})

	db.finishRewrite(file)
}

var setCmd = []byte("SET")

func stringToCmd(key string, bytes []byte) *reply.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = bytes
	return reply.MakeMultiBulkReply(args)
}

var rPushAllCmd = []byte("RPUSH")

func listToCmd(key string, list *List.LinkedList) *reply.MultiBulkReply {
	args := make([][]byte, 2+list.Len())
	args[0] = rPushAllCmd
	args[1] = []byte(key)
	list.ForEach(func(i int, val interface{}) bool {
		bytes, _ := val.([]byte)
		args[2+i] = bytes
		return true
	})
	return reply.MakeMultiBulkReply(args)
}

var sAddCmd = []byte("SADD")

func setToCmd(key string, set *set.Set) *reply.MultiBulkReply {
	args := make([][]byte, 2+set.Len())
	args[0] = sAddCmd
	args[1] = []byte(key)
	i := 0
	set.ForEach(func(val string) bool {
		args[2+i] = []byte(val)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(args)
}

var hMSetCmd = []byte("HMSET")

func hashToCmd(key string, hash dict.Dict) *reply.MultiBulkReply {
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
	return reply.MakeMultiBulkReply(args)
}

var zAddCmd = []byte("ZADD")

func zSetToCmd(key string, zset *SortedSet.SortedSet) *reply.MultiBulkReply {
	args := make([][]byte, 2+zset.Len()*2)
	args[0] = zAddCmd
	args[1] = []byte(key)
	i := 0
	zset.ForEach(int64(0), int64(zset.Len()), true, func(element *SortedSet.Element) bool {
		value := strconv.FormatFloat(element.Score, 'f', -1, 64)
		args[2+i*2] = []byte(value)
		args[3+i*2] = []byte(element.Member)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(args)
}

// EntityToCmd serialize data entity to redis command
func EntityToCmd(key string, entity *DataEntity) *reply.MultiBulkReply {
	if entity == nil {
		return nil
	}
	var cmd *reply.MultiBulkReply
	switch val := entity.Data.(type) {
	case []byte:
		cmd = stringToCmd(key, val)
	case *List.LinkedList:
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

func (db *DB) startRewrite() (*os.File, int64, error) {
	db.pausingAof.Lock() // pausing aof
	defer db.pausingAof.Unlock()

	err := db.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, 0, err
	}
	// create rewrite channel
	db.aofRewriteBuffer = make(chan *reply.MultiBulkReply, aofQueueSize)

	// get current aof file size
	fileInfo, _ := os.Stat(db.aofFilename)
	filesize := fileInfo.Size()

	// create tmp file
	file, err := ioutil.TempFile("", "aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, 0, err
	}
	return file, filesize, nil
}

func (db *DB) finishRewrite(tmpFile *os.File) {
	db.pausingAof.Lock() // pausing aof
	defer db.pausingAof.Unlock()

	// write commands created during rewriting to tmp file
loop:
	for {
		// aof is pausing, there won't be any new commands in aofRewriteBuffer
		select {
		case cmd := <-db.aofRewriteBuffer:
			_, err := tmpFile.Write(cmd.ToBytes())
			if err != nil {
				logger.Warn(err)
			}
		default:
			// channel is empty, break loop
			break loop
		}
	}
	close(db.aofRewriteBuffer)
	db.aofRewriteBuffer = nil

	// replace current aof file by tmp file
	_ = db.aofFile.Close()
	_ = os.Rename(tmpFile.Name(), db.aofFilename)

	// reopen aof file for further write
	aofFile, err := os.OpenFile(db.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	db.aofFile = aofFile
}
