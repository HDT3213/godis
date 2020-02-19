package db

import (
	"bufio"
	"github.com/HDT3213/godis/src/datastruct/dict"
	List "github.com/HDT3213/godis/src/datastruct/list"
	"github.com/HDT3213/godis/src/datastruct/lock"
	"github.com/HDT3213/godis/src/datastruct/set"
	SortedSet "github.com/HDT3213/godis/src/datastruct/sortedset"
	"github.com/HDT3213/godis/src/lib/logger"
	"github.com/HDT3213/godis/src/redis/reply"
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

// send command to aof
func (db *DB) addAof(args *reply.MultiBulkReply) {
	db.aofChan <- args
}

// listen aof channel and write into file
func (db *DB) handleAof() {
	for cmd := range db.aofChan {
		db.pausingAof.RLock() // prevent other goroutines from pausing aof
		if db.aofRewriteChan != nil {
			// replica during rewrite
			db.aofRewriteChan <- cmd
		}
		_, err := db.aofFile.Write(cmd.ToBytes())
		if err != nil {
			logger.Warn(err)
		}
		db.pausingAof.RUnlock()
	}
}

func trim(msg []byte) string {
	trimed := ""
	for i := len(msg) - 1; i >= 0; i-- {
		if msg[i] == '\r' || msg[i] == '\n' {
			continue
		}
		trimed = string(msg[:i+1])
	}
	return trimed
}

// read aof file
func (db *DB) loadAof() {
	file, err := os.Open(db.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var fixedLen int64 = 0
	var expectedArgsCount uint32
	var receivedCount uint32
	var args [][]byte
	processing := false
	var msg []byte
	for {
		if fixedLen == 0 {
			msg, err = reader.ReadBytes('\n')
			if err == io.EOF {
				return
			}
			if len(msg) == 0 {
				logger.Warn("invalid format: line should end with \\r\\n")
				return
			}
		} else {
			msg = make([]byte, fixedLen+2)
			_, err = io.ReadFull(reader, msg)
			if err == io.EOF {
				return
			}
			if len(msg) == 0 {
				logger.Warn("invalid multibulk length")
				return
			}
			fixedLen = 0
		}
		if err != nil {
			logger.Warn(err)
			return
		}

		if !processing {
			// new request
			if msg[0] == '*' {
				// bulk multi msg
				expectedLine, err := strconv.ParseUint(trim(msg[1:]), 10, 32)
				if err != nil {
					logger.Warn(err)
					return
				}
				expectedArgsCount = uint32(expectedLine)
				receivedCount = 0
				processing = true
				args = make([][]byte, expectedLine)
			} else {
				logger.Warn("msg should start with '*'")
				return
			}
		} else {
			// receive following part of a request
			line := msg[0 : len(msg)-2]
			if line[0] == '$' {
				fixedLen, err = strconv.ParseInt(trim(line[1:]), 10, 64)
				if err != nil {
					logger.Warn(err)
					return
				}
				if fixedLen <= 0 {
					logger.Warn("invalid multibulk length")
					return
				}
			} else {
				args[receivedCount] = line
				receivedCount++
			}

			// if sending finished
			if receivedCount == expectedArgsCount {
				processing = false

				cmd := strings.ToLower(string(args[0]))
				cmdFunc, ok := router[cmd]
				if ok {
					cmdFunc(db, args[1:])
				}

				// finish
				expectedArgsCount = 0
				receivedCount = 0
				args = nil
			}
		}
	}
}

/*-- aof rewrite --*/
func (db *DB) aofRewrite() {
	file, err := db.startRewrite()
	if err != nil {
		logger.Warn(err)
		return
	}

	// load aof file
	tmpDB := &DB{
		Data:     dict.MakeSimple(),
		TTLMap:   dict.MakeSimple(),
		Locker:   lock.Make(lockerSize),
		interval: 5 * time.Second,

		aofFilename: db.aofFilename,
	}
	tmpDB.loadAof()

	// rewrite aof file
	tmpDB.Data.ForEach(func(key string, raw interface{}) bool {
		var cmd *reply.MultiBulkReply
		entity, _ := raw.(*DataEntity)
		switch val := entity.Data.(type) {
		case []byte:
			cmd = persistString(key, val)
		case *List.LinkedList:
			cmd = persistList(key, val)
		case *set.Set:
			cmd = persistSet(key, val)
		case dict.Dict:
			cmd = persistHash(key, val)
		case *SortedSet.SortedSet:
			cmd = persistZSet(key, val)

		}
		if cmd != nil {
			_, _ = file.Write(cmd.ToBytes())
		}
		return true
	})

	db.finishRewrite(file)
}

var setCmd = []byte("SET")

func persistString(key string, bytes []byte) *reply.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = bytes
	return reply.MakeMultiBulkReply(args)
}

var rPushAllCmd = []byte("RPUSHALL")

func persistList(key string, list *List.LinkedList) *reply.MultiBulkReply {
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

func persistSet(key string, set *set.Set) *reply.MultiBulkReply {
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

func persistHash(key string, hash dict.Dict) *reply.MultiBulkReply {
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

func persistZSet(key string, zset *SortedSet.SortedSet) *reply.MultiBulkReply {
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

func (db *DB) startRewrite() (*os.File, error) {
	db.pausingAof.Lock() // pausing aof
	defer db.pausingAof.Unlock()

	// create rewrite channel
	db.aofRewriteChan = make(chan *reply.MultiBulkReply, aofQueueSize)

	// create tmp file
	file, err := ioutil.TempFile("", "aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, err
	}
	return file, nil
}

func (db *DB) finishRewrite(tmpFile *os.File) {
	db.pausingAof.Lock() // pausing aof
	defer db.pausingAof.Unlock()

	// write commands created during rewriting to tmp file
loop:
	for {
		// aof is pausing, there won't be any new commands in aofRewriteChan
		select {
		case cmd := <-db.aofRewriteChan:
			_, err := tmpFile.Write(cmd.ToBytes())
			if err != nil {
				logger.Warn(err)
			}
		default:
			// channel is empty, break loop
			break loop
		}
	}
	close(db.aofRewriteChan)
	db.aofRewriteChan = nil

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
