package aof

import (
	"context"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 16
)

const (
	// FsyncAlways do fsync for every command
	FsyncAlways = "always"
	// FsyncEverySec do fsync every second
	FsyncEverySec = "everysec"
	// FsyncNo lets operating system decides when to do fsync
	FsyncNo = "no"
)

type payload struct {
	cmdLine CmdLine
	dbIndex int
	wg      *sync.WaitGroup
}

// Listener will be called-back after receiving a aof payload
// with a listener we can forward the updates to slave nodes etc.
type Listener interface {
	// Callback will be called-back after receiving a aof payload
	Callback([]CmdLine)
}

// Persister receive msgs from channel and write to AOF file
type Persister struct {
	ctx         context.Context
	cancel      context.CancelFunc
	db          database.DBEngine
	tmpDBMaker  func() database.DBEngine
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	aofFsync    string
	// aof goroutine will send msg to main goroutine through this channel when aof tasks finished and ready to shut down
	aofFinished chan struct{}
	// pause aof for start/finish aof rewrite progress
	pausingAof sync.Mutex
	currentDB  int
	listeners  map[Listener]struct{}
	// reuse cmdLine buffer
	buffer []CmdLine
}

// NewPersister creates a new aof.Persister
func NewPersister(db database.DBEngine, filename string, load bool, fsync string, tmpDBMaker func() database.DBEngine) (*Persister, error) {
	persister := &Persister{}
	persister.aofFilename = filename
	persister.aofFsync = strings.ToLower(fsync)
	persister.db = db
	persister.tmpDBMaker = tmpDBMaker
	persister.currentDB = 0
	if load {
		persister.LoadAof(0)
	}
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	persister.aofFile = aofFile
	persister.aofChan = make(chan *payload, aofQueueSize)
	persister.aofFinished = make(chan struct{})
	persister.listeners = make(map[Listener]struct{})
	go func() {
		persister.listenCmd()
	}()
	ctx, cancel := context.WithCancel(context.Background())
	persister.ctx = ctx
	persister.cancel = cancel
	if persister.aofFsync == FsyncEverySec {
		persister.fsyncEverySecond()
	}
	return persister, nil
}

// RemoveListener removes a listener from aof handler, so we can close the listener
func (persister *Persister) RemoveListener(listener Listener) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	delete(persister.listeners, listener)
}

// SaveCmdLine send command to aof goroutine through channel
func (persister *Persister) SaveCmdLine(dbIndex int, cmdLine CmdLine) {
	// aofChan will be set as nil temporarily during load aof see Persister.LoadAof
	if persister.aofChan == nil {
		return
	}
	if persister.aofFsync == FsyncAlways {
		p := &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
		persister.writeAof(p)
		return
	}
	persister.aofChan <- &payload{
		cmdLine: cmdLine,
		dbIndex: dbIndex,
	}
}

// listenCmd listen aof channel and write into file
func (persister *Persister) listenCmd() {
	for p := range persister.aofChan {
		persister.writeAof(p)
	}
	persister.aofFinished <- struct{}{}
}

func (persister *Persister) writeAof(p *payload) {
	persister.buffer = persister.buffer[:0] // reuse underlying array
	persister.pausingAof.Lock()             // prevent other goroutines from pausing aof
	defer persister.pausingAof.Unlock()
	// ensure aof is in the right database
	if p.dbIndex != persister.currentDB {
		// select db
		selectCmd := utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))
		persister.buffer = append(persister.buffer, selectCmd)
		data := protocol.MakeMultiBulkReply(selectCmd).ToBytes()
		_, err := persister.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
			return // skip this command
		}
		persister.currentDB = p.dbIndex
	}
	// save command
	data := protocol.MakeMultiBulkReply(p.cmdLine).ToBytes()
	persister.buffer = append(persister.buffer, p.cmdLine)
	_, err := persister.aofFile.Write(data)
	if err != nil {
		logger.Warn(err)
	}
	for listener := range persister.listeners {
		listener.Callback(persister.buffer)
	}
	if persister.aofFsync == FsyncAlways {
		_ = persister.aofFile.Sync()
	}
}

// LoadAof read aof file, can only be used before Persister.listenCmd started
func (persister *Persister) LoadAof(maxBytes int) {
	// persister.db.Exec may call persister.addAof
	// delete aofChan to prevent loaded commands back into aofChan
	aofChan := persister.aofChan
	persister.aofChan = nil
	defer func(aofChan chan *payload) {
		persister.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(persister.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}
	ch := parser.ParseStream(reader)
	fakeConn := connection.NewFakeConn() // only used for save dbIndex
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
		r, ok := p.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		ret := persister.db.Exec(fakeConn, r.Args)
		if protocol.IsErrorReply(ret) {
			logger.Error("exec err", string(ret.ToBytes()))
		}
		if strings.ToLower(string(r.Args[0])) == "select" {
			// execSelect success, here must be no error
			dbIndex, err := strconv.Atoi(string(r.Args[1]))
			if err == nil {
				persister.currentDB = dbIndex
			}
		}
	}
}

// Close gracefully stops aof persistence procedure
func (persister *Persister) Close() {
	if persister.aofFile != nil {
		close(persister.aofChan)
		<-persister.aofFinished // wait for aof finished
		err := persister.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
	persister.cancel()
}

func (persister *Persister) fsyncEverySecond() {
	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				persister.pausingAof.Lock()
				if err := persister.aofFile.Sync(); err != nil {
					logger.Errorf("fsync failed: %v", err)
				}
				persister.pausingAof.Unlock()
			case <-persister.ctx.Done():
				return
			}
		}
	}()
}
