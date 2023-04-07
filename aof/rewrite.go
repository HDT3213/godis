package aof

import (
	"io"
	"os"
	"strconv"

	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

func (persister *Persister) newRewriteHandler() *Persister {
	h := &Persister{}
	h.aofFilename = persister.aofFilename
	h.db = persister.tmpDBMaker()
	return h
}

// RewriteCtx holds context of an AOF rewriting procedure
type RewriteCtx struct {
	tmpFile  *os.File // tmpFile is the file handler of aof tmpFile
	fileSize int64
	dbIdx    int // selected db index when startRewrite
}

// Rewrite carries out AOF rewrite
func (persister *Persister) Rewrite() error {
	ctx, err := persister.StartRewrite()
	if err != nil {
		return err
	}
	err = persister.DoRewrite(ctx)
	if err != nil {
		return err
	}

	persister.FinishRewrite(ctx)
	return nil
}

// DoRewrite actually rewrite aof file
// makes DoRewrite public for testing only, please use Rewrite instead
func (persister *Persister) DoRewrite(ctx *RewriteCtx) (err error) {
	// start rewrite
	if !config.Properties.AofUseRdbPreamble {
		logger.Info("generate aof preamble")
		err = persister.generateAof(ctx)
	} else {
		logger.Info("generate rdb preamble")
		err = persister.generateRDB(ctx)
	}
	return err
}

// StartRewrite prepares rewrite procedure
func (persister *Persister) StartRewrite() (*RewriteCtx, error) {
	// pausing aof
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()

	err := persister.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}

	// get current aof file size
	fileInfo, _ := os.Stat(persister.aofFilename)
	filesize := fileInfo.Size()

	// create tmp file
	file, err := os.CreateTemp(config.GetTmpDir(), "*.aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, err
	}
	return &RewriteCtx{
		tmpFile:  file,
		fileSize: filesize,
		dbIdx:    persister.currentDB,
	}, nil
}

// FinishRewrite finish rewrite procedure
func (persister *Persister) FinishRewrite(ctx *RewriteCtx) {
	persister.pausingAof.Lock() // pausing aof
	defer persister.pausingAof.Unlock()
	tmpFile := ctx.tmpFile

	// copy commands executed during rewriting to tmpFile
	errOccurs := func() bool {
		/* read write commands executed during rewriting */
		src, err := os.Open(persister.aofFilename)
		if err != nil {
			logger.Error("open aofFilename failed: " + err.Error())
			return true
		}
		defer func() {
			_ = src.Close()
			_ = tmpFile.Close()
		}()

		_, err = src.Seek(ctx.fileSize, 0)
		if err != nil {
			logger.Error("seek failed: " + err.Error())
			return true
		}
		// sync tmpFile's db index with online aofFile
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(ctx.dbIdx))).ToBytes()
		_, err = tmpFile.Write(data)
		if err != nil {
			logger.Error("tmp file rewrite failed: " + err.Error())
			return true
		}
		// copy data
		_, err = io.Copy(tmpFile, src)
		if err != nil {
			logger.Error("copy aof filed failed: " + err.Error())
			return true
		}
		return false
	}()
	if errOccurs {
		return
	}

	// replace current aof file by tmp file
	_ = persister.aofFile.Close()
	if err := os.Rename(tmpFile.Name(), persister.aofFilename); err != nil {
		logger.Warn(err)
	}
	// reopen aof file for further write
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	persister.aofFile = aofFile

	// write select command again to resume aof file selected db
	// it should have the same db index with  persister.currentDB
	data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(persister.currentDB))).ToBytes()
	_, err = persister.aofFile.Write(data)
	if err != nil {
		panic(err)
	}
}
