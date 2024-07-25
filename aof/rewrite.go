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

/*
通过创建一个临时文件并最终将其替换为主 AOF 文件，我们可以在不中断服务的情况下进行文件重写，同时保持数据的完整性和一致性。
这种方法避免了需要系统级 fork 操作的需求，适应了 Go 语言的运行时特性。
*/

// newRewriteHandler 创建一个新的重写处理器，用于在重写期间操作独立的数据库实例
func (persister *Persister) newRewriteHandler() *Persister {
	h := &Persister{}
	h.aofFilename = persister.aofFilename // 保留原始 AOF 文件名
	h.db = persister.tmpDBMaker()         // 创建一个临时的数据库实例，避免干扰现有数据
	return h
}

// RewriteCtx 保存 AOF 重写过程的上下文信息
type RewriteCtx struct {
	tmpFile  *os.File // tmpFile 是临时 AOF 文件的文件句柄
	fileSize int64    // 记录重写开始时原 AOF 文件的大小
	dbIdx    int      // 重写开始时选定的数据库索引
}

// Rewrite 执行AOF重写的整个流程
func (persister *Persister) Rewrite() error {
	ctx, err := persister.StartRewrite() // 准备重写操作
	if err != nil {
		return err
	}
	err = persister.DoRewrite(ctx) // 执行实际的重写操作
	if err != nil {
		return err
	}

	persister.FinishRewrite(ctx) // 完成重写，整理并关闭相关资源
	return nil
}

// DoRewrite 实际执行 AOF 文件的重写
func (persister *Persister) DoRewrite(ctx *RewriteCtx) (err error) {
	// start rewrite
	if !config.Properties.AofUseRdbPreamble {
		logger.Info("generate aof preamble")
		err = persister.generateAof(ctx) // 生成 AOF 前导数据，// generateAof 根据当前数据库状态生成新的AOF文件
	} else {
		logger.Info("generate rdb preamble")
		err = persister.generateRDB(ctx) // 生成 RDB 前导数据
	}
	return err
}

// StartRewrite 准备重写流程，主要包括锁定写操作和创建临时文件
func (persister *Persister) StartRewrite() (*RewriteCtx, error) {
	// pausing aof
	persister.pausingAof.Lock() // 锁定以暂停 AOF 写入
	defer persister.pausingAof.Unlock()
	err := persister.aofFile.Sync() // 同步现有 AOF 文件，确保所有数据都已写入， 将AOF文件的内容同步到磁盘
	if err != nil {
		logger.Warn("fsync failed") // 同步失败警告
		return nil, err
	}

	fileInfo, _ := os.Stat(persister.aofFilename) // 获取当前 AOF 文件的状态
	filesize := fileInfo.Size()                   //获取当前AOF文件的大小

	file, err := os.CreateTemp(config.GetTmpDir(), "*.aof") // 在临时目录中创建一个新的临时文件
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

// FinishRewrite 完成重写操作，整理资源并更新AOF文件
func (persister *Persister) FinishRewrite(ctx *RewriteCtx) {
	persister.pausingAof.Lock() // 再次锁定AOF写入，准备替换文件
	// 这一步确保在重写的最后阶段，没有其他写入操作会干扰文件替换过程。
	defer persister.pausingAof.Unlock()
	tmpFile := ctx.tmpFile

	// 处理并判断是否在重写期间有错误发生
	errOccurs := func() bool {
		// 重新打开当前 AOF 文件以便从重写开始时的位置继续拷贝数据
		src, err := os.Open(persister.aofFilename)
		if err != nil {
			logger.Error("open aofFilename failed: " + err.Error())
			return true
		}
		defer func() {
			_ = src.Close()
			_ = tmpFile.Close()
		}()

		_, err = src.Seek(ctx.fileSize, 0) // 定位到重写开始时的文件位置，即只拷贝重写后发生的写操作
		if err != nil {
			logger.Error("seek failed: " + err.Error())
			return true
		}
		// 将临时文件的数据库索引与在线 AOF 文件同步，确保数据一致性
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(ctx.dbIdx))).ToBytes()
		_, err = tmpFile.Write(data)
		if err != nil {
			logger.Error("tmp file rewrite failed: " + err.Error())
			return true
		}
		// 将重写期间新接收的命令拷贝到临时文件，这样临时文件包含所有最新的数据
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

	// 完成重写后，用临时文件替换旧的 AOF 文件，这个操作使用原子操作确保数据不丢失
	_ = persister.aofFile.Close()
	if err := os.Rename(tmpFile.Name(), persister.aofFilename); err != nil {
		logger.Warn(err)
	}
	// 重新打开新的 AOF 文件以便继续后续的写操作
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err) // 如果无法打开新的 AOF 文件，抛出异常
	}
	persister.aofFile = aofFile // 重新打开新的AOF文件，继续写入操作

	// 为了保证新的 AOF 文件的数据库索引与当前数据库索引一致，再次写入 SELECT 命令
	data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(persister.currentDB))).ToBytes()
	_, err = persister.aofFile.Write(data) // 保证新的AOF文件与当前数据库索引一致
	if err != nil {
		panic(err)
	}
}
