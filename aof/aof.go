package aof

import (
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	rdb "github.com/hdt3213/rdb/core"

	"github.com/hdt3213/godis/config"

	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
)

/*在aof过程中需要注意的事情
get 之类的读命令并不需要进行持久化
expire 命令要用等效的 expireat 命令替换。
举例说明，10:00 执行 expire a 3600 表示键 a 在 11:00 过期，
在 10:30 载入AOF文件时执行 expire a 3600 就成了 11:30 过期与原数据不符。
*/
// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// aofQueueSize 定义AOF队列大小
const (
	aofQueueSize = 1 << 20
)

// Fsync 策略常量
const (
	// 每个命令执行后都同步到磁盘
	FsyncAlways = "always"
	// 每秒同步一次到磁盘
	FsyncEverySec = "everysec"
	// 由操作系统决定何时同步到磁盘
	FsyncNo = "no"
)

// payload 结构体用于封装要写入AOF的命令数据
type payload struct {
	cmdLine CmdLine
	dbIndex int
	wg      *sync.WaitGroup // WaitGroup，用于同步等待所有命令的写入操作完成，主要用于测试和确保数据一致性
}

// Listener 接口用于回调监听AOF的变更
type Listener interface {
	// Callback 会在接收到AOF 的 payload后被调用
	Callback([]CmdLine)
}

// Aof的处理抽象结构体，AOF持久化处理器
type Persister struct {
	ctx         context.Context          // 上下文，用于管理和取消长时间运行的goroutine
	cancel      context.CancelFunc       // 取消函数，用于在关闭Persister时停止所有goroutine
	db          database.DBEngine        // 数据库引擎接口，必须持有以便操作redis的业务核心
	tmpDBMaker  func() database.DBEngine // 用于创建临时数据库实例的函数，通常在重写AOF时使用
	aofChan     chan *payload            // AOF载荷通道，用于接收来自其他组件的命令，以便异步写入AOF文件
	aofFile     *os.File                 // AOF文件的文件句柄，用于文件读写操作
	aofFilename string                   // AOF文件的路径，用于文件操作时指定正确的文件
	aofFsync    string                   // Fsync策略，控制数据何时从内存同步到磁盘（"always", "everysec", "no"）

	aofFinished chan struct{}         // 当AOF处理goroutine完成后，通过此通道通知主goroutine
	pausingAof  sync.Mutex            // 在AOF重写过程中用于暂停AOF记录的互斥锁
	currentDB   int                   // 当前数据库的索引，用于支持多数据库环境，保证命令在正确的数据库上执行
	listeners   map[Listener]struct{} // 监听器集合，用于实现发布-订阅模式，当AOF有更新时通知这些监听器
	buffer      []CmdLine             // 命令行缓冲区，用于减少内存分配，重用命令行数据
}

// NewPersister 创建新的AOF持久化处理器
func NewPersister(db database.DBEngine, filename string, load bool, fsync string, tmpDBMaker func() database.DBEngine) (*Persister, error) {
	persister := &Persister{} // 初始化Persister结构体实例
	persister.aofFilename = filename
	persister.aofFsync = strings.ToLower(fsync) // 设置文件同步策略，统一转为小写以防止大小写错误
	persister.db = db
	persister.tmpDBMaker = tmpDBMaker // 设置临时数据库创建函数，用于AOF重写等场景
	persister.currentDB = 0
	// load aof file if needed
	if load {
		persister.LoadAof(0) // 根据需要加载AOF文件
	}
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	persister.aofFile = aofFile
	persister.aofChan = make(chan *payload, aofQueueSize) // 创建一个负载通道，用于异步接收要写入的命令
	persister.aofFinished = make(chan struct{})           // 创建一个通道，用于通知AOF处理完成
	persister.listeners = make(map[Listener]struct{})
	// 启动一个协程来监听和处理AOF命令
	go func() {
		persister.listenCmd() // 启动一个协程监听和处理AOF命令
	}()
	// 设置context和cancel，用于管理协程的生命周期
	ctx, cancel := context.WithCancel(context.Background())
	persister.ctx = ctx
	persister.cancel = cancel
	// 如果策略是每秒同步，则启动定时同步
	if persister.aofFsync == FsyncEverySec { // 如果策略是每秒同步，则启动定时同步
		persister.fsyncEverySecond()
	}
	return persister, nil
}

// RemoveListener 移除一个监听器
func (persister *Persister) RemoveListener(listener Listener) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	delete(persister.listeners, listener)
}

// SaveCmdLine 将命令行数据发送到AOF处理协程
func (persister *Persister) SaveCmdLine(dbIndex int, cmdLine CmdLine) {
	//如果没有初始化管道，会出错
	if persister.aofChan == nil {
		return
	}
	//判断一下开启的aof的策略
	if persister.aofFsync == FsyncAlways {
		p := &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
		persister.writeAof(p) // 如果策略是立即同步，则直接写入文件
	}

	persister.aofChan <- &payload{
		cmdLine: cmdLine,
		dbIndex: dbIndex,
	}

}

// listenCmd 监听AOF队列，并将命令写入文件
func (persister *Persister) listenCmd() {
	for p := range persister.aofChan {
		persister.writeAof(p)
	}
	persister.aofFinished <- struct{}{}
}

// writeAof 处理写入AOF文件的具体逻辑
func (persister *Persister) writeAof(p *payload) {
	persister.buffer = persister.buffer[:0] // 清空缓冲区以重用
	persister.pausingAof.Lock()             // 加锁以同步对AOF操作的访问，防止重写冲突
	defer persister.pausingAof.Unlock()
	// 确认我当前操作是否需要切换db，如果跟上次相同则不需要再插入select db相关命令
	if p.dbIndex != persister.currentDB {
		// 如果当前数据库索引不正确，则发送SELECT命令切换数据库
		selectCmd := utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex)) //ToCmdLine功能是把输入的字符串序列，变成[][] string的切片
		persister.buffer = append(persister.buffer, selectCmd)
		data := protocol.MakeMultiBulkReply(selectCmd).ToBytes() //调用ToBytes之后会变成redis协议的格式
		_, err := persister.aofFile.Write(data)                  //写入文件
		if err != nil {
			logger.Warn(err)
			return // skip this command
		}
		persister.currentDB = p.dbIndex // 更新当前数据库索引
	}
	// 写入实际的命令
	data := protocol.MakeMultiBulkReply(p.cmdLine).ToBytes()
	persister.buffer = append(persister.buffer, p.cmdLine)
	_, err := persister.aofFile.Write(data)
	if err != nil {
		logger.Warn(err)
	}
	// 通知所有注册的监听器
	for listener := range persister.listeners {
		listener.Callback(persister.buffer)
	}
	// 如果同步策略为每个命令后立即同步，则执行同步
	if persister.aofFsync == FsyncAlways {
		_ = persister.aofFile.Sync()
	}
}

// LoadAof 从AOF文件加载数据，通常在服务器启动时调用
func (persister *Persister) LoadAof(maxBytes int) {
	// 在加载过程中暂时关闭aofChan，防止新的写入命令干扰加载过程
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

	// 从文件中解析可能的RDB预数据
	decoder := rdb.NewDecoder(file)
	err = persister.db.LoadRDB(decoder)
	if err != nil {
		// 如果没有RDB预数据，从文件开头开始加载
		file.Seek(0, io.SeekStart)
	} else {
		// 如果存在RDB预数据，从预数据后开始读取
		_, _ = file.Seek(int64(decoder.GetReadCount())+1, io.SeekStart)
		maxBytes = maxBytes - decoder.GetReadCount()
	}
	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}
	// 解析AOF数据流
	ch := parser.ParseStream(reader)
	fakeConn := connection.NewFakeConn() // 用于保存数据库索引的虚拟连接
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break // 文件读取完成
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

// Fsync 将AOF文件的内容同步到磁盘
func (persister *Persister) Fsync() {
	persister.pausingAof.Lock() // 加锁以防止在同步时发生并发写操作
	if err := persister.aofFile.Sync(); err != nil {
		logger.Errorf("fsync failed: %v", err) // 同步失败时记录错误
	}
	persister.pausingAof.Unlock()
}

// Close 优雅地停止AOF持久化过程
func (persister *Persister) Close() {
	if persister.aofFile != nil {
		close(persister.aofChan)         // 关闭AOF命令通道
		<-persister.aofFinished          // 等待后台goroutine完成AOF处理
		err := persister.aofFile.Close() // 关闭AOF文件
		if err != nil {
			logger.Warn(err)
		}
	}
	persister.cancel() // 取消相关的context，确保所有goroutine可以清理并退出
}

// fsyncEverySecond 每秒同步AOF文件到磁盘
func (persister *Persister) fsyncEverySecond() {
	ticker := time.NewTicker(time.Second) // 创建一个定时器，每秒触发一次
	go func() {
		for {
			select {
			case <-ticker.C: // 每秒触发
				persister.Fsync() // 调用Fsync方法同步数据到磁盘
			case <-persister.ctx.Done(): // 监听context的取消信号
				return // 如果接收到取消信号，退出goroutine
			}
		}
	}()
}

// generateAof 根据当前数据库状态生成新的AOF文件
func (persister *Persister) generateAof(ctx *RewriteCtx) error {
	tmpFile := ctx.tmpFile                  // 获取临时文件的文件句柄
	tmpAof := persister.newRewriteHandler() // 创建一个新的AOF重写处理器
	tmpAof.LoadAof(int(ctx.fileSize))       // 加载AOF数据到临时处理器
	for i := 0; i < config.Properties.Databases; i++ {
		// select db
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
		_, err := tmpFile.Write(data) // 写入选择数据库的命令到临时文件
		if err != nil {
			return err
		}
		// 遍历数据库中的每个键值对，并写入临时文件
		tmpAof.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			cmd := EntityToCmd(key, entity)
			if cmd != nil {
				_, _ = tmpFile.Write(cmd.ToBytes())
			}
			if expiration != nil {
				cmd := MakeExpireCmd(key, *expiration) // 如果有过期时间，生成过期命令
				if cmd != nil {
					_, _ = tmpFile.Write(cmd.ToBytes()) // 写入过期命令到临时文件
				}
			}
			return true // 继续遍历
		})
	}
	return nil
}
