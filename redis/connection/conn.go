package connection

import (
	"net"
	"sync"
	"time"

	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/wait"
)

const (
	// flagSlave 表示这是一个从服务器的连接
	flagSlave = uint64(1 << iota)
	// flagMaster 表示这是一个主服务器的连接
	flagMaster
	// flagMulti 表示这个连接正在执行事务
	flagMulti
)

// Connection 代表与一个Redis客户端的连接
type Connection struct {
	conn net.Conn // 网络连接实例

	// sendingData 用于优雅关闭时等待数据发送完成
	sendingData wait.Wait

	// mu 用于在发送响应时的互斥锁
	mu    sync.Mutex
	flags uint64 // 连接的标志位，如是否为主从连接、是否处于事务中等

	/// subs 保存订阅的频道
	subs map[string]bool

	// password 可能会在运行时通过CONFIG命令被修改，因此需要存储密码
	password string

	// queue 保存在事务中排队的命令
	queue [][][]byte
	// watching 保存被WATCH命令监视的键及其版本号
	watching map[string]uint32
	// txErrors 保存事务中的错误信息
	txErrors []error

	// selectedDB 表示当前选择的数据库索引
	selectedDB int
}

// 连接池，用于重用连接对象
var connPool = sync.Pool{
	New: func() interface{} {
		return &Connection{}
	},
}

// RemoteAddr 返回远程连接的网络地址
func (c *Connection) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

// Close 用于断开与客户端的连接
func (c *Connection) Close() error {
	c.sendingData.WaitWithTimeout(10 * time.Second) // 等待正在发送的数据完成或超时
	_ = c.conn.Close()                              // 关闭底层网络连接
	// 清理连接相关的状态信息
	c.subs = nil
	c.password = ""
	c.queue = nil
	c.watching = nil
	c.txErrors = nil
	c.selectedDB = 0
	connPool.Put(c) // 将连接对象放回池中
	return nil
}

// NewConn 用于创建新的Connection实例
func NewConn(conn net.Conn) *Connection {
	c, ok := connPool.Get().(*Connection)
	if !ok {
		logger.Error("connection pool make wrong type")
		return &Connection{
			conn: conn,
		}
	}
	c.conn = conn
	return c
}

// Write 向客户端发送响应
func (c *Connection) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	c.sendingData.Add(1)
	defer func() {
		c.sendingData.Done()
	}()

	return c.conn.Write(b)
}

func (c *Connection) Name() string {
	if c.conn != nil {
		return c.conn.RemoteAddr().String()
	}
	return ""
}

// Subscribe 将当前连接添加到指定频道的订阅者中
func (c *Connection) Subscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subs == nil {
		c.subs = make(map[string]bool)
	}
	c.subs[channel] = true
}

// UnSubscribe 从指定频道的订阅者中移除当前连接
func (c *Connection) UnSubscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.subs) == 0 {
		return
	}
	delete(c.subs, channel)
}

// SubsCount 返回当前连接订阅的频道数量
func (c *Connection) SubsCount() int {
	return len(c.subs)
}

// GetChannels 返回当前连接订阅的所有频道
func (c *Connection) GetChannels() []string {
	if c.subs == nil {
		return make([]string, 0)
	}
	channels := make([]string, len(c.subs))
	i := 0
	for channel := range c.subs {
		channels[i] = channel
		i++
	}
	return channels
}

// SetPassword 设置连接的密码，用于认证
func (c *Connection) SetPassword(password string) {
	c.password = password
}

// GetPassword 获取连接的密码
func (c *Connection) GetPassword() string {
	return c.password
}

// InMultiState 检查连接是否处于事务状态
func (c *Connection) InMultiState() bool {
	return c.flags&flagMulti > 0
}

// SetMultiState 设置连接的事务状态
func (c *Connection) SetMultiState(state bool) {
	if !state { // 如果取消事务，重置相关数据
		c.watching = nil
		c.queue = nil
		c.flags &= ^flagMulti // 清除事务标志
		return
	}
	c.flags |= flagMulti // 设置事务标志
}

// GetQueuedCmdLine 返回事务中排队的命令
func (c *Connection) GetQueuedCmdLine() [][][]byte {
	return c.queue
}

// EnqueueCmd 将命令添加到事务队列
func (c *Connection) EnqueueCmd(cmdLine [][]byte) {
	c.queue = append(c.queue, cmdLine)
}

// AddTxError 添加事务执行中的错误
func (c *Connection) AddTxError(err error) {
	c.txErrors = append(c.txErrors, err)
}

// GetTxErrors 获取事务中的错误
func (c *Connection) GetTxErrors() []error {
	return c.txErrors
}

// ClearQueuedCmds 清除事务中排队的命令
func (c *Connection) ClearQueuedCmds() {
	c.queue = nil
}

// GetWatching 返回被监视的键和它们的版本号
func (c *Connection) GetWatching() map[string]uint32 {
	if c.watching == nil {
		c.watching = make(map[string]uint32)
	}
	return c.watching
}

// GetDBIndex 返回选定的数据库索引
func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

// SelectDB 选择一个数据库
func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}

// SetSlave 设置连接为从服务器模式
func (c *Connection) SetSlave() {
	c.flags |= flagSlave
}

// IsSlave 检查连接是否为从服务器模式
func (c *Connection) IsSlave() bool {
	return c.flags&flagSlave > 0
}

// SetMaster 设置连接为主服务器模式

func (c *Connection) SetMaster() {
	c.flags |= flagMaster
}

// IsMaster 检查连接是否为主服务器模式
func (c *Connection) IsMaster() bool {
	return c.flags&flagMaster > 0
}
