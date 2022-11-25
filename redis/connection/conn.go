package connection

import (
	"bytes"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

const (
	// NormalCli is client with user
	NormalCli = iota
	// ReplicationRecvCli is fake client with replication master
	ReplicationRecvCli
)

// Connection represents a connection with a redis-cli
type Connection struct {
	conn net.Conn

	// waiting until protocol finished
	waitingReply wait.Wait

	// lock while server sending response
	mu sync.Mutex

	// subscribing channels
	subs map[string]bool

	// password may be changed by CONFIG command during runtime, so store the password
	password string

	// queued commands for `multi`
	multiState bool
	queue      [][][]byte
	watching   map[string]uint32
	txErrors   []error

	// selected db
	selectedDB int
	role       int32
}

var connPool = sync.Pool{
	New: func() interface{} {
		return &Connection{}
	},
}

// GetConnPool returns the connection pool pointer for putting and getting connection
func (c *Connection) GetConnPool() *sync.Pool {
	return &connPool
}

// RemoteAddr returns the remote network address
func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// Close disconnect with the client
func (c *Connection) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

// NewConn creates Connection instance
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

// Write sends response to client over tcp connection
func (c *Connection) Write(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	c.waitingReply.Add(1)
	defer func() {
		c.waitingReply.Done()
	}()

	_, err := c.conn.Write(b)
	return err
}

// Subscribe add current connection into subscribers of the given channel
func (c *Connection) Subscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subs == nil {
		c.subs = make(map[string]bool)
	}
	c.subs[channel] = true
}

// UnSubscribe removes current connection into subscribers of the given channel
func (c *Connection) UnSubscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.subs) == 0 {
		return
	}
	delete(c.subs, channel)
}

// SubsCount returns the number of subscribing channels
func (c *Connection) SubsCount() int {
	return len(c.subs)
}

// GetChannels returns all subscribing channels
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

// SetPassword stores password for authentication
func (c *Connection) SetPassword(password string) {
	c.password = password
}

// GetPassword get password for authentication
func (c *Connection) GetPassword() string {
	return c.password
}

// InMultiState tells is connection in an uncommitted transaction
func (c *Connection) InMultiState() bool {
	return c.multiState
}

// SetMultiState sets transaction flag
func (c *Connection) SetMultiState(state bool) {
	if !state { // reset data when cancel multi
		c.watching = nil
		c.queue = nil
	}
	c.multiState = state
}

// GetQueuedCmdLine returns queued commands of current transaction
func (c *Connection) GetQueuedCmdLine() [][][]byte {
	return c.queue
}

// EnqueueCmd  enqueues command of current transaction
func (c *Connection) EnqueueCmd(cmdLine [][]byte) {
	c.queue = append(c.queue, cmdLine)
}

// AddTxError stores syntax error within transaction
func (c *Connection) AddTxError(err error) {
	c.txErrors = append(c.txErrors, err)
}

// GetTxErrors returns syntax error within transaction
func (c *Connection) GetTxErrors() []error {
	return c.txErrors
}

// ClearQueuedCmds clears queued commands of current transaction
func (c *Connection) ClearQueuedCmds() {
	c.queue = nil
}

// GetRole returns role of connection, such as connection with master
func (c *Connection) GetRole() int32 {
	if c == nil {
		return NormalCli
	}
	return c.role
}

func (c *Connection) SetRole(r int32) {
	c.role = r
}

// GetWatching returns watching keys and their version code when started watching
func (c *Connection) GetWatching() map[string]uint32 {
	if c.watching == nil {
		c.watching = make(map[string]uint32)
	}
	return c.watching
}

// GetDBIndex returns selected db
func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

// SelectDB selects a database
func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}

// FakeConn implements redis.Connection for test
type FakeConn struct {
	Connection
	buf bytes.Buffer
}

// Write writes data to buffer
func (c *FakeConn) Write(b []byte) error {
	c.buf.Write(b)
	return nil
}

// Clean resets the buffer
func (c *FakeConn) Clean() {
	c.buf.Reset()
}

// Bytes returns written data
func (c *FakeConn) Bytes() []byte {
	return c.buf.Bytes()
}
