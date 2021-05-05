package connection

import (
	"bytes"
	"github.com/hdt3213/godis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

// Connection represents a connection with a redis-cli
type Connection struct {
	conn net.Conn

	// waiting util reply finished
	waitingReply wait.Wait

	// lock while server sending response
	mu sync.Mutex

	// subscribing channels
	subs map[string]bool
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
	return &Connection{
		conn: conn,
	}
}

// Write sends response to client over tcp connection
func (c *Connection) Write(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

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

	if c.subs == nil {
		return
	}
	delete(c.subs, channel)
}

// SubsCount returns the number of subscribing channels
func (c *Connection) SubsCount() int {
	if c.subs == nil {
		return 0
	}
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

type FakeConn struct {
	Connection
	buf bytes.Buffer
}

func (c *FakeConn) Write(b []byte) error {
	c.buf.Write(b)
	return nil
}

func (c *FakeConn) Clean() {
	c.buf.Reset()
}

func (c *FakeConn) Bytes() []byte {
	return c.buf.Bytes()
}