package connection

import (
	"bytes"
	"io"
	"sync"
)

// FakeConn implements redis.Connection for test
type FakeConn struct {
	Connection
	buf    bytes.Buffer
	wait   chan struct{}
	closed bool
	mu     sync.Mutex
}

func NewFakeConn() *FakeConn {
	c := &FakeConn{}
	return c
}

// Write writes data to buffer
func (c *FakeConn) Write(b []byte) (int, error) {
	if c.closed {
		return 0, io.EOF
	}
	n, _ := c.buf.Write(b)
	c.notify()
	return n, nil
}

func (c *FakeConn) notify() {
	if c.wait != nil {
		c.mu.Lock()
		if c.wait != nil {
			close(c.wait)
			c.wait = nil
		}
		c.mu.Unlock()
	}
}

func (c *FakeConn) waiting() {
	c.mu.Lock()
	c.wait = make(chan struct{})
	c.mu.Unlock()
	<-c.wait
}

// Read reads data from buffer
func (c *FakeConn) Read(p []byte) (int, error) {
	n, err := c.buf.Read(p)
	if err == io.EOF {
		if c.closed {
			return 0, io.EOF
		}
		c.waiting()
		return c.buf.Read(p)
	}
	return n, err
}

// Clean resets the buffer
func (c *FakeConn) Clean() {
	c.wait = make(chan struct{})
	c.buf.Reset()
}

// Bytes returns written data
func (c *FakeConn) Bytes() []byte {
	return c.buf.Bytes()
}

func (c *FakeConn) Close() error {
	c.closed = true
	c.notify()
	return nil
}
