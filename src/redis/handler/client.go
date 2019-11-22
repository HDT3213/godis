package handler

import (
    "github.com/HDT3213/godis/src/lib/sync/atomic"
    "github.com/HDT3213/godis/src/lib/sync/wait"
    "net"
    "time"
)

// abstract of active server
type Client struct {
    conn   net.Conn

    // waiting util reply finished
    waitingReply wait.Wait

    // is sending request in progress
    sending atomic.AtomicBool
    // multi bulk msg lineCount - 1(first line)
    expectedArgsCount uint32
    // sent line count, exclude first line
    receivedCount uint32
    // sent lines, exclude first line
    args [][]byte

}

func (c *Client)Close()error {
    c.waitingReply.WaitWithTimeout(10 * time.Second)
    _ = c.conn.Close()
    return nil
}

func MakeClient(conn net.Conn) *Client {
    return &Client{
        conn: conn,
    }
}
