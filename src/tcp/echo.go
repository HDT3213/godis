package tcp

/**
 * A echo server to test whether the server is functioning normally
 */

import (
    "net"
    "context"
    "bufio"
    "github.com/HDT3213/godis/src/lib/logger"
    "sync"
    "io"
    "github.com/HDT3213/godis/src/lib/sync/atomic"
    "time"
    "github.com/HDT3213/godis/src/lib/sync/wait"
)

type EchoHandler struct {
    activeConn sync.Map
    closing atomic.AtomicBool
}

func MakeEchoHandler()(*EchoHandler) {
    return &EchoHandler{
    }
}

type Client struct {
    Conn net.Conn
    Waiting wait.Wait
}

func (c *Client)Close()error {
    c.Waiting.WaitWithTimeout(10 * time.Second)
    c.Conn.Close()
    return nil
}

func (h *EchoHandler)Handle(ctx context.Context, conn net.Conn) {
    if h.closing.Get() {
        // closing handler refuse new connection
        conn.Close()
    }

    client := &Client {
        Conn: conn,
    }
    h.activeConn.Store(client, 1)

    reader := bufio.NewReader(conn)
    for {
        // may occurs: client EOF, client timeout, server early close
        msg, err := reader.ReadString('\n')
        if err != nil {
            if err == io.EOF {
                logger.Info("connection close")
                h.activeConn.Delete(conn)
            } else {
                logger.Warn(err)
            }
            return
        }
        client.Waiting.Add(1)
        //logger.Info("sleeping")
        //time.Sleep(10 * time.Second)
        b := []byte(msg)
        conn.Write(b)
        client.Waiting.Done()
    }
}

func (h *EchoHandler)Close()error {
    logger.Info("handler shuting down...")
    h.closing.Set(true)
    // TODO: concurrent wait
    h.activeConn.Range(func(key interface{}, val interface{})bool {
        client := key.(*Client)
        client.Close()
        return true
    })
    return nil
}