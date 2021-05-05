package client

import (
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/wait"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/reply"
	"net"
	"runtime/debug"
	"sync"
	"time"
)

type Client struct {
	conn        net.Conn
	pendingReqs chan *Request // wait to send
	waitingReqs chan *Request // waiting response
	ticker      *time.Ticker
	addr        string

	working *sync.WaitGroup // its counter presents unfinished requests(pending and waiting)
}

type Request struct {
	id        uint64
	args      [][]byte
	reply     redis.Reply
	heartbeat bool
	waiting   *wait.Wait
	err       error
}

const (
	chanSize = 256
	maxWait  = 3 * time.Second
)

func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		addr:        addr,
		conn:        conn,
		pendingReqs: make(chan *Request, chanSize),
		waitingReqs: make(chan *Request, chanSize),
		working:     &sync.WaitGroup{},
	}, nil
}

func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second)
	go client.handleWrite()
	go func() {
		err := client.handleRead()
		if err != nil {
			logger.Error(err)
		}
	}()
	go client.heartbeat()
}

func (client *Client) Close() {
	client.ticker.Stop()
	// stop new request
	close(client.pendingReqs)

	// wait stop process
	client.working.Wait()

	// clean
	_ = client.conn.Close()
	close(client.waitingReqs)
}

func (client *Client) handleConnectionError(err error) error {
	err1 := client.conn.Close()
	if err1 != nil {
		if opErr, ok := err1.(*net.OpError); ok {
			if opErr.Err.Error() != "use of closed network connection" {
				return err1
			}
		} else {
			return err1
		}
	}
	conn, err1 := net.Dial("tcp", client.addr)
	if err1 != nil {
		logger.Error(err1)
		return err1
	}
	client.conn = conn
	go func() {
		_ = client.handleRead()
	}()
	return nil
}

func (client *Client) heartbeat() {
	for range client.ticker.C {
		client.doHeartbeat()
	}
}

func (client *Client) handleWrite() {
	for req := range client.pendingReqs {
		client.doRequest(req)
	}
}

func (client *Client) Send(args [][]byte) redis.Reply {
	request := &Request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingReqs <- request
	timeout := request.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return reply.MakeErrReply("server time out")
	}
	if request.err != nil {
		return reply.MakeErrReply("request failed")
	}
	return request.reply
}

func (client *Client) doHeartbeat() {
	request := &Request{
		args:      [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingReqs <- request
	request.waiting.WaitWithTimeout(maxWait)
}

func (client *Client) doRequest(req *Request) {
	if req == nil || len(req.args) == 0 {
		return
	}
	re := reply.MakeMultiBulkReply(req.args)
	bytes := re.ToBytes()
	_, err := client.conn.Write(bytes)
	i := 0
	for err != nil && i < 3 {
		err = client.handleConnectionError(err)
		if err == nil {
			_, err = client.conn.Write(bytes)
		}
		i++
	}
	if err == nil {
		client.waitingReqs <- req
	} else {
		req.err = err
		req.waiting.Done()
	}
}

func (client *Client) finishRequest(reply redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()
	request := <-client.waitingReqs
	if request == nil {
		return
	}
	request.reply = reply
	if request.waiting != nil {
		request.waiting.Done()
	}
}

func (client *Client) handleRead() error {
	ch := parser.ParseStream(client.conn)
	for payload := range ch {
		if payload.Err != nil {
			client.finishRequest(reply.MakeErrReply(payload.Err.Error()))
			continue
		}
		client.finishRequest(payload.Data)
	}
	return nil
}
