package cluster

import (
    "bufio"
    "context"
    "errors"
    "github.com/HDT3213/godis/src/cluster/idgenerator"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/lib/logger"
    "github.com/HDT3213/godis/src/lib/sync/wait"
    "github.com/HDT3213/godis/src/redis/reply"
    "io"
    "net"
    "strconv"
    "sync"
    "time"
)

const (
    timeout = 2 * time.Second
    CRLF    = "\r\n"
)

type InternalClient struct {
    idGen       *idgenerator.IdGenerator
    conn        net.Conn
    sendingReqs chan *AsyncRequest
    ticker      *time.Ticker
    addr        string
    waitingMap  *sync.Map // key -> request

    ctx        context.Context
    cancelFunc context.CancelFunc
    writing    *sync.WaitGroup
}

type AsyncRequest struct {
    id        int64
    args      [][]byte
    reply     redis.Reply
    heartbeat bool
    waiting   *wait.Wait
}

type AsyncMultiBulkReply struct {
    Args [][]byte
}

func MakeAsyncMultiBulkReply(args [][]byte) *AsyncMultiBulkReply {
    return &AsyncMultiBulkReply{
        Args: args,
    }
}

func (r *AsyncMultiBulkReply) ToBytes() []byte {
    argLen := len(r.Args)
    res := "@" + strconv.Itoa(argLen) + CRLF
    for _, arg := range r.Args {
        if arg == nil {
            res += "$-1" + CRLF
        } else {
            res += "$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF
        }
    }
    return []byte(res)
}

func MakeInternalClient(addr string, idGen *idgenerator.IdGenerator) (*InternalClient, error) {
    conn, err := net.Dial("tcp", addr)
    if err != nil {
        return nil, err
    }
    ctx, cancel := context.WithCancel(context.Background())
    return &InternalClient{
        addr:        addr,
        conn:        conn,
        sendingReqs: make(chan *AsyncRequest, 256),
        waitingMap:  &sync.Map{},

        ctx:        ctx,
        cancelFunc: cancel,
        writing:    &sync.WaitGroup{},
        idGen:      idGen,
    }, nil
}

func (client *InternalClient) Start() {
    client.ticker = time.NewTicker(10 * time.Second)
    go client.handleWrite()
    go func() {
        err := client.handleRead()
        logger.Warn(err)
    }()
    go client.heartbeat()
}

func (client *InternalClient) Close() {
    // send stop signal
    client.cancelFunc()

    // wait stop process
    client.writing.Wait()

    // clean
    _ = client.conn.Close()
    close(client.sendingReqs)
}

func (client *InternalClient) handleConnectionError(err error) error {
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

func (client *InternalClient) heartbeat() {
loop:
    for {
        select {
        case <-client.ticker.C:
            client.sendingReqs <- &AsyncRequest{
                args:      [][]byte{[]byte("PING")},
                heartbeat: true,
            }
        case <-client.ctx.Done():
            break loop
        }
    }
}

func (client *InternalClient) handleWrite() {
    client.writing.Add(1)
loop:
    for {
        select {
        case req := <-client.sendingReqs:
            client.doRequest(req)
        case <-client.ctx.Done():
            break loop
        }
    }
    client.writing.Done()
}

func (client *InternalClient) Send(args [][]byte) redis.Reply {
    request := &AsyncRequest{
        id:        client.idGen.NextId(),
        args:      args,
        heartbeat: false,
        waiting:   &wait.Wait{},
    }
    request.waiting.Add(1)
    client.sendingReqs <- request
    client.waitingMap.Store(request.id, request)
    timeUp := request.waiting.WaitWithTimeout(timeout)
    if timeUp {
        client.waitingMap.Delete(request.id)
        return nil
    } else {
        return request.reply
    }
}

func (client *InternalClient) doRequest(req *AsyncRequest) {
    bytes := reply.MakeMultiBulkReply(req.args).ToBytes()
    _, err := client.conn.Write(bytes)
    i := 0
    for err != nil && i < 3 {
        err = client.handleConnectionError(err)
        if err == nil {
            _, err = client.conn.Write(bytes)
        }
        i++
    }
}

func (client *InternalClient) finishRequest(reply *AsyncMultiBulkReply) {
    if reply == nil || reply.Args == nil || len(reply.Args) == 0 {
        return
    }
    reqId, err := strconv.ParseInt(string(reply.Args[0]), 10, 64)
    if err != nil {
        logger.Warn(err)
        return
    }
    raw, ok := client.waitingMap.Load(reqId)
    if !ok {
        return
    }
    request := raw.(*AsyncRequest)
    request.reply = reply
    if request.waiting != nil {
        request.waiting.Done()
    }
}

func (client *InternalClient) handleRead() error {
    reader := bufio.NewReader(client.conn)
    downloading := false
    expectedArgsCount := 0
    receivedCount := 0
    var args [][]byte
    var fixedLen int64 = 0
    var err error
    var msg []byte
    for {
        // read line
        if fixedLen == 0 { // read normal line
            msg, err = reader.ReadBytes('\n')
            if err != nil {
                if err == io.EOF || err == io.ErrUnexpectedEOF {
                    logger.Info("connection close")
                } else {
                    logger.Warn(err)
                }

                return errors.New("connection closed")
            }
            if len(msg) == 0 || msg[len(msg)-2] != '\r' {
                return errors.New("protocol error")
            }
        } else { // read bulk line (binary safe)
            msg = make([]byte, fixedLen+2)
            _, err = io.ReadFull(reader, msg)
            if err != nil {
                if err == io.EOF || err == io.ErrUnexpectedEOF {
                    return errors.New("connection closed")
                } else {
                    return err
                }
            }
            if len(msg) == 0 ||
                msg[len(msg)-2] != '\r' ||
                msg[len(msg)-1] != '\n' {
                return errors.New("protocol error")
            }
            fixedLen = 0
        }

        // parse line
        if !downloading {
            // receive new response
            if msg[0] == '@' { // customized multi bulk response
                // bulk multi msg
                expectedLine, err := strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
                if err != nil {
                    return errors.New("protocol error: " + err.Error())
                }
                if expectedLine == 0 {
                    client.finishRequest(nil)
                } else if expectedLine > 0 {
                    downloading = true
                    expectedArgsCount = int(expectedLine)
                    receivedCount = 0
                    args = make([][]byte, expectedLine)
                } else {
                    return errors.New("protocol error")
                }
            }
        } else {
            // receive following part of a request
            line := msg[0 : len(msg)-2]
            if line[0] == '$' {
                fixedLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
                if err != nil {
                    return err
                }
                if fixedLen <= 0 { // null bulk in multi bulks
                    args[receivedCount] = []byte{}
                    receivedCount++
                    fixedLen = 0
                }
            } else {
                args[receivedCount] = line
                receivedCount++
            }

            // if sending finished
            if receivedCount == expectedArgsCount {
                downloading = false // finish downloading progress

                client.finishRequest(&AsyncMultiBulkReply{Args: args})

                // finish reply
                expectedArgsCount = 0
                receivedCount = 0
                args = nil
            }
        }
    }
}
