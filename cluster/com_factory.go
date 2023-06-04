package cluster

import (
	"errors"
	"fmt"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/pool"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/client"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
	"net"
)

type defaultClientFactory struct {
	nodeConnections dict.Dict // map[string]*pool.Pool
}

var connectionPoolConfig = pool.Config{
	MaxIdle:   1,
	MaxActive: 16,
}

// GetPeerClient gets a client with peer form pool
func (factory *defaultClientFactory) GetPeerClient(peerAddr string) (peerClient, error) {
	var connectionPool *pool.Pool
	raw, ok := factory.nodeConnections.Get(peerAddr)
	if !ok {
		creator := func() (interface{}, error) {
			c, err := client.MakeClient(peerAddr)
			if err != nil {
				return nil, err
			}
			c.Start()
			// all peers of cluster should use the same password
			if config.Properties.RequirePass != "" {
				authResp := c.Send(utils.ToCmdLine("AUTH", config.Properties.RequirePass))
				if !protocol.IsOKReply(authResp) {
					return nil, fmt.Errorf("auth failed, resp: %s", string(authResp.ToBytes()))
				}
			}
			return c, nil
		}
		finalizer := func(x interface{}) {
			logger.Debug("destroy client")
			cli, ok := x.(client.Client)
			if !ok {
				return
			}
			cli.Close()
		}
		connectionPool = pool.New(creator, finalizer, connectionPoolConfig)
		factory.nodeConnections.Put(peerAddr, connectionPool)
	} else {
		connectionPool = raw.(*pool.Pool)
	}
	raw, err := connectionPool.Get()
	if err != nil {
		return nil, err
	}
	conn, ok := raw.(*client.Client)
	if !ok {
		return nil, errors.New("connection pool make wrong type")
	}
	return conn, nil
}

// ReturnPeerClient returns client to pool
func (factory *defaultClientFactory) ReturnPeerClient(peer string, peerClient peerClient) error {
	raw, ok := factory.nodeConnections.Get(peer)
	if !ok {
		return errors.New("connection pool not found")
	}
	raw.(*pool.Pool).Put(peerClient)
	return nil
}

type tcpStream struct {
	conn net.Conn
	ch   <-chan *parser.Payload
}

func (s *tcpStream) Stream() <-chan *parser.Payload {
	return s.ch
}

func (s *tcpStream) Close() error {
	return s.conn.Close()
}

func (factory *defaultClientFactory) NewStream(peerAddr string, cmdLine CmdLine) (peerStream, error) {
	// todo: reuse connection
	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		return nil, fmt.Errorf("connect with %s failed: %v", peerAddr, err)
	}
	ch := parser.ParseStream(conn)
	send2node := func(cmdLine CmdLine) redis.Reply {
		req := protocol.MakeMultiBulkReply(cmdLine)
		_, err := conn.Write(req.ToBytes())
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		resp := <-ch
		if resp.Err != nil {
			return protocol.MakeErrReply(resp.Err.Error())
		}
		return resp.Data
	}
	if config.Properties.RequirePass != "" {
		authResp := send2node(utils.ToCmdLine("AUTH", config.Properties.RequirePass))
		if !protocol.IsOKReply(authResp) {
			return nil, fmt.Errorf("auth failed, resp: %s", string(authResp.ToBytes()))
		}
	}
	req := protocol.MakeMultiBulkReply(cmdLine)
	_, err = conn.Write(req.ToBytes())
	if err != nil {
		return nil, protocol.MakeErrReply("send cmdLine failed: " + err.Error())
	}
	return &tcpStream{
		conn: conn,
		ch:   ch,
	}, nil
}

func newDefaultClientFactory() *defaultClientFactory {
	return &defaultClientFactory{
		nodeConnections: dict.MakeConcurrent(1),
	}
}

func (factory *defaultClientFactory) Close() error {
	factory.nodeConnections.ForEach(func(key string, val interface{}) bool {
		val.(*pool.Pool).Close()
		return true
	})
	return nil
}
