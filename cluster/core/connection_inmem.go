package core

import (
	"sync"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
)

type InMemConnectionFactory struct {
	nodes map[string]*Cluster
	mu    sync.Mutex
}

func NewInMemConnectionFactory() *InMemConnectionFactory {
	return &InMemConnectionFactory{
		nodes: make(map[string]*Cluster),
	}
}

type InMemClient struct {
	addr    string
	cluster *Cluster
}

// RemoteAddress implements peerClient.
func (c *InMemClient) RemoteAddress() string {
	return c.addr
}

// Send implements peerClient.
func (c *InMemClient) Send(args [][]byte) redis.Reply {
	fakeConn := connection.NewFakeConn()
	return c.cluster.Exec(fakeConn, args)
}

type InMemStream struct {
	conn    *connection.FakeConn
	cluster *Cluster
}

func (c *InMemStream) Stream() <-chan *parser.Payload {
	return parser.ParseStream(c.conn)
}

func (c *InMemStream) Close() error {
	return nil
}

func (factory *InMemConnectionFactory) NewPeerClient(peerAddr string) (peerClient, error) {
	factory.mu.Lock()
	cluster := factory.nodes[peerAddr]
	factory.mu.Unlock()
	return &InMemClient{
		addr:    peerAddr,
		cluster: cluster,
	}, nil
}

func (factory *InMemConnectionFactory) BorrowPeerClient(peerAddr string) (peerClient, error) {
	return factory.NewPeerClient(peerAddr)
}

func (factory *InMemConnectionFactory) ReturnPeerClient(peerClient peerClient) error {
	return nil
}

func (factory *InMemConnectionFactory) NewStream(peerAddr string, cmdLine CmdLine) (peerStream, error) {
	factory.mu.Lock()
	cluster := factory.nodes[peerAddr]
	factory.mu.Unlock()
	conn := connection.NewFakeConn()
	reply := cluster.Exec(conn, cmdLine)
	conn.Write(reply.ToBytes()) // append response at the end
	return &InMemStream{
		conn:    conn,
		cluster: cluster,
	}, nil
}

func (factory *InMemConnectionFactory) Close() error {
	return nil
}
