package gnet

import (
	"sync/atomic"

	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/panjf2000/gnet/v2"
)

type GnetServer struct {
	gnet.BuiltinEventEngine
	eng       gnet.Engine
	connected int32
	db        database.DB
}

func NewGnetServer(db database.DB) *GnetServer {
	return &GnetServer{
		db: db,
	}
}

func (s *GnetServer) OnBoot(eng gnet.Engine) (action gnet.Action) {
	s.eng = eng
	return
}

func (s *GnetServer) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	client := connection.NewConn(c)
	c.SetContext(client)
	atomic.AddInt32(&s.connected, 1)
	return
}

func (s *GnetServer) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	if err != nil {
		logger.Infof("error occurred on connection=%s, %v\n", c.RemoteAddr().String(), err)
	}
	atomic.AddInt32(&s.connected, -1)
	conn := c.Context().(redis.Connection)
	s.db.AfterClientClose(conn)
	return
}

func (s *GnetServer) OnTraffic(c gnet.Conn) (action gnet.Action) {
	conn := c.Context().(redis.Connection)
	cmdLine, err := Parse(c)
	if err != nil {
		logger.Infof("parse command line failed: %v", err)
		return gnet.Close
	}
	if len(cmdLine) == 0 {
		return gnet.None
	}
	result := s.db.Exec(conn, cmdLine)
	buffer := result.ToBytes()
	if len(buffer) > 0 {
		c.Write(buffer)
	}
	return gnet.None
}
