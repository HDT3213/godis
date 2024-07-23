package server

/*
 * A tcp.Handler implements redis protocol
 */

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/hdt3213/godis/cluster"
	"github.com/hdt3213/godis/config"
	database2 "github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/atomic"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

// Handler 实现 tcp.Handler 接口，作为 Redis 服务器使用
type Handler struct {
	activeConn sync.Map       // 存储活动的客户端连接
	db         database.DB    // 数据库接口
	closing    atomic.Boolean // 是否正在关闭服务的标志
}

// MakeHandler 创建一个 Handler 实例
func MakeHandler() *Handler {
	var db database.DB
	if config.Properties.ClusterEnable {
		db = cluster.MakeCluster() // 配置为集群模式
	} else {
		db = database2.NewStandaloneServer() // 单机模式
	}
	return &Handler{
		db: db,
	}
}

// closeClient 用于关闭客户端连接，并处理相关的清理工作
func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()            // 关闭连接
	h.db.AfterClientClose(client) // 处理客户端关闭后的数据库操作
	h.activeConn.Delete(client)   // 从活动连接中移除
}

// Handle 接收并执行 Redis 命令
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// 如果服务正在关闭，则拒绝新连接
		_ = conn.Close()
		return
	}

	client := connection.NewConn(conn) // 创建一个新的连接封装，此处的client指的是我们的一个tcp连接
	h.activeConn.Store(client, struct{}{})

	ch := parser.ParseStream(conn) // 解析连接流得到命令,ch是一个管道
	for payload := range ch {      //循环的从管道中拿数据。
		if payload.Err != nil {
			//先判断我们的redis的解析器解析到的错误类型是什么样子的
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// connection closed
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			// protocol err
			errReply := protocol.MakeErrReply(payload.Err.Error())
			_, err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			continue
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		result := h.db.Exec(client, r.Args)
		if result != nil {
			_, _ = client.Write(result.ToBytes())
		} else {
			_, _ = client.Write(unknownErrReplyBytes)
		}
	}
}

// Close stops handler
func (h *Handler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	// TODO: concurrent wait
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}
