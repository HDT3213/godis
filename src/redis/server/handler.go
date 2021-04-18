package server

/*
 * A tcp.Handler implements redis protocol
 */

import (
	"context"
	"github.com/hdt3213/godis/src/cluster"
	"github.com/hdt3213/godis/src/config"
	DBImpl "github.com/hdt3213/godis/src/db"
	"github.com/hdt3213/godis/src/interface/db"
	"github.com/hdt3213/godis/src/lib/logger"
	"github.com/hdt3213/godis/src/lib/sync/atomic"
	"github.com/hdt3213/godis/src/redis/parser"
	"github.com/hdt3213/godis/src/redis/reply"
	"io"
	"net"
	"strings"
	"sync"
)

var (
	UnknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

type Handler struct {
	activeConn sync.Map // *client -> placeholder
	db         db.DB
	closing    atomic.AtomicBool // refusing new client and new request
}

func MakeHandler() *Handler {
	var db db.DB
	if config.Properties.Self != "" &&
		len(config.Properties.Peers) > 0 {
		db = cluster.MakeCluster()
	} else {
		db = DBImpl.MakeDB()
	}
	return &Handler{
		db: db,
	}
}

func (h *Handler) closeClient(client *Client) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// closing handler refuse new connection
		_ = conn.Close()
	}

	client := MakeClient(conn)
	h.activeConn.Store(client, 1)

	ch := parser.Parse(conn)
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// connection closed
				h.closeClient(client)
				logger.Info("connection closed: " + client.conn.RemoteAddr().String())
				return
			} else {
				// protocol err
				errReply := reply.MakeErrReply(payload.Err.Error())
				err := client.Write(errReply.ToBytes())
				if err != nil {
					h.closeClient(client)
					logger.Info("connection closed: " + client.conn.RemoteAddr().String())
					return
				}
				continue
			}
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		result := h.db.Exec(client, r.Args)
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			_ = client.Write(UnknownErrReplyBytes)
		}
	}
}

func (h *Handler) Close() error {
	logger.Info("handler shuting down...")
	h.closing.Set(true)
	// TODO: concurrent wait
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*Client)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}
