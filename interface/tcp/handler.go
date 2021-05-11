package tcp

import (
	"context"
	"net"
)

// HandleFunc represents application handler function
type HandleFunc func(ctx context.Context, conn net.Conn)

// Handler represents application server over tcp
type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}
