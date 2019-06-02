package tcp

import (
    "net"
    "context"
)

type HandleFunc func(ctx context.Context, conn net.Conn)

type Handler interface {
    Handle(ctx context.Context, conn net.Conn)
    Close()error
}