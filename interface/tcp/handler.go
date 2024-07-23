package tcp

import (
	"context"
	"net"
)

// HandleFunc 定义了一个应用程序处理函数的类型
// 这种类型的函数接收一个context和一个网络连接
// ctx context.Context：上下文，用于控制子程序的生命周期
// conn net.Conn：表示一个网络连接，用于读取和写入数据
type HandleFunc func(ctx context.Context, conn net.Conn)

// Handler 接口代表一个基于TCP的应用服务器
// 它定义了处理连接和关闭服务器的方法
type Handler interface {
	Handle(ctx context.Context, conn net.Conn) // 处理接收到的网络连接
	Close() error                              // 关闭服务器，清理资源，如果有错误返回错误
}
