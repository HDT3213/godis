package tcp

/**
 * A tcp server
 */

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hdt3213/godis/interface/tcp"
	"github.com/hdt3213/godis/lib/logger"
)

// Config 用于存储TCP服务器的配置属性
type Config struct {
	Address    string        `yaml:"address"`     // 监听的服务器地址
	MaxConnect uint32        `yaml:"max-connect"` // 允许的最大连接数
	Timeout    time.Duration `yaml:"timeout"`     // 连接的超时时间
}

// ClientCounter 用于记录当前Godis服务器中的客户端数量，是一个原子计数器
var ClientCounter int32

// ListenAndServeWithSignal 在接收到信号时终止服务
// cfg：服务器配置
// handler：处理TCP连接的处理器接口
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{}) // 用于通知服务器关闭
	sigCh := make(chan os.Signal)    // 监听系统信号
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{} // 接收到停止信号时，发送关闭通知
		}
	}()
	listener, err := net.Listen("tcp", cfg.Address) // 开始在指定地址监听TCP连接
	if err != nil {
		return err
	}
	//cfg.Address = listener.Addr().String()
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address)) // 记录日志，开始监听
	ListenAndServe(listener, handler, closeChan)                          // 调用函数处理监听和连接请求
	return nil
}

// ListenAndServe 维持服务运行，直到被告知关闭
// listener：监听器
// handler：处理TCP连接的处理器接口
// closeChan：接收关闭服务器的通知
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// listen signal
	errCh := make(chan error, 1)
	defer close(errCh)
	//TCP 服务器的优雅关闭模式通常为: 先关闭listener阻止新连接进入，然后遍历所有连接逐个进行关闭
	go func() {
		select {
		case <-closeChan:
			logger.Info("get exit signal") // 接收到退出信号的日志
		case er := <-errCh:
			logger.Info(fmt.Sprintf("accept error: %s", er.Error())) // 接收连接时出错的日志
		}
		logger.Info("shutting down...")
		_ = listener.Close() // 关闭监听器
		_ = handler.Close()  // 关闭处理器中的所有连接
	}()

	ctx := context.Background()
	var waitDone sync.WaitGroup
	for {
		conn, err := listener.Accept() // 接受新的连接
		if err != nil {
			// 根据HTTP服务的错误处理方式，处理临时的网络错误
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				logger.Infof("accept occurs temporary error: %v, retry in 5ms", err)
				time.Sleep(5 * time.Millisecond)
				continue
			}
			errCh <- err // 将错误发送到错误通道
			break
		}
		// handle
		logger.Info("accept link") // 记录接受连接的日志
		ClientCounter++            // 客户端计数器增加
		waitDone.Add(1)
		go func() {
			defer func() {
				waitDone.Done()                     // 减少等待组的计数
				atomic.AddInt32(&ClientCounter, -1) // 原子减少客户端计数器
			}()
			handler.Handle(ctx, conn) // 使用传入的handler处理连接
		}()
	}
	waitDone.Wait() // 等待所有连接处理完成
}
