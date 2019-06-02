package server

import (
    "net"
    "fmt"
    "github.com/HDT3213/godis/src/interface/tcp"
    "time"
    "context"
    "github.com/HDT3213/godis/src/lib/logger"
    "os"
    "os/signal"
    "syscall"
    "github.com/HDT3213/godis/src/lib/sync/atomic"
)

type Config struct {
    Address string `yaml:"address"`
    MaxConnect uint32 `yaml:"max-connect"`
    Timeout time.Duration `yaml:"timeout"`
}

func ListenAndServe(cfg *Config, handler tcp.Handler) {
    listener, err := net.Listen("tcp", cfg.Address)
    if err != nil {
        logger.Fatal(fmt.Sprintf("listen err: %v", err))
    }

    // listen signal
    var closing atomic.AtomicBool
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
    go func() {
        sig := <-sigCh
        switch sig {
        case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
            logger.Info("shuting down...")
            closing.Set(true)
            listener.Close() // listener.Accept() will return err immediately
        }
    }()


    // listen port
    logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
    // closing listener than closing handler while shuting down
    defer handler.Close()
    defer listener.Close() // close listener during unexpected error
    ctx, _ := context.WithCancel(context.Background())
    for {
        conn, err := listener.Accept()
        if err != nil {
            if closing.Get() {
                return // handler will be closed by defer
            }
            logger.Error(fmt.Sprintf("accept err: %v", err))
            continue
        }
        // handle
        logger.Info("accept link")
        go handler.Handle(ctx, conn)
    }
}
