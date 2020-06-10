package main

import (
	"fmt"
	"github.com/HDT3213/godis/src/config"
	"github.com/HDT3213/godis/src/lib/logger"
    RedisServer "github.com/HDT3213/godis/src/redis/server"
	"github.com/HDT3213/godis/src/tcp"
)

func main() {
	config.SetupConfig("redis.conf")
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})

	tcp.ListenAndServe(&tcp.Config{
		Address:    fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
    }, RedisServer.MakeHandler())
}
