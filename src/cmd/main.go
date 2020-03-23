package main

import (
	"fmt"
	"github.com/HDT3213/godis/src/config"
	"github.com/HDT3213/godis/src/lib/logger"
	"github.com/HDT3213/godis/src/redis/handler"
	"github.com/HDT3213/godis/src/server"
	"time"
)

func main() {
	config.SetupConfig("redis.conf")
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})

	server.ListenAndServe(&server.Config{
		Address:    fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
		MaxConnect: uint32(config.Properties.MaxClients),
		Timeout:    2 * time.Second,
	}, handler.MakeHandler())
}
