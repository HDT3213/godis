package main

import (
	"fmt"
	"github.com/hdt3213/godis/src/config"
	"github.com/hdt3213/godis/src/lib/logger"
	RedisServer "github.com/hdt3213/godis/src/redis/server"
	"github.com/hdt3213/godis/src/tcp"
	"os"
)

func main() {
	configFilename := os.Getenv("CONFIG")
	if configFilename == "" {
		configFilename = "redis.conf"
	}
	config.SetupConfig(configFilename)
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})

	err := tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, RedisServer.MakeHandler())
	if err != nil {
		logger.Error(err)
	}
}
