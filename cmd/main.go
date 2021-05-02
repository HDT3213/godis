package main

import (
	_ "embed"
	"fmt"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/logger"
	RedisServer "github.com/hdt3213/godis/redis/server"
	"github.com/hdt3213/godis/tcp"
	"os"
)

//go:embed banner.txt
var banner string

func main() {
	print(banner)
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
