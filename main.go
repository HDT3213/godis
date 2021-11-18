package main

import (
	"fmt"
	"os"

	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/logger"
	RedisServer "github.com/hdt3213/godis/redis/server"
	"github.com/hdt3213/godis/tcp"
)

var banner = `
   ______          ___
  / ____/___  ____/ (_)____
 / / __/ __ \/ __  / / ___/
/ /_/ / /_/ / /_/ / (__  )
\____/\____/\__,_/_/____/
`

var defaultProperties = &config.ServerProperties{
	Bind:           "0.0.0.0",
	Port:           6399,
	AppendOnly:     false,
	AppendFilename: "",
	MaxClients:     1000,
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func init() {
	print(banner)

	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})

	configFilename := os.Getenv("CONFIG")
	if configFilename != "" {
		config.SetupConfig(configFilename)
		return
	}
	if fileExists("redis.conf") {
		config.SetupConfig("redis.conf")
		return
	}
	config.Properties = defaultProperties
}

func main() {
	err := tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, RedisServer.MakeHandler())
	if err != nil {
		logger.Error(err)
	}
}
