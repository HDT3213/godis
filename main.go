package main

import (
	"fmt"
	"os"

	"github.com/hdt3213/godis/cluster"
	"github.com/hdt3213/godis/config"
	database2 "github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/gnet"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"

	// RedisServer "github.com/hdt3213/godis/redis/server"
	// "github.com/hdt3213/godis/tcp"
	gnetv2 "github.com/panjf2000/gnet/v2"
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
	RunID:          utils.RandString(40),
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	print(banner)
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})
	configFilename := os.Getenv("CONFIG")
	if configFilename == "" {
		if fileExists("redis.conf") {
			config.SetupConfig("redis.conf")
		} else {
			config.Properties = defaultProperties
		}
	} else {
		config.SetupConfig(configFilename)
	}
	listenAddr := fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port)
	// err := tcp.ListenAndServeWithSignal(&tcp.Config{
	// 	Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	// }, RedisServer.MakeHandler())
	// if err != nil {
	// 	logger.Error(err)
	// }

	var db database.DB
	if config.Properties.ClusterEnable {
		db = cluster.MakeCluster()
	} else {
		db = database2.NewStandaloneServer()
	}
	server := gnet.NewGnetServer(db)
	err := gnetv2.Run(server, "tcp://" + listenAddr, gnetv2.WithMulticore(true))	
	if err != nil {
		logger.Errorf("start server failed: %v", err)
	}
}
