package main

import (
	"fmt"
	"os"

	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
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
	Bind:           "0.0.0.0",            // 默认绑定的IP地址
	Port:           6399,                 // 默认端口号
	AppendOnly:     false,                // 是否开启追加模式
	AppendFilename: "",                   // 追加模式的文件名
	MaxClients:     1000,                 // 最大客户端连接数
	RunID:          utils.RandString(40), // 生成一个随机的运行ID
}

// fileExists 检查文件是否存在且不是目录
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	print(banner)                  // 打印启动标志
	logger.Setup(&logger.Settings{ // 设置日志文件配置
		Path:       "logs",
		Name:       "godis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})
	configFilename := os.Getenv("CONFIG") // 获取环境变量中的配置文件名
	if configFilename == "" {
		if fileExists("redis.conf") { // 检查默认配置文件是否存在
			config.SetupConfig("redis.conf") // 使用默认配置文件
		} else {
			config.Properties = defaultProperties // 使用内置默认配置
		}
	} else {
		config.SetupConfig(configFilename) // 使用环境变量指定的配置文件
	}
	err := tcp.ListenAndServeWithSignal(&tcp.Config{ // 启动TCP服务器
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, RedisServer.MakeHandler()) //调用我们的RedisServer.MakeHandler
	if err != nil {
		logger.Error(err)
	}
}
