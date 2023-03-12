package servercli

import (
	"fmt"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/logger"
	RedisServer "github.com/hdt3213/godis/redis/server"
	"github.com/hdt3213/godis/tcp"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
)

var defaultProperties = &config.ServerProperties{
	Bind:           "0.0.0.0",
	Port:           6399,
	AppendOnly:     false,
	AppendFilename: "",
	MaxClients:     1000,
}

var banner = `
   ______          ___
  / ____/___  ____/ (_)____
 / / __/ __ \/ __  / / ___/
/ /_/ / /_/ / /_/ / (__  )
\____/\____/\__,_/_/____/
`

var rootCmd = &cobra.Command{
	Use:   "godis",
	Short: "godis is a golang implementation of Redis Server, which intents to provide an example of writing a high concurrent middleware using golang.",
	Run: func(cmd *cobra.Command, args []string) {
		f := ""
		StartServer(f)
	},
}

// AddCommand add command into Cli
func AddCommand(cmdline *cobra.Command) {
	rootCmd.AddCommand(cmdline)
}

func StartServer(cf string) {
	configFilename := os.Getenv("CONFIG")
	if configFilename == "" {
		if fileExists(cf) {
			config.SetupConfig(cf)
			abs, err := filepath.Abs(cf)
			if err == nil {
				config.Properties.CfPath = abs
			}
		} else {
			config.Properties = defaultProperties
		}
	} else {
		config.SetupConfig(configFilename)
	}

	print(banner)
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})
	err := tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, RedisServer.MakeHandler())
	if err != nil {
		logger.Error(err)
	}
}

func Execute() error {
	return rootCmd.Execute()
}
