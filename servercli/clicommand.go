package servercli

import (
	"fmt"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/spf13/cobra"
	"os"
	"strconv"
)

var godisCreate = &cobra.Command{
	Use:   "create [redis config filepath]",
	Short: "Create a godis from the configuration file",
	Args:  cobra.MinimumNArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		for _, f := range args {
			StartServer(f)
		}
	},
	PersistentPreRunE: func(cmdline *cobra.Command, args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("just enter the godis configration file path")
		}
		return nil
	},
}

var commandWithPort = &cobra.Command{
	Use:   "port [redis port]",
	Short: "Start godis with the given port",
	Args:  cobra.MinimumNArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		for _, f := range args {
			n, err := IsNum(f)
			if err != nil {
				logger.Error(err)
				return
			}
			defaultProperties.Port = int(n)
			StartServer(f)
		}
	},
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func IsNum(s string) (uint64, error) {
	n, err := strconv.ParseUint(s, 10, 16)
	if err != nil {
		return 0, err
	}
	if n < 1024 || n > 65535 {
		return 0, fmt.Errorf("listening port is greater than 65535 or less than 1024")
	}
	return n, nil
}

func init() {
	AddCommand(godisCreate)
	AddCommand(commandWithPort)
}
