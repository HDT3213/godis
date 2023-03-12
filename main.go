package main

import (
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/servercli"
)

func main() {
	cliExecuteErr := servercli.Execute()
	if cliExecuteErr != nil {
		logger.Error(cliExecuteErr)
		return
	}
}
