package main

import (
    "github.com/HDT3213/godis/src/server"
    "time"
    "github.com/HDT3213/godis/src/lib/logger"
)

func main() {
    logger.Setup(&logger.Settings{
        Path: "logs",
        Name: "godis",
        Ext: ".log",
        TimeFormat: "2006-01-02",
    })

    server.ListenAndServe(&server.Config{
        Address: ":6399",
        MaxConnect: 16,
        Timeout: 2 * time.Second,
    }, server.MakeEchoHandler())
}
