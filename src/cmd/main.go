package main

import (
    "github.com/HDT3213/godis/src/lib/logger"
    "github.com/HDT3213/godis/src/redis/handler"
    "github.com/HDT3213/godis/src/server"
    "time"
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
    }, handler.MakeHandler())
}
