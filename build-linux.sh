#!/usr/bin/env bash

GOOS=linux GOARCH=amd64 go build -o target/godis-linux ./src/cmd