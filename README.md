# Godis

![license](https://img.shields.io/github/license/HDT3213/godis)
[![Build Status](https://github.com/hdt3213/godis/actions/workflows/coverall.yml/badge.svg)](https://github.com/HDT3213/godis/actions?query=branch%3Amaster)
[![Coverage Status](https://coveralls.io/repos/github/HDT3213/godis/badge.svg?branch=master)](https://coveralls.io/github/HDT3213/godis?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/HDT3213/godis)](https://goreportcard.com/report/github.com/HDT3213/godis)
[![Go Reference](https://pkg.go.dev/badge/github.com/hdt3213/godis.svg)](https://pkg.go.dev/github.com/hdt3213/godis)
<br>
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge-flat.svg)](https://github.com/avelino/awesome-go)

[中文版](https://github.com/hdt3213/godis/blob/master/README_CN.md)

`Godis` is a golang implementation of Redis Server, which intents to provide an example of writing a high concurrent
middleware using golang.

Key Features:

- Support string, list, hash, set, sorted set, bitmap
- Concurrent Core for better performance
- TTL
- Publish/Subscribe
- GEO
- AOF and AOF Rewrite
- RDB read and write
- Multi Database and `SELECT` command
- Transaction is **Atomic** and Isolated. If any errors are encountered during execution, godis will rollback the executed commands
- Replication
- Server-side Cluster which is transparent to client. You can connect to any node in the cluster to access all data in the cluster.
  - Use the raft algorithm to maintain cluster metadata. (experimental)
  - `MSET`, `MSETNX`, `DEL`, `Rename`, `RenameNX` command is supported and atomically executed in cluster mode, allow over multi node
  - `MULTI` Commands Transaction is supported within slot in cluster mode

If you could read Chinese, you can find more details in [My Blog](https://www.cnblogs.com/Finley/category/1598973.html).

## Get Started

You can get runnable program in the releases of this repository, which supports Linux and Darwin system.

```bash
./godis-darwin
```

```bash
./godis-linux
```

![](https://i.loli.net/2021/05/15/oQM1yZ6pWm3AIEj.png)

You could use redis-cli or other redis client to connect godis server, which listens on 0.0.0.0:6399 on default mode.

![](https://i.loli.net/2021/05/15/7WquEgonzY62sZI.png)

The program will try to read config file path from environment variable `CONFIG`.

If environment variable is not set, then the program try to read `redis.conf` in the working directory.

If there is no such file, then the program will run with default config.

### cluster mode

Godis can work in cluster mode, please append following lines to redis.conf file

```ini
peers localhost:7379,localhost:7389 // other node in cluster
self  localhost:6399 // self address
```

We provide node1.conf and node2.conf for demonstration. use following command line to start a two-node-cluster:

```bash
CONFIG=node1.conf ./godis-darwin &
CONFIG=node2.conf ./godis-darwin &
``` 

Connect to a node in the cluster to access all data in the cluster:

```cmd
redis-cli -p 6399
```

## Supported Commands

See: [commands.md](https://github.com/HDT3213/godis/blob/master/commands.md)

## Benchmark

Environment:

Go version：1.23
System: MacOS Monterey 12.5 M2 Air

Performance report by redis-benchmark: 

```
PING_INLINE: 179211.45 requests per second, p50=1.031 msec                    
PING_MBULK: 173611.12 requests per second, p50=1.071 msec                    
SET: 158478.61 requests per second, p50=1.535 msec                    
GET: 156985.86 requests per second, p50=1.127 msec                    
INCR: 164473.69 requests per second, p50=1.063 msec                    
LPUSH: 151285.92 requests per second, p50=1.079 msec                    
RPUSH: 176678.45 requests per second, p50=1.023 msec                    
LPOP: 177619.89 requests per second, p50=1.039 msec                    
RPOP: 172413.80 requests per second, p50=1.039 msec                    
SADD: 159489.64 requests per second, p50=1.047 msec                    
HSET: 175131.36 requests per second, p50=1.031 msec                    
SPOP: 170648.45 requests per second, p50=1.031 msec                    
ZADD: 165289.25 requests per second, p50=1.039 msec                    
ZPOPMIN: 185528.77 requests per second, p50=0.999 msec                    
LPUSH (needed to benchmark LRANGE): 172117.05 requests per second, p50=1.055 msec                    
LRANGE_100 (first 100 elements): 46511.62 requests per second, p50=4.063 msec                   
LRANGE_300 (first 300 elements): 21217.91 requests per second, p50=9.311 msec                     
LRANGE_500 (first 500 elements): 13331.56 requests per second, p50=14.407 msec                    
LRANGE_600 (first 600 elements): 11153.25 requests per second, p50=17.007 msec                    
MSET (10 keys): 88417.33 requests per second, p50=3.687 msec  
```

## Read My Code

If you want to read my code in this repository, here is a simple guidance.

- project root: only the entry point
- config: config parser
- interface: some interface definitions
- lib: some utils, such as logger, sync utils and wildcard

I suggest focusing on the following directories:

- tcp: the tcp server
- redis: the redis protocol parser
- datastruct: the implements of data structures
    - dict: a concurrent hash map
    - list: a linked list
    - lock: it is used to lock keys to ensure thread safety
    - set: a hash set based on map
    - sortedset: a sorted set implements based on skiplist
- database: the core of storage engine
    - server.go: a standalone redis server, with multiple database
    - database.go: data structure and base functions of single database
    - exec.go: the gateway of database
    - router.go: the command table
    - keys.go: handlers for keys commands
    - string.go: handlers for string commands
    - list.go: handlers for list commands
    - hash.go: handlers for hash commands
    - set.go: handlers for set commands
    - sortedset.go: handlers for sorted set commands
    - pubsub.go: implements of publish / subscribe
    - aof.go: implements of AOF persistence and rewrite
    - geo.go: implements of geography features
    - sys.go: authentication and other system function
    - transaction.go: local transaction
- cluster: 
    - cluster.go: entrance of cluster mode
    - com.go: communication within nodes
    - del.go: atomic implementation of `delete` command in cluster
    - keys.go: keys command
    - mset.go: atomic implementation of `mset` command in cluster
    - multi.go: entrance of distributed transaction
    - pubsub.go: pub/sub in cluster
    - rename.go: `rename` command in cluster 
    - tcc.go: try-commit-catch distributed transaction implementation
- aof: AOF persistence

# License

This project is licensed under the [GPL license](https://github.com/hdt3213/godis/blob/master/LICENSE).