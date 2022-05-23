# Godis

![license](https://img.shields.io/github/license/HDT3213/godis)
[![Build Status](https://travis-ci.com/HDT3213/godis.svg?branch=master)](https://app.travis-ci.com/github/HDT3213/godis)
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
- Multi Database and `SELECT` command
- TTL
- Publish/Subscribe
- GEO
- AOF and AOF Rewrite
- RDB read and write
- MULTI Commands Transaction is Atomic and Isolated. If any errors are encountered during execution, godis will rollback
  the executed commands
- Server-side Cluster which is transparent to client. You can connect to any node in the cluster to
  access all data in the cluster.
  - `MSET`, `MSETNX`, `DEL`, `Rename`, `RenameNX` command is supported and atomically executed in cluster mode, allow over multi node
  - `MULTI` Commands Transaction is supported within slot in cluster mode
- Concurrent Core, so you don't have to worry about your commands blocking the server too much. 

If you could read Chinese, you can find more details in [My Blog](https://www.cnblogs.com/Finley/category/1598973.html).

## Get Started

You can get runnable program in the releases of this repository, which supports Linux and Darwin system.

```bash
./godis-darwin
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

Go version：1.16

System: macOS Catalina 10.15.7

CPU: 2.6GHz 6-Core Intel Core i7

Memory: 16 GB 2667 MHz DDR4

Performance report by redis-benchmark: 

```
PING_INLINE: 87260.03 requests per second
PING_BULK: 89206.06 requests per second
SET: 85034.02 requests per second
GET: 87565.68 requests per second
INCR: 91157.70 requests per second
LPUSH: 90334.23 requests per second
RPUSH: 90334.23 requests per second
LPOP: 90334.23 requests per second
RPOP: 90415.91 requests per second
SADD: 90909.09 requests per second
HSET: 84104.29 requests per second
SPOP: 82918.74 requests per second
LPUSH (needed to benchmark LRANGE): 78247.26 requests per second
LRANGE_100 (first 100 elements): 26406.13 requests per second
LRANGE_300 (first 300 elements): 11307.10 requests per second
LRANGE_500 (first 450 elements): 7968.13 requests per second
LRANGE_600 (first 600 elements): 6092.73 requests per second
MSET (10 keys): 65487.89 requests per second
```

## Todo List

+ [x] `Multi` Command
+ [x] `Watch` Command and CAS support
+ [ ] Stream support
+ [x] RDB file loader
+ [ ] Master-Slave mode
+ [ ] Sentinel

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
    - database.go: a standalone redis server, with multiple database 
    - single_db.go: data structure and base functions of single database
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