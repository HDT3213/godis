# Godis

![license](https://img.shields.io/github/license/HDT3213/godis)
[![Build Status](https://github.com/hdt3213/godis/actions/workflows/coverall.yml/badge.svg)](https://github.com/HDT3213/godis/actions?query=branch%3Amaster)
[![Coverage Status](https://coveralls.io/repos/github/HDT3213/godis/badge.svg?branch=master)](https://coveralls.io/github/HDT3213/godis?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/HDT3213/godis)](https://goreportcard.com/report/github.com/HDT3213/godis)
<br>
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge-flat.svg)](https://github.com/avelino/awesome-go)

Godis 是一个用 Go 语言实现的 Redis 服务器。本项目旨在为尝试使用 Go 语言开发高并发中间件的朋友提供一些参考。

关键功能:
- 支持 string, list, hash, set, sorted set, bitmap 数据结构
- 并行内核，提供更优秀的性能
- 自动过期功能(TTL)
- 发布订阅
- 地理位置
- AOF 持久化、RDB 持久化、aof-use-rdb-preamble 混合持久化
- 主从复制
- Multi 命令开启的事务具有**原子性**和隔离性. 若在执行过程中遇到错误, godis 会回滚已执行的命令
- 内置集群模式. 集群对客户端是透明的, 您可以像使用单机版 redis 一样使用 godis 集群
  - 使用 Raft 算法维护集群元数据。支持动态扩缩容、自动平衡和主从切换。
  - `MSET`, `MSETNX`, `DEL`, `Rename`, `RenameNX`  命令在集群模式下原子性执行, 允许 key 在集群的不同节点上

可以在[我的博客](https://www.cnblogs.com/Finley/category/1598973.html)了解更多关于
Godis 的信息。

# 运行 Godis

在 GitHub 的 release 页下载 Darwin(MacOS) 和 Linux 版可执行文件。使用命令行启动 Godis 服务器

```bash
./godis-darwin
```

```bash
./godis-linux
```

![](https://i.loli.net/2021/05/15/oQM1yZ6pWm3AIEj.png)

godis 默认监听 0.0.0.0:6399，可以使用 redis-cli 或者其它 redis 客户端连接 Godis 服务器。

![](https://i.loli.net/2021/05/15/7WquEgonzY62sZI.png)

godis 首先会从CONFIG环境变量中读取配置文件路径。若环境变量中未设置配置文件路径，则会尝试读取工作目录中的 redis.conf 文件。 

所有配置项均在 [example.conf](./example.conf) 中作了说明。

## 集群模式

可以使用 node1.conf 和 node2.conf 配置文件，在本地启动一个双节点集群:

```bash
CONFIG=node1.conf ./godis-darwin &
CONFIG=node2.conf ./godis-darwin &
```

集群模式对客户端是透明的，只要连接上集群中任意一个节点就可以访问集群中所有数据：

```bash
redis-cli -p 6399
```

更多配置请查阅 [example.conf](./example.conf)

## 支持的命令

请参考 [commands.md](https://github.com/HDT3213/godis/blob/master/commands.md)

## 性能测试

环境:

Go version: 1.23
System: MacOS Monterey 12.5 M2 Air

redis-benchmark 测试结果:

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

## 如何阅读源码

本项目的目录结构:

- 根目录: main 函数，执行入口
- config: 配置文件解析
- interface: 一些模块间的接口定义
- lib: 各种工具，比如logger、同步和通配符

建议按照下列顺序阅读各包:

- tcp: tcp 服务器实现
- redis: redis 协议解析器
- datastruct: redis 的各类数据结构实现
    - dict: hash 表
    - list: 链表
    - lock: 用于锁定 key 的锁组件
    - set： 基于hash表的集合
    - sortedset: 基于跳表实现的有序集合
- database: 存储引擎核心
    - server.go: redis 服务实例, 支持多数据库, 持久化, 主从复制等能力
    - database.go: 单个 database 的数据结构和功能
    - router.go: 将命令路由给响应的处理函数
    - keys.go: del、ttl、expire 等通用命令实现
    - string.go: get、set 等字符串命令实现
    - list.go: lpush、lindex 等列表命令实现
    - hash.go: hget、hset 等哈希表命令实现
    - set.go: sadd 等集合命令实现
    - sortedset.go: zadd 等有序集合命令实现
    - pubsub.go: 发布订阅命令实现
    - geo.go: GEO 相关命令实现
    - sys.go: Auth 等系统功能实现
    - transaction.go: 单机事务实现
- cluster: 集群
  - cluster.go: 集群入口
  - com.go: 节点间通信
  - del.go: delete 命令原子性实现
  - keys.go: key 相关命令集群中实现
  - mset.go: mset 命令原子性实现
  - multi.go: 集群内事务实现
  - pubsub.go: 发布订阅实现
  - rename.go: rename 命令集群实现
  - tcc.go: tcc 分布式事务底层实现
- aof: AOF 持久化实现 