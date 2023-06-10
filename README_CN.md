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
- 自动过期功能(TTL)
- 发布订阅
- 地理位置
- AOF 持久化及 AOF 重写
- 加载和导出 RDB 文件
- 主从复制 (测试中)
- Multi 命令开启的事务具有`原子性`和`隔离性`. 若在执行过程中遇到错误, godis 会回滚已执行的命令
- 内置集群模式. 集群对客户端是透明的, 您可以像使用单机版 redis 一样使用 godis 集群
  - 使用 raft 算法维护集群元数据(测试中)
  - `MSET`, `MSETNX`, `DEL`, `Rename`, `RenameNX`  命令在集群模式下原子性执行, 允许 key 在集群的不同节点上
  - 在集群模式下支持在同一个 slot 内执行事务
- 并行引擎, 无需担心您的操作会阻塞整个服务器.

可以在[我的博客](https://www.cnblogs.com/Finley/category/1598973.html)了解更多关于
Godis 的信息。

# 运行 Godis

在 GitHub 的 release 页下载 Darwin(MacOS) 和 Linux 版可执行文件。使用命令行启动 Godis 服务器

```bash
./godis-darwin
./godis-linux
```

![](https://i.loli.net/2021/05/15/oQM1yZ6pWm3AIEj.png)

godis 默认监听 0.0.0.0:6399，可以使用 redis-cli 或者其它 redis 客户端连接 Godis 服务器。

![](https://i.loli.net/2021/05/15/7WquEgonzY62sZI.png)

godis 首先会从CONFIG环境变量中读取配置文件路径。若环境变量中未设置配置文件路径，则会尝试读取工作目录中的 redis.conf 文件。 若 redis.conf 文件不存在则会使用自带的默认配置。

## 集群模式

godis 支持以集群模式运行，请在 redis.conf 文件中添加下列配置:

```ini
peers localhost:7379,localhost:7389 // 集群中其它节点的地址
self  localhost:6399 // 自身地址
```

可以使用 node1.conf 和 node2.conf 配置文件，在本地启动一个双节点集群:

```bash
CONFIG=node1.conf ./godis-darwin &
CONFIG=node2.conf ./godis-darwin &
```

集群模式对客户端是透明的，只要连接上集群中任意一个节点就可以访问集群中所有数据：

```bash
redis-cli -p 6399
```

## 支持的命令

请参考 [commands.md](https://github.com/HDT3213/godis/blob/master/commands.md)

## 性能测试

环境:

Go version：1.17

System: macOS Catalina 10.15.7

CPU: 2.6GHz 6-Core Intel Core i7

Memory: 16 GB 2667 MHz DDR4

redis-benchmark 测试结果:

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

## 开发计划

+ [x] `Multi` 命令
+ [x] `Watch` 命令和 CAS 支持
+ [ ] Stream 队列 
+ [ ] 加载 RDB 文件
+ [ ] 主从模式
+ [ ] 哨兵

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