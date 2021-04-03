Godis 是一个用 Go 语言实现的 Redis 服务器。本项目旨在为尝试使用 Go 语言开发高并发中间件的朋友提供一些参考。

**请注意：不要在生产环境使用使用此项目**

Godis 实现了 Redis 的大多数功能，包括5种数据结构、TTL、发布订阅以及 AOF 持久化。可以在[我的博客](https://www.cnblogs.com/Finley/category/1598973.html)了解更多关于
Godis 的信息。

# 运行 Godis

在 GitHub 的 release 页下载 Darwin(MacOS) 和 Linux 版可执行文件。使用命令行启动 Godis 服务器

```bash
./godis-darwin
./godis-linux
```

godis 默认监听 127.0.0.1:6379，可以使用 redis-cli 或者其它 redis 客户端连接 Godis 服务器。

godis 首先会从CONFIG环境变量中读取配置文件路径。若环境变量中未设置配置文件路径，则会读取工作目录中的 redis.conf 文件。若 redis.conf 文件不存在则会使用自带的默认配置。

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

- Keys
    - del
    - expire
    - expireat
    - pexpire
    - pexpireat
    - ttl
    - pttl
    - persist
    - exists
    - type
    - rename
    - renamenx
- Server
    - flushdb
    - flushall
    - keys
    - bgrewriteaof
- String
    - set
    - setnx
    - setex
    - psetex
    - mset
    - mget
    - msetnx
    - get
    - getset
    - incr
    - incrby
    - incrbyfloat
    - decr
    - decrby
- List
    - lpush
    - lpushx
    - rpush
    - rpushx
    - lpop
    - rpop
    - rpoplpush
    - lrem
    - llen
    - lindex
    - lset
    - lrange
- Hash
    - hset
    - hsetnx
    - hget
    - hexists
    - hdel
    - hlen
    - hmget
    - hmset
    - hkeys
    - hvals
    - hgetall
    - hincrby
    - hincrbyfloat
- Set
    - sadd
    - sismember
    - srem
    - scard
    - smembers
    - sinter
    - sinterstore
    - sunion
    - sunionstore
    - sdiff
    - sdiffstore
    - srandmember
- SortedSet
    - zadd
    - zscore
    - zincrby
    - zrank
    - zcount
    - zrevrank
    - zcard
    - zrange
    - zrevrange
    - zrangebyscore
    - zrevrangebyscore
    - zrem
    - zremrangebyscore
    - zremrangebyrank
- Pub / Sub
    - publish
    - subscribe
    - unsubscribe

## 如何阅读源码

本项目的目录结构:

- cmd: main 函数，执行入口
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
- db: redis 存储引擎实现
    - db.go: 引擎的基础功能
    - router.go: 将命令路由给响应的处理函数
    - keys.go: del、ttl、expire 等通用命令实现
    - string.go: get、set 等字符串命令实现
    - list.go: lpush、lindex 等列表命令实现
    - hash.go: hget、hset 等哈希表命令实现
    - set.go: sadd 等集合命令实现
    - sortedset.go: zadd 等有序集合命令实现
    - pubsub.go: 发布订阅命令实现
    - aof.go: aof持久化实现