package database

import (
	"time"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/rdb/core"
)

// CmdLine 是命令行的别名，表示一个命令行为一个二维字节切片
type CmdLine = [][]byte

// DB 是一个接口，为Redis风格的存储引擎定义了必要的方法
type DB interface {
	Exec(client redis.Connection, cmdLine [][]byte) redis.Reply // 执行给定的命令行，并返回响应
	AfterClientClose(c redis.Connection)                        // 客户端关闭后的回调处理
	Close()                                                     // 关闭数据库连接
	LoadRDB(dec *core.Decoder) error                            // 从RDB解码器加载数据到数据库
}

// KeyEventCallback 是键事件的回调函数类型，如键被插入或删除时调用
// 可能会并发调用
type KeyEventCallback func(dbIndex int, key string, entity *DataEntity)

// DBEngine 是一个更高级的存储引擎接口，提供了更多的方法以支持复杂的应用场景
type DBEngine interface {
	DB
	ExecWithLock(conn redis.Connection, cmdLine [][]byte) redis.Reply                            // 在执行命令时加锁保护
	ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply // 执行多个命令，支持事务
	GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine                                         // 获取撤销日志
	ForEach(dbIndex int, cb func(key string, data *DataEntity, expiration *time.Time) bool)      // 遍历数据库中的键
	RWLocks(dbIndex int, writeKeys []string, readKeys []string)                                  // 读写锁定一组键
	RWUnLocks(dbIndex int, writeKeys []string, readKeys []string)                                // 解锁一组键
	GetDBSize(dbIndex int) (int, int)                                                            // 获取数据库大小
	GetEntity(dbIndex int, key string) (*DataEntity, bool)                                       // 获取与键关联的数据实体
	GetExpiration(dbIndex int, key string) *time.Time                                            // 获取键的过期时间
	SetKeyInsertedCallback(cb KeyEventCallback)                                                  // 设置键插入事件的回调
	SetKeyDeletedCallback(cb KeyEventCallback)                                                   // 设置键删除事件的回调
}

// DataEntity 存储绑定到键的数据，包括字符串、列表、哈希、集合等
type DataEntity struct {
	Data interface{} // 存储实际的数据，数据类型可以是任何类型
}
