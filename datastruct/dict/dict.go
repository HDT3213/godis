package dict

// Consumer 是一个遍历字典时使用的函数类型，如果返回false，则终止遍历
type Consumer func(key string, val interface{}) bool

// Dict 是 key-value 数据结构的接口定义
type Dict interface {
	Get(key string) (val interface{}, exists bool)        // 获取键的值
	Len() int                                             // 返回字典的长度
	Put(key string, val interface{}) (result int)         // 插入键值对
	PutIfAbsent(key string, val interface{}) (result int) // 如果键不存在，则插入
	PutIfExists(key string, val interface{}) (result int) // 如果键存在，则插入
	Remove(key string) (val interface{}, result int)      // 移除键
	ForEach(consumer Consumer)                            // 遍历字典,传进去的函数是Consumer
	Keys() []string                                       // 返回所有键的列表
	RandomKeys(limit int) []string                        // 随机返回一定数量的键
	RandomDistinctKeys(limit int) []string                //返回一些不重复的键
	Clear()                                               // 清除字典中的所有元素
	DictScan(cursor int, count int, pattern string) ([][]byte, int)
}
