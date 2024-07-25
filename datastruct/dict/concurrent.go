package dict

import (
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hdt3213/godis/lib/wildcard"
)

// ConcurrentDict 是一个使用分片锁的线程安全的哈希表
type ConcurrentDict struct {
	table      []*shard // 分片的数组，每个分片持有一部分键值对
	count      int32    // 键值对的总数量
	shardCount int      // 分片的数量
}

// shard 代表单个分片，包含一个map和一个读写锁，该map中有多个锁
type shard struct {
	m     map[string]interface{} // 存储键值对的哈希表
	mutex sync.RWMutex           // 保护map的读写锁
}

// computeCapacity 计算哈希表容量，确保为2的次幂，这有助于更快的计算索引
// 这个函数的目的是确定一个接近于给定参数 param 但不小于它的2的次幂的数。这样做的好处主要有两点：
func computeCapacity(param int) (size int) {
	// 以下代码是经典的位操作技巧，用于找到大于等于param的最小2的次幂
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

// MakeConcurrent 创建一个具有指定分片数量的ConcurrentDict
func MakeConcurrent(shardCount int) *ConcurrentDict {
	shardCount = computeCapacity(shardCount)
	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{
			m: make(map[string]interface{}),
		}
	}
	d := &ConcurrentDict{
		count:      0,
		table:      table,
		shardCount: shardCount,
	}
	return d
}

/*
这部分代码定义了 ConcurrentDict 和它的构造函数。
通过指定分片数来创建字典，每个分片包含一个map和一个读写锁。这种设计可以减小锁的粒度，提高并发性能。
*/
//----------------------------------------------------------------------------------------------------

const prime32 = uint32(16777619) // 用于哈希函数的质数基
// fnv32 是一种哈希函数，使用FNV算法为给定的键生成32位哈希码
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// spread 将哈希码散列到一个合适的分片索引
func (dict *ConcurrentDict) spread(hashCode uint32) uint32 {
	if dict == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(dict.table))

	//因为表的大小是2的次幂，这个按位与操作等同于对表大小取模，但运算速度更快。
	//当表大小是2的次幂时，tableSize - 1 的二进制表示中所有低位都是1，这使得按位与操作只保留 hashCode 的最低的几位。
	return (tableSize - 1) & hashCode
}

//fnv32 是基于 FNV (Fowler-Noll-Vo) 算法的哈希函数，适用于快速散列字符串。
//spread 函数通过位运算快速定位哈希码应该映射到哪一个分片。
//--------------------------------------------------------------------------------------------------------

// getShard 根据索引获取对应的分片
func (dict *ConcurrentDict) getShard(index uint32) *shard {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.table[index]
}

// Get 返回给定键的值以及是否存在该键
func (dict *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	s.mutex.RLock()         // 对分片加读锁
	defer s.mutex.RUnlock() // 读锁结束时解锁
	val, exists = s.m[key]
	return
}

// GetWithLock 是一个示例方法，显示如何不使用锁安全地获取值
func (dict *ConcurrentDict) GetWithLock(key string) (val interface{}, exists bool) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	val, exists = s.m[key]
	return
}

// Len 返回字典中键值对的数量
func (dict *ConcurrentDict) Len() int {
	if dict == nil {
		panic("dict is nil")
	}
	return int(atomic.LoadInt32(&dict.count))
}

// Put 将键值对放入字典中，并返回是否插入了新的键
func (dict *ConcurrentDict) Put(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 0
	}
	dict.addCount()
	s.m[key] = val
	return 1
}

// PutWithLock 是一个示例方法，显示如何不使用锁安全地放入键值对
func (dict *ConcurrentDict) PutWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 0
	}
	dict.addCount()
	s.m[key] = val
	return 1
}

// PutIfAbsent 尝试只在键不存在时插入键值对，并返回是否插入了键
func (dict *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		return 0
	}
	s.m[key] = val
	dict.addCount()
	return 1
}

// PutIfAbsentWithLock 是一个示例方法，显示如何不使用锁安全地尝试插入键值对
func (dict *ConcurrentDict) PutIfAbsentWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)

	if _, ok := s.m[key]; ok {
		return 0
	}
	s.m[key] = val
	dict.addCount()
	return 1
}

// PutIfExists 尝试只在键存在时更新键值对，并返回是否更新了键
func (dict *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}

// PutIfExistsWithLock 是一个示例方法，显示如何不使用锁安全地尝试更新键值对
func (dict *ConcurrentDict) PutIfExistsWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}

// Remove 删除键，并返回被删除的值及是否成功删除
func (dict *ConcurrentDict) Remove(key string) (val interface{}, result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if val, ok := s.m[key]; ok {
		delete(s.m, key)
		dict.decreaseCount()
		return val, 1
	}
	return nil, 0
}

// RemoveWithLock 是一个示例方法，显示如何不使用锁安全地删除键
func (dict *ConcurrentDict) RemoveWithLock(key string) (val interface{}, result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)

	if val, ok := s.m[key]; ok {
		delete(s.m, key)
		dict.decreaseCount()
		return val, 1
	}
	return val, 0
}

// addCount 增加字典中的键值对总数
func (dict *ConcurrentDict) addCount() int32 {
	return atomic.AddInt32(&dict.count, 1)
}

// decreaseCount 减少字典中的键值对总数
func (dict *ConcurrentDict) decreaseCount() int32 {
	return atomic.AddInt32(&dict.count, -1)
}

// ForEach 遍历字典
// 注意：遍历期间插入的新条目可能不会被访问
func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		panic("dict is nil")
	}

	for _, s := range dict.table {
		s.mutex.RLock()
		f := func() bool {
			defer s.mutex.RUnlock()
			for key, value := range s.m {
				continues := consumer(key, value)
				if !continues {
					return false
				}
			}
			return true
		}
		if !f() {
			break
		}
	}
}

// Keys 返回字典中所有的键
func (dict *ConcurrentDict) Keys() []string {
	keys := make([]string, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		if i < len(keys) {
			keys[i] = key
			i++
		} else {
			keys = append(keys, key)
		}
		return true
	})
	return keys
}

// RandomKey 随机返回一个键
func (shard *shard) RandomKey() string {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	for key := range shard.m {
		return key
	}
	return ""
}

// RandomKeys 随机返回指定数量的键，可能包含重复的键
func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}
	shardCount := len(dict.table)

	result := make([]string, limit)
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < limit; {
		s := dict.getShard(uint32(nR.Intn(shardCount)))
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if key != "" {
			result[i] = key
			i++
		}
	}
	return result
}

// RandomDistinctKeys 随机返回指定数量的不重复键
func (dict *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}

	shardCount := len(dict.table)
	result := make(map[string]struct{})
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(result) < limit {
		shardIndex := uint32(nR.Intn(shardCount))
		s := dict.getShard(shardIndex)
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if key != "" {
			if _, exists := result[key]; !exists {
				result[key] = struct{}{}
			}
		}
	}
	arr := make([]string, limit)
	i := 0
	for k := range result {
		arr[i] = k
		i++
	}
	return arr
}

// Clear 清空字典中的所有键
func (dict *ConcurrentDict) Clear() {
	*dict = *MakeConcurrent(dict.shardCount)
}

func (dict *ConcurrentDict) toLockIndices(keys []string, reverse bool) []uint32 {
	indexMap := make(map[uint32]struct{})
	for _, key := range keys {
		index := dict.spread(fnv32(key))
		indexMap[index] = struct{}{}
	}
	indices := make([]uint32, 0, len(indexMap))
	for index := range indexMap {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] < indices[j]
		}
		return indices[i] > indices[j]
	})
	return indices
}

// RWLocks 锁定写键和读键，允许重复键
func (dict *ConcurrentDict) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := dict.toLockIndices(keys, false)
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := dict.spread(fnv32(wKey))
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := &dict.table[index].mutex
		if w {
			mu.Lock()
		} else {
			mu.RLock()
		}
	}
}

// RWUnLocks 解锁写键和读键，允许重复键
func (dict *ConcurrentDict) RWUnLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := dict.toLockIndices(keys, true)
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := dict.spread(fnv32(wKey))
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := &dict.table[index].mutex
		if w {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
	}
}

// stringsToBytes 将字符串切片转换为字节切片的切片。
// 输入：strSlice - 字符串切片
// 输出：每个字符串转换为字节切片后的切片
func stringsToBytes(strSlice []string) [][]byte {
	byteSlice := make([][]byte, len(strSlice))
	for i, str := range strSlice {
		byteSlice[i] = []byte(str)
	}
	return byteSlice
}

func (dict *ConcurrentDict) DictScan(cursor int, count int, pattern string) ([][]byte, int) {
	size := dict.Len()
	result := make([][]byte, 0)

	if pattern == "*" && count >= size {
		return stringsToBytes(dict.Keys()), 0
	}

	matchKey, err := wildcard.CompilePattern(pattern)
	if err != nil {
		return result, -1
	}

	shardCount := len(dict.table)
	shardIndex := cursor

	for shardIndex < shardCount {
		shard := dict.table[shardIndex]
		shard.mutex.RLock()
		if len(result)+len(shard.m) > count && shardIndex > cursor {
			shard.mutex.RUnlock()
			return result, shardIndex
		}

		for key := range shard.m {
			if pattern == "*" || matchKey.IsMatch(key) {
				result = append(result, []byte(key))
			}
		}
		shard.mutex.RUnlock()
		shardIndex++
	}

	return result, 0
}
