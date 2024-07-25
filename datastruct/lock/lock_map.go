package lock

import (
	"sort"
	"sync"
)

/*
即使使用了 ConcurrentMap 保证了对单个 key 的并发安全性，但在某些情况下这还不够。
例如，Incr 命令需要完成从读取到修改再到写入的一系列操作，而 MSETNX 命令需要在所有给定键都不存在的情况下才设置值。
这些操作需要更复杂的并发控制，即锁定多个键直到操作完成。
*/

/*
锁定哈希槽而非单个键：通常，每个键都需要独立锁定来确保并发访问的安全性。
但这在键数量很大时会造成大量内存的使用。通过锁定哈希槽，可以大大减少所需的锁的数量，因为多个键会映射到同一个哈希槽。
避免内存泄漏：如果为每个键分配一个锁，随着键的增加和删除，锁的数量也会不断增加，可能会造成内存泄漏。
锁定哈希槽可以固定锁的数量，即使在键的数量动态变化时也不会增加额外的内存负担。
*/
//在锁定多个key时需要注意，若协程A持有键a的锁试图获得键b的锁，此时协程B持有键b的锁试图获得键a的锁则会形成死锁。
//解决方法是所有协程都按照相同顺序加锁，若两个协程都想获得键a和键b的锁，那么必须先获取键a的锁后获取键b的锁，这样就可以避免循环等待。
// 使用固定素数，用于FNV哈希函数
const (
	prime32 = uint32(16777619)
)

// Locks结构体包含了一个用于读写锁定的锁表
type Locks struct {
	table []*sync.RWMutex
}

// Make 初始化并返回一个具有指定数量哈希槽的Locks实例
func Make(tableSize int) *Locks {
	table := make([]*sync.RWMutex, tableSize)
	for i := 0; i < tableSize; i++ {
		table[i] = &sync.RWMutex{}
	}
	return &Locks{
		table: table,
	}
}

// fnv32 实现了FNV哈希算法，用于生成键的哈希值
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// spread 计算给定哈希码的哈希槽索引
func (locks *Locks) spread(hashCode uint32) uint32 {
	if locks == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(locks.table))
	return (tableSize - 1) & hashCode
}

// Lock 为给定键获取独占写锁
func (locks *Locks) Lock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Lock()
}

// RLock 为给定键获取共享读锁
func (locks *Locks) RLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.RLock()
}

// UnLock 释放给定键的独占写锁
func (locks *Locks) UnLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Unlock()
}

// RUnLock 释放给定键的共享读锁
func (locks *Locks) RUnLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.RUnlock()
}

// toLockIndices 计算一组键的哈希槽索引，并进行排序（可选逆序）
func (locks *Locks) toLockIndices(keys []string, reverse bool) []uint32 {
	indexMap := make(map[uint32]struct{}) // 使用集合去重
	for _, key := range keys {
		index := locks.spread(fnv32(key))
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

// Locks 获取多个键的独占写锁，避免死锁
func (locks *Locks) Locks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		mu := locks.table[index]
		mu.Lock() // 锁定每个索引对应的哈希槽
	}
}

// RLocks 获取多个键的共享读锁，避免死锁

func (locks *Locks) RLocks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		mu := locks.table[index]
		mu.RLock()
	}
}

// UnLocks 释放多个键的独占写锁
func (locks *Locks) UnLocks(keys ...string) {
	indices := locks.toLockIndices(keys, true)
	for _, index := range indices {
		mu := locks.table[index]
		mu.Unlock()
	}
}

// RUnLocks 释放多个键的共享读锁
func (locks *Locks) RUnLocks(keys ...string) {
	indices := locks.toLockIndices(keys, true)
	for _, index := range indices {
		mu := locks.table[index]
		mu.RUnlock()
	}
}

// RWLocks 同时获取写锁和读锁，允许键重复
func (locks *Locks) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locks.toLockIndices(keys, false)
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := locks.spread(fnv32(wKey))
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := locks.table[index]
		if w {
			mu.Lock()
		} else {
			mu.RLock()
		}
	}
}

// RWUnLocks 同时释放写锁和读锁，允许键重复
func (locks *Locks) RWUnLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locks.toLockIndices(keys, true)
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := locks.spread(fnv32(wKey))
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := locks.table[index]
		if w {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
	}
}
