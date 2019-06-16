package dict

import (
    "sync"
    "sync/atomic"
)

type Dict struct {
    shards []*Shard
    shardCount int
    count int32
}

type Shard struct {
    table map[string]interface{}
    mutex sync.RWMutex
}

const (
    maxCapacity = 1 << 16
    minCapacity = 256
)

// return the mini power of two which is not less than cap
// See Hackers Delight, sec 3.2
func computeCapacity(param int)(size int) {
    if param <= minCapacity {
        return minCapacity
    }
    n := param - 1
    n |= n >> 1
    n |= n >> 2
    n |= n >> 4
    n |= n >> 8
    n |= n >> 16
    if n < 0 || n >= maxCapacity {
        return maxCapacity
    } else {
        return int(n + 1)
    }
}

func Make(shardCountHint int)*Dict {
    shardCount := computeCapacity(shardCountHint)
    shards := make([]*Shard, shardCount)
    for i := 0; i < shardCount; i++ {
        shards[i] = &Shard{
            table: make(map[string]interface{}),
        }
    }
    return &Dict{
        shards: shards,
        shardCount: shardCount,
        count: 0,
    }
}

func fnv32(key string) uint32 {
    hash := uint32(2166136261)
    const prime32 = uint32(16777619)
    for i := 0; i < len(key); i++ {
        hash *= prime32
        hash ^= uint32(key[i])
    }
    return hash
}


func (d *Dict)spread(key string) int {
    h := int(fnv32(key))
    return (d.shardCount - 1) & h
}

func (d *Dict)Get(key string)(val interface{}, exists bool) {
   shard := d.shards[d.spread(key)]
   shard.mutex.RLock()
   defer shard.mutex.RUnlock()

   val, ok := shard.table[key]
   return val, ok
}

func (d *Dict)Len()int {
    return int(atomic.LoadInt32(&d.count))
}

// return the number of new inserted key-value
func (d *Dict)Put(key string, val interface{})int {
    shard := d.shards[d.spread(key)]
    shard.mutex.Lock()
    defer shard.mutex.Unlock()

    _, existed := shard.table[key]
    shard.table[key] = val

    if existed {
        // update
        return 0
    } else {
        // insert
        atomic.AddInt32(&d.count, 1)
        return 1
    }
}

// return the number of updated key-value
func (d *Dict)PutIfAbsent(key string, val interface{})int {
    shard := d.shards[d.spread(key)]
    shard.mutex.Lock()
    defer shard.mutex.Unlock()

    _, existed := shard.table[key]

    if existed {
        return 0
    } else {
        // update
        shard.table[key] = val
        return 1
    }
}

// return the number of deleted key-value
func (d *Dict)Remove(key string)int {
    shard := d.shards[d.spread(key)]
    shard.mutex.Lock()
    defer shard.mutex.Unlock()

    _, existed := shard.table[key]
    delete(shard.table, key)

    if existed {
        atomic.AddInt32(&d.count, -1)
        return 1
    } else {
        return 0
    }
}

