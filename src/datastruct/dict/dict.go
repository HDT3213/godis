package dict

import (
    "math/rand"
    "sync"
    "sync/atomic"
)

type Dict struct {
    table       []*Shard
    count       int32
}

type Shard struct {
    m  map[string]interface{}
    mutex sync.RWMutex
}


func Make(shardCount int) *Dict {
    if shardCount < 1 {
        shardCount = 16
    }
    table := make([]*Shard, shardCount)
    for i := 0; i < shardCount; i++ {
        table[i] = &Shard{
            m: make(map[string]interface{}),
        }
    }
    d := &Dict{
        count:       0,
        table:   table,
    }
    return d
}

const prime32 = uint32(16777619)

func fnv32(key string) uint32 {
    hash := uint32(2166136261)
    for i := 0; i < len(key); i++ {
        hash *= prime32
        hash ^= uint32(key[i])
    }
    return hash
}

func (dict *Dict) spread(hashCode uint32) uint32 {
    if dict == nil {
        panic("dict is nil")
    }
    tableSize := uint32(len(dict.table))
    return (tableSize - 1) & uint32(hashCode)
}

func (dict *Dict) getShard(index uint32) *Shard {
    if dict == nil {
        panic("dict is nil")
    }
    return dict.table[index]
}

func (dict *Dict) Get(key string) (val interface{}, exists bool) {
    if dict == nil {
        panic("dict is nil")
    }
    hashCode := fnv32(key)
    index := dict.spread(hashCode)
    shard := dict.getShard(index)
    shard.mutex.RLock()
    defer shard.mutex.RUnlock()
    val, exists = shard.m[key]
    return
}

func (dict *Dict) Len() int {
    if dict == nil {
        panic("dict is nil")
    }
    return int(atomic.LoadInt32(&dict.count))
}

// return the number of new inserted key-value
func (dict *Dict) Put(key string, val interface{}) (result int) {
    if dict == nil {
        panic("dict is nil")
    }
    hashCode := fnv32(key)
    index := dict.spread(hashCode)
    shard := dict.getShard(index)
    shard.mutex.Lock()
    defer shard.mutex.Unlock()

    if _, ok := shard.m[key]; ok {
        shard.m[key] = val
        return 0
    } else {
        shard.m[key] = val
        dict.addCount()
        return 1
    }
}

// return the number of updated key-value
func (dict *Dict) PutIfAbsent(key string, val interface{}) (result int) {
    if dict == nil {
        panic("dict is nil")
    }
    hashCode := fnv32(key)
    index := dict.spread(hashCode)
    shard := dict.getShard(index)
    shard.mutex.Lock()
    defer shard.mutex.Unlock()

    if _, ok := shard.m[key]; ok {
        return 0
    } else {
        shard.m[key] = val
        dict.addCount()
        return 1
    }
}


// return the number of updated key-value
func (dict *Dict) PutIfExists(key string, val interface{})(result int) {
    if dict == nil {
        panic("dict is nil")
    }
    hashCode := fnv32(key)
    index := dict.spread(hashCode)
    shard := dict.getShard(index)
    shard.mutex.Lock()
    defer shard.mutex.Unlock()

    if _, ok := shard.m[key]; ok {
        shard.m[key] = val
        return 1
    } else {
        return 0
    }
}

// return the number of deleted key-value
func (dict *Dict) Remove(key string)(result int) {
    if dict == nil {
        panic("dict is nil")
    }
    hashCode := fnv32(key)
    index := dict.spread(hashCode)
    shard := dict.getShard(index)
    shard.mutex.Lock()
    defer shard.mutex.Unlock()

    if _, ok := shard.m[key]; ok {
        delete(shard.m, key)
        return 1
    } else {
        return 0
    }
    return
}

func (dict *Dict) addCount() int32 {
    return atomic.AddInt32(&dict.count, 1)
}

type Consumer func(key string, val interface{})bool

/*
 * may not contains new entry inserted during traversal
 */
func (dict *Dict)ForEach(consumer Consumer) {
   if dict == nil {
       panic("dict is nil")
   }

   for _, shard := range dict.table {
       for key, value := range shard.m {
           shard.mutex.RLock()
           continues := consumer(key, value)
           shard.mutex.RUnlock()
           if !continues {
               return
           }
       }
   }
}

func (dict *Dict)Keys()[]string {
    keys := make([]string, dict.Len())
    i := 0
    dict.ForEach(func(key string, val interface{})bool {
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

func (shard *Shard)RandomKey()string {
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

func (dict *Dict)RandomKeys(limit int)[]string {
    size := dict.Len()
    if limit >= size {
        return dict.Keys()
    }
    shardCount := len(dict.table)

    result := make([]string, limit)
    for i := 0; i < limit; {
        shard := dict.getShard(uint32(rand.Intn(shardCount)))
        if shard == nil {
            continue
        }
        key := shard.RandomKey()
        if key != "" {
            result[i] = key
            i++
        }
    }
    return result
}

func (dict *Dict)RandomDistinctKeys(limit int)[]string {
    size := dict.Len()
    if limit >= size {
        return dict.Keys()
    }

    shardCount := len(dict.table)
    result := make(map[string]bool)
    for len(result) < limit {
        shardIndex := uint32(rand.Intn(shardCount))
        shard := dict.getShard(shardIndex)
        if shard == nil {
            continue
        }
        key := shard.RandomKey()
        if key != "" {
            result[key] = true
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