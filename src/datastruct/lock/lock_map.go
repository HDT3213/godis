package lock

import (
    "fmt"
    "runtime"
    "sort"
    "strconv"
    "strings"
    "sync"
    "testing"
    "time"
)

const (
    prime32 = uint32(16777619)
)

type Locks struct {
    table []*sync.RWMutex
}

func Make(tableSize int) *Locks {
    table := make([]*sync.RWMutex, tableSize)
    for i := 0; i < tableSize; i++ {
        table[i] = &sync.RWMutex{}
    }
    return &Locks{
        table: table,
    }
}

func fnv32(key string) uint32 {
    hash := uint32(2166136261)
    for i := 0; i < len(key); i++ {
        hash *= prime32
        hash ^= uint32(key[i])
    }
    return hash
}

func (locks *Locks) spread(hashCode uint32) uint32 {
    if locks == nil {
        panic("dict is nil")
    }
    tableSize := uint32(len(locks.table))
    return (tableSize - 1) & uint32(hashCode)
}

func (locks *Locks)Lock(key string) {
    index := locks.spread(fnv32(key))
    mu := locks.table[index]
    mu.Lock()
}

func (locks *Locks)RLock(key string) {
    index := locks.spread(fnv32(key))
    mu := locks.table[index]
    mu.RLock()
}

func (locks *Locks)UnLock(key string) {
    index := locks.spread(fnv32(key))
    mu := locks.table[index]
    mu.Unlock()
}

func (locks *Locks)RUnLock(key string) {
    index := locks.spread(fnv32(key))
    mu := locks.table[index]
    mu.RUnlock()
}

func (locks *Locks)Locks(keys ...string) {
    keySlice := make(sort.StringSlice, len(keys))
    copy(keySlice, keys)
    sort.Sort(keySlice)
    for _, key := range keySlice {
        locks.Lock(key)
    }
}

func (locks *Locks)RLocks(keys ...string) {
    keySlice := make(sort.StringSlice, len(keys))
    copy(keySlice, keys)
    sort.Sort(keySlice)
    for _, key := range keySlice {
        locks.RLock(key)
    }
}


func (locks *Locks)UnLocks(keys ...string) {
    size := len(keys)
    keySlice := make(sort.StringSlice, size)
    copy(keySlice, keys)
    sort.Sort(keySlice)
    for i := size - 1; i >= 0; i-- {
        key := keySlice[i]
        locks.UnLock(key)
    }
}

func (locks *Locks)RUnLocks(keys ...string) {
    size := len(keys)
    keySlice := make(sort.StringSlice, size)
    copy(keySlice, keys)
    sort.Sort(keySlice)
    for i := size - 1; i >= 0; i-- {
        key := keySlice[i]
        locks.RUnLock(key)
    }
}

func GoID() int {
    var buf [64]byte
    n := runtime.Stack(buf[:], false)
    idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
    id, err := strconv.Atoi(idField)
    if err != nil {
        panic(fmt.Sprintf("cannot get goroutine id: %v", err))
    }
    return id
}

func debug(testing.T) {
    lm := Locks{}
    size := 10
    var wg sync.WaitGroup
    wg.Add(size)
    for i := 0; i < size; i++ {
        go func(i int) {
            lm.Locks("1", "2")
            println("go: " + strconv.Itoa(GoID()))
            time.Sleep(time.Second)
            println("go: " + strconv.Itoa(GoID()))
            lm.UnLocks("1", "2")
            wg.Done()
        }(i)
    }
    wg.Wait()
}