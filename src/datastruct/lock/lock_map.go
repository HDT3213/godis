package lock

import (
    "fmt"
    "runtime"
    "sort"
    "strconv"
    "strings"
    "sync"
    "time"
)

type LockMap struct {
    m sync.Map // key -> mutex
}

func (lock *LockMap)Lock(key string) {
    mu := &sync.RWMutex{}
    existed, loaded := lock.m.LoadOrStore(key, mu)
    if loaded {
        mu, _ = existed.(*sync.RWMutex)
    }
    mu.Lock()
}

func (lock *LockMap)RLock(key string) {
    mu := &sync.RWMutex{}
    existed, loaded := lock.m.LoadOrStore(key, mu)
    if loaded {
        mu, _ = existed.(*sync.RWMutex)
    }
    mu.RLock()
}

func (lock *LockMap)UnLock(key string) {
    value, ok := lock.m.Load(key)
    if !ok {
        return
    }
    mu := value.(*sync.RWMutex)
    mu.Unlock()
}

func (lock *LockMap)RUnLock(key string) {
    value, ok := lock.m.Load(key)
    if !ok {
        return
    }
    mu := value.(*sync.RWMutex)
    mu.RUnlock()
}

func (lock *LockMap)Locks(keys ...string) {
    keySlice := make(sort.StringSlice, len(keys))
    copy(keySlice, keys)
    sort.Sort(keySlice)
    for _, key := range keySlice {
        lock.Lock(key)
    }
}

func (lock *LockMap)RLocks(keys ...string) {
    keySlice := make(sort.StringSlice, len(keys))
    copy(keySlice, keys)
    sort.Sort(keySlice)
    for _, key := range keySlice {
        lock.RLock(key)
    }
}


func (lock *LockMap)UnLocks(keys ...string) {
    size := len(keys)
    keySlice := make(sort.StringSlice, size)
    copy(keySlice, keys)
    sort.Sort(keySlice)
    for i := size - 1; i >= 0; i-- {
        key := keySlice[i]
        lock.UnLock(key)
    }
}

func (lock *LockMap)RUnLocks(keys ...string) {
    size := len(keys)
    keySlice := make(sort.StringSlice, size)
    copy(keySlice, keys)
    sort.Sort(keySlice)
    for i := size - 1; i >= 0; i-- {
        key := keySlice[i]
        lock.RUnLock(key)
    }
}

func (lock *LockMap)Clean(key string) {
    lock.m.Delete(key)
}

func (lock *LockMap)Cleans(keys ...string) {
    for _, key := range keys {
        lock.Clean(key)
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

func debug() {
    lm := LockMap{}
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