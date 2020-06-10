package consistenthash

import (
    "hash/crc32"
    "sort"
    "strconv"
)

type HashFunc func(data []byte) uint32

type Map struct {
    hashFunc HashFunc
    replicas int
    keys     []int // sorted
    hashMap  map[int]string
}

func New(replicas int, fn HashFunc) *Map {
    m := &Map{
        replicas: replicas,
        hashFunc: fn,
        hashMap:  make(map[int]string),
    }
    if m.hashFunc == nil {
        m.hashFunc = crc32.ChecksumIEEE
    }
    return m
}

func (m *Map) IsEmpty() bool {
    return len(m.keys) == 0
}

func (m *Map) Add(keys ...string) {
    for _, key := range keys {
        for i := 0; i < m.replicas; i++ {
            hash := int(m.hashFunc([]byte(strconv.Itoa(i) + key)))
            m.keys = append(m.keys, hash)
            m.hashMap[hash] = key
        }
    }
    sort.Ints(m.keys)
}

// Get gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string {
    if m.IsEmpty() {
        return ""
    }

    hash := int(m.hashFunc([]byte(key)))

    // Binary search for appropriate replica.
    idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= hash })

    // Means we have cycled back to the first replica.
    if idx == len(m.keys) {
        idx = 0
    }

    return m.hashMap[m.keys[idx]]
}
