package dict

import (
    "strconv"
    "sync"
    "testing"
    "time"
)

func TestPut(t *testing.T) {
    d := Make(0)
    count := 100
    var wg sync.WaitGroup
    wg.Add(count)
    for i := 0; i < count; i++ {
        go func(i int) {
            // insert
            key := "k" + strconv.Itoa(i)
            ret := d.Put(key, i)
            if ret != 1 { // insert 1
                t.Error("put test failed: expected result 1, actual: " + strconv.Itoa(ret) + ", key: " + key)
            }
            val, ok := d.Get(key)
            if ok {
                intVal, _ := val.(int)
                if intVal != i {
                    t.Error("put test failed: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal) + ", key: " + key)
                }
            } else {
                time.Sleep(2 * time.Second)
                val2, ok2 := d.Get(key)
                intVal2, _ := val2.(int)
                t.Error("put test failed: expected true, actual: false, key: " + key +
                    ", retry: " + strconv.FormatBool(ok2) + ", val: " + strconv.Itoa(intVal2))
            }
        }(i)
    }
}

func TestPutIfAbsent(t *testing.T) {
    d := Make(0)
    count := 100
    var wg sync.WaitGroup
    wg.Add(count)
    for i := 0; i < count; i++ {
        go func(i int) {
            // insert
            key := "k" + strconv.Itoa(i)
            ret := d.PutIfAbsent(key, i)
            if ret != 1 { // insert 1
                t.Error("put test failed: expected result 1, actual: " + strconv.Itoa(ret) + ", key: " + key)
            }
            val, ok := d.Get(key)
            if ok {
                intVal, _ := val.(int)
                if intVal != i {
                    t.Error("put test failed: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal) +
                        ", key: " + key)
                }
            } else {
                t.Error("put test failed: expected true, actual: false, key: " + key)
            }

            // update
            ret = d.PutIfAbsent(key, i * 10)
            if ret != 0 { // no update
                t.Error("put test failed: expected result 0, actual: " + strconv.Itoa(ret))
            }
            val, ok = d.Get(key)
            if ok {
                intVal, _ := val.(int)
                if intVal != i {
                    t.Error("put test failed: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal) + ", key: " + key)
                }
            } else {
                t.Error("put test failed: expected true, actual: false, key: " + key)
            }
        }(i)
    }
}

func TestPutIfExists(t *testing.T) {
    d := Make(0)
    count := 100
    var wg sync.WaitGroup
    wg.Add(count)
    for i := 0; i < count; i++ {
        go func(i int) {
            // insert
            key := "k" + strconv.Itoa(i)
            // insert
            ret := d.PutIfExists(key, i)
            if ret != 0 { // insert
                t.Error("put test failed: expected result 0, actual: " + strconv.Itoa(ret))
            }

            d.Put(key, i)
            ret = d.PutIfExists(key, 10 * i)
            val, ok := d.Get(key)
            if ok {
                intVal, _ := val.(int)
                if intVal != 10 * i {
                    t.Error("put test failed: expected " + strconv.Itoa(10 * i) + ", actual: " + strconv.Itoa(intVal))
                }
            } else {
                t.Error("put test failed: expected true, actual: false")
            }
        }(i)
    }
}

func TestRemove(t *testing.T) {
    d := Make(0)

    // remove head node
    for i := 0; i < 100; i++ {
        // insert
        key := "k" + strconv.Itoa(i)
        d.Put(key, i)
    }
    for i := 0; i < 100; i++ {
        key := "k" + strconv.Itoa(i)

        val, ok := d.Get(key)
        if ok {
            intVal, _ := val.(int)
            if intVal != i {
                t.Error("put test failed: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal))
            }
        } else {
            t.Error("put test failed: expected true, actual: false")
        }

        ret := d.Remove(key)
        if ret != 1 {
            t.Error("remove test failed: expected result 1, actual: " + strconv.Itoa(ret) + ", key:" + key)
        }
        _, ok = d.Get(key)
        if ok {
            t.Error("remove test failed: expected true, actual false")
        }
        ret = d.Remove(key)
        if ret != 0 {
            t.Error("remove test failed: expected result 0 actual: " + strconv.Itoa(ret))
        }
    }

    // remove tail node
    d = Make(0)
    for i := 0; i < 100; i++ {
        // insert
        key := "k" + strconv.Itoa(i)
        d.Put(key, i)
    }
    for i := 9; i >= 0; i-- {
        key := "k" + strconv.Itoa(i)

        val, ok := d.Get(key)
        if ok {
            intVal, _ := val.(int)
            if intVal != i {
                t.Error("put test failed: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal))
            }
        } else {
            t.Error("put test failed: expected true, actual: false")
        }

        ret := d.Remove(key)
        if ret != 1 {
            t.Error("remove test failed: expected result 1, actual: " + strconv.Itoa(ret))
        }
        _, ok = d.Get(key)
        if ok {
            t.Error("remove test failed: expected true, actual false")
        }
        ret = d.Remove(key)
        if ret != 0 {
            t.Error("remove test failed: expected result 0 actual: " + strconv.Itoa(ret))
        }
    }

    // remove middle node
    d = Make(0)
    d.Put("head", 0)
    for i := 0; i < 10; i++ {
        // insert
        key := "k" + strconv.Itoa(i)
        d.Put(key, i)
    }
    d.Put("tail", 0)
    for i := 9; i >= 0; i-- {
        key := "k" + strconv.Itoa(i)

        val, ok := d.Get(key)
        if ok {
            intVal, _ := val.(int)
            if intVal != i {
                t.Error("put test failed: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal))
            }
        } else {
            t.Error("put test failed: expected true, actual: false")
        }

        ret := d.Remove(key)
        if ret != 1 {
            t.Error("remove test failed: expected result 1, actual: " + strconv.Itoa(ret))
        }
        _, ok = d.Get(key)
        if ok {
            t.Error("remove test failed: expected true, actual false")
        }
        ret = d.Remove(key)
        if ret != 0 {
            t.Error("remove test failed: expected result 0 actual: " + strconv.Itoa(ret))
        }
    }
}

func TestForEach(t *testing.T) {
    d := Make(0)
    size := 100
    for i := 0; i < size; i++ {
        // insert
        key := "k" + strconv.Itoa(i)
        d.Put(key, i)
    }
    i := 0
    d.ForEach(func(key string, value interface{})bool {
        intVal, _ := value.(int)
        expectedKey := "k" + strconv.Itoa(intVal)
        if key != expectedKey {
            t.Error("remove test failed: expected " + expectedKey + ", actual: " + key)
        }
        i++
        return true
    })
    if i != size {
        t.Error("remove test failed: expected " + strconv.Itoa(size) + ", actual: " + strconv.Itoa(i))
    }
}