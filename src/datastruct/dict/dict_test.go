package dict

import (
    "testing"
    "strconv"
)

func TestPut(t *testing.T) {
    d := Make(0)

    // insert
    ret := d.Put("a", 1)
    if ret != 1 { // insert 1
        t.Error("put test failed: expected result 1, actual: " + strconv.Itoa(ret))
    }
    val, ok := d.Get("a")
    if ok {
        intVal, _ := val.(int)
        if intVal != 1 {
            t.Error("put test failed: expected 2, actual: " + strconv.Itoa(intVal))
        }
    } else {
        t.Error("put test failed: expected true, actual: false")
    }

    // update
    ret = d.Put("a", 2)
    if ret != 0 { // no insert
        t.Error("put test failed: expected result 0, actual: " + strconv.Itoa(ret))
    }
    val, ok = d.Get("a")
    if ok {
        intVal, _ := val.(int)
        if intVal != 2 {
            t.Error("put test failed: expected 2, actual: " + strconv.Itoa(intVal))
        }
    } else {
        t.Error("put test failed: expected true, actual: false")
    }
}

func TestPutIfAbsent(t *testing.T) {
    d := Make(0)

    // insert
    ret := d.PutIfAbsent("a", 1)
    if ret != 1 { // insert 1
        t.Error("put test failed: expected result 1, actual: " + strconv.Itoa(ret))
    }
    val, ok := d.Get("a")
    if ok {
        intVal, _ := val.(int)
        if intVal != 1 {
            t.Error("put test failed: expected 2, actual: " + strconv.Itoa(intVal))
        }
    } else {
        t.Error("put test failed: expected true, actual: false")
    }

    // update
    ret = d.PutIfAbsent("a", 2)
    if ret != 0 { // no update
        t.Error("put test failed: expected result 0, actual: " + strconv.Itoa(ret))
    }
    val, ok = d.Get("a")
    if ok {
        intVal, _ := val.(int)
        if intVal != 1 {
            t.Error("put test failed: expected 2, actual: " + strconv.Itoa(intVal))
        }
    } else {
        t.Error("put test failed: expected true, actual: false")
    }
}

func TestPutIfExists(t *testing.T) {
    d := Make(0)

    // insert
    ret := d.PutIfExists("a", 1)
    if ret != 0 { // insert
        t.Error("put test failed: expected result 0, actual: " + strconv.Itoa(ret))
    }

    d.Put("a", 1)
    ret = d.PutIfExists("a", 2)
    val, ok := d.Get("a")
    if ok {
        intVal, _ := val.(int)
        if intVal != 2 {
            t.Error("put test failed: expected 2, actual: " + strconv.Itoa(intVal))
        }
    } else {
        t.Error("put test failed: expected true, actual: false")
    }
}

func TestRemove(t *testing.T) {
    d := Make(0)

    // insert
    d.Put("a", 1)
    ret := d.Remove("a")
    if ret != 1 {
        t.Error("remove test failed: expected result 1, actual: " + strconv.Itoa(ret))
    }
    _, ok := d.Get("a")
    if ok {
        t.Error("remove test failed: expected true, actual false")
    }
    ret = d.Remove("a")
    if ret != 0 {
        t.Error("remove test failed: expected result 0 actual: " + strconv.Itoa(ret))
    }
}