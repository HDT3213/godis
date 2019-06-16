package list

import (
    "testing"
    "strconv"
    "strings"
)

func ToString(list *LinkedList) string {
    arr := make([]string, list.size)
    list.ForEach(func(i int, v interface{}) bool {
        integer, _ := v.(int)
        arr[i] = strconv.Itoa(integer)
        return true
    })
    return "[" + strings.Join(arr, ", ") + "]"
}

func TestAdd(t *testing.T) {
    list := Make()
    for i := 0; i < 10; i++ {
        list.Add(i)
    }
    list.ForEach(func(i int, v interface{}) bool {
        intVal, _ := v.(int)
        if intVal != i {
            t.Error("add test fail: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal))
        }
        return true
    })
}

func TestGet(t *testing.T) {
    list := Make()
    for i := 0; i < 10; i++ {
        list.Add(i)
    }
    for i := 0; i < 10; i++ {
        v := list.Get(i)
        k, _ := v.(int)
        if i != k {
            t.Error("get test fail: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(k))
        }
    }
}

func TestRemove(t *testing.T) {
    list := Make()
    for i := 0; i < 10; i++ {
        list.Add(i)
    }
    for i := 9; i >= 0; i-- {
        list.Remove(i)
        if i != list.Len() {
            t.Error("remove test fail: expected size " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(list.Len()))
        }
        list.ForEach(func(i int, v interface{}) bool {
            intVal, _ := v.(int)
            if intVal != i {
                t.Error("remove test fail: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal))
            }
            return true
        })
    }
}

func TestInsert(t *testing.T) {
    list := Make()
    for i := 0; i < 10; i++ {
        list.Add(i)
    }
    for i := 0; i < 10; i++ {
        list.Insert(i*2, i)

        list.ForEach(func(j int, v interface{}) bool {
            var expected int
            if j < (i + 1) * 2 {
                if j%2 == 0 {
                    expected = j / 2
                } else {
                    expected = (j - 1) / 2
                }
            } else {
                expected = j - i - 1
            }
            actual, _ := list.Get(j).(int)
            if actual != expected {
                t.Error("insert test fail: at i " + strconv.Itoa(i) + " expected " + strconv.Itoa(expected) + ", actual: " + strconv.Itoa(actual))
            }
            return true
        })

        for j := 0; j < list.Len(); j++ {
            var expected int
            if j < (i + 1) * 2 {
                if j%2 == 0 {
                    expected = j / 2
                } else {
                    expected = (j - 1) / 2
                }
            } else {
                expected = j - i - 1
            }
            actual, _ := list.Get(j).(int)
            if actual != expected {
                t.Error("insert test fail: at i " + strconv.Itoa(i) + " expected " + strconv.Itoa(expected) + ", actual: " + strconv.Itoa(actual))
            }
        }

    }
}
