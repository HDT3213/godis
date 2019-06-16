package list

type LinkedList struct {
    first *node
    last *node
    size int
}

type node struct {
    val  interface{}
    prev *node
    next * node
}

func (list *LinkedList)Add(val interface{}) {
    if list == nil {
        panic("list is nil")
    }
    n := &node{
        val: val,
    }
    if list.last == nil {
        // empty list
        list.first = n
        list.last = n
    } else {
        n.prev = list.last
        list.last.next = n
        list.last = n
    }
    list.size++
}

func (list *LinkedList)find(index int)(val *node) {
    if index < list.size / 2 {
        n := list.first
        for i := 0; i < index; i++ {
            n = n.next
        }
        return n
    } else {
        n := list.last
        for i := list.size - 1; i > index; i-- {
            n = n.prev
        }
        return n
    }
}

func (list *LinkedList)Get(index int)(val interface{}) {
    if list == nil {
        panic("list is nil")
    }
    if index < 0 || index >= list.size {
        panic("index out of bound")
    }
    return list.find(index).val
}

func (list *LinkedList)Insert(index int, val interface{}) {
    if list == nil {
        panic("list is nil")
    }
    if index < 0 || index >= list.size {
        panic("index out of bound")
    }

    if index == list.size {
        list.Add(val)
    } else {
        // list is not empty
        pivot := list.find(index)
        n := &node{
            val: val,
            prev: pivot.prev,
            next: pivot,
        }
        if pivot.prev == nil {
            list.first = n
        } else {
            pivot.prev.next = n
        }
        pivot.prev = n
    }
    list.size++
}

func (list *LinkedList)Remove(index int) {
    if list == nil {
        panic("list is nil")
    }
    if index < 0 || index >= list.size {
        panic("index out of bound")
    }

    n := list.find(index)
    if n.prev == nil {
        list.first = n.next
    } else {
        n.prev.next = n.next
    }
    if n.next == nil {
        list.last = n.prev
    } else {
        n.next.prev = n.prev
    }

    // for gc
    n.prev = nil
    n.next = nil
    n.val = nil

    list.size--
}

func (list *LinkedList)Len()int {
    if list == nil {
        panic("list is nil")
    }
    return list.size
}

func (list *LinkedList)ForEach(consumer func(int, interface{})bool) {
    if list == nil {
        panic("list is nil")
    }
    n := list.first
    i := 0
    for n != nil {
        goNext := consumer(i, n.val)
        if !goNext || n.next == nil {
            break
        } else {
            i++
            n = n.next
        }
    }
}

func Make(vals ...interface{})(*LinkedList) {
    list := LinkedList{}
    for _, v := range vals {
        list.Add(v)
    }
    return &list
}