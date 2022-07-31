package list

import (
	"github.com/hdt3213/godis/lib/utils"
	"strconv"
	"testing"
)

func TestQuickList_Add(t *testing.T) {
	list := NewQuickList()
	for i := 0; i < pageSize*10; i++ {
		list.Add(i)
	}
	for i := 0; i < pageSize*10; i++ {
		v := list.Get(i).(int)
		if v != i {
			t.Errorf("wrong value at: %d", i)
		}
	}
	list.ForEach(func(i int, v interface{}) bool {
		if v != i {
			t.Errorf("wrong value at: %d", i)
		}
		return true
	})
}

func BenchmarkQuickList_Add(b *testing.B) {
	list := NewQuickList()
	for i := 0; i < pageSize*10; i++ {
		list.Add(i)
	}
}

func BenchmarkQuickList_Range(b *testing.B) {
	list := NewQuickList()
	for i := 0; i < pageSize*10; i++ {
		list.Add(i)
	}
	list.ForEach(func(i int, v interface{}) bool {
		return true
	})
}

func TestQuickList_Set(t *testing.T) {
	list := NewQuickList()
	for i := 0; i < pageSize*10; i++ {
		list.Add(i)
	}
	for i := 0; i < pageSize*10; i++ {
		list.Set(i, 2*i)
	}
	for i := 0; i < pageSize*10; i++ {
		v := list.Get(i).(int)
		if v != 2*i {
			t.Errorf("wrong value at: %d", i)
		}
	}
}

func TestQuickList_Insert(t *testing.T) {
	list := NewQuickList()
	for i := 0; i < pageSize*10; i++ {
		list.Insert(0, i)
	}
	for i := 0; i < pageSize*10; i++ {
		v := list.Get(i).(int)
		if v != pageSize*10-i-1 {
			t.Errorf("wrong value at: %d", i)
		}
	}

	// insert into second half page
	list = NewQuickList()
	for i := 0; i < pageSize; i++ {
		list.Add(0)
	}
	for i := 0; i < pageSize; i++ {
		list.Insert(pageSize-1, i+1)
	}
	for i := pageSize - 1; i < list.size; i++ {
		v := list.Get(i).(int)
		if v != 2*pageSize-1-i {
			t.Errorf("wrong value at: %d", i)
		}
	}
}

func TestQuickList_RemoveLast(t *testing.T) {
	list := NewQuickList()
	size := pageSize * 10
	for i := 0; i < size; i++ {
		list.Add(i)
	}
	for i := 0; i < size; i++ {
		v := list.RemoveLast()
		if v != size-i-1 {
			t.Errorf("wrong value at: %d", i)
		}
		if list.Len() != size-i-1 {
			t.Errorf("wrong len: %d", list.Len())
		}
	}

}

func TestQuickListRemoveVal(t *testing.T) {
	list := NewQuickList()
	size := pageSize * 10
	for i := 0; i < size; i++ {
		list.Add(i)
		list.Add(i)
	}
	for index := 0; index < list.Len(); index++ {
		list.RemoveAllByVal(func(a interface{}) bool {
			return utils.Equals(a, index)
		})
		list.ForEach(func(i int, v interface{}) bool {
			intVal, _ := v.(int)
			if intVal == index {
				t.Error("remove test fail: found  " + strconv.Itoa(index) + " at index: " + strconv.Itoa(i))
			}
			return true
		})
	}

	list = NewQuickList()
	for i := 0; i < size; i++ {
		list.Add(i)
		list.Add(i)
	}
	for i := 0; i < size; i++ {
		list.RemoveByVal(func(a interface{}) bool {
			return utils.Equals(a, i)
		}, 1)
	}
	list.ForEach(func(i int, v interface{}) bool {
		intVal, _ := v.(int)
		if intVal != i {
			t.Error("test fail: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal))
		}
		return true
	})
	for i := 0; i < size; i++ {
		list.RemoveByVal(func(a interface{}) bool {
			return utils.Equals(a, i)
		}, 1)
	}
	if list.Len() != 0 {
		t.Error("test fail: expected 0, actual: " + strconv.Itoa(list.Len()))
	}

	list = NewQuickList()
	for i := 0; i < size; i++ {
		list.Add(i)
		list.Add(i)
	}
	for i := 0; i < size; i++ {
		list.ReverseRemoveByVal(func(a interface{}) bool {
			return utils.Equals(a, i)
		}, 1)
	}
	list.ForEach(func(i int, v interface{}) bool {
		intVal, _ := v.(int)
		if intVal != i {
			t.Error("test fail: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal))
		}
		return true
	})
	for i := 0; i < size; i++ {
		list.ReverseRemoveByVal(func(a interface{}) bool {
			return utils.Equals(a, i)
		}, 1)
	}
	if list.Len() != 0 {
		t.Error("test fail: expected 0, actual: " + strconv.Itoa(list.Len()))
	}
}

func TestQuickList_Contains(t *testing.T) {
	list := NewQuickList()
	list.Add(1)
	list.Add(2)
	list.Add(3)
	if !list.Contains(func(a interface{}) bool {
		return a == 1
	}) {
		t.Error("expect true actual false")
	}
	if list.Contains(func(a interface{}) bool {
		return a == -1
	}) {
		t.Error("expect false actual true")
	}
}

func TestQuickList_Range(t *testing.T) {
	list := NewQuickList()
	size := 10
	for i := 0; i < size; i++ {
		list.Add(i)
	}
	for start := 0; start < size; start++ {
		for stop := start; stop < size; stop++ {
			slice := list.Range(start, stop)
			if len(slice) != stop-start {
				t.Error("expected " + strconv.Itoa(stop-start) + ", get: " + strconv.Itoa(len(slice)) +
					", range: [" + strconv.Itoa(start) + "," + strconv.Itoa(stop) + "]")
			}
			sliceIndex := 0
			for i := start; i < stop; i++ {
				val := slice[sliceIndex]
				intVal, _ := val.(int)
				if intVal != i {
					t.Error("expected " + strconv.Itoa(i) + ", get: " + strconv.Itoa(intVal) +
						", range: [" + strconv.Itoa(start) + "," + strconv.Itoa(stop) + "]")
				}
				sliceIndex++
			}
		}
	}
}

func TestQuickList_Remove(t *testing.T) {
	list := NewQuickList()
	size := pageSize * 10
	for i := 0; i < size; i++ {
		list.Add(i)
	}
	for i := size - 1; i >= 0; i-- {
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

func TestQuickList_Prev(t *testing.T) {
	list := NewQuickList()
	size := pageSize * 10
	for i := 0; i < size; i++ {
		list.Add(i)
	}
	iter := list.find(size - 1)
	i := size - 1
	for !iter.atBegin() {
		v := iter.get()
		if v != i {
			t.Errorf("wrong value at %d", i)
		}
		iter.prev()
		i--
	}
}
