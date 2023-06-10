package dict

import (
	"github.com/hdt3213/godis/lib/utils"
	"strconv"
	"sync"
	"testing"
)

func TestConcurrentPut(t *testing.T) {
	d := MakeConcurrent(0)
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
				_, ok := d.Get(key)
				t.Error("put test failed: expected true, actual: false, key: " + key + ", retry: " + strconv.FormatBool(ok))
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestConcurrentPutWithLock(t *testing.T) {
	d := MakeConcurrent(0)
	count := 100
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		go func(i int) {
			// insert
			key := "k" + strconv.Itoa(i)
			keys := []string{key}
			d.RWLocks(keys, nil)
			ret := d.PutWithLock(key, i)
			if ret != 1 { // insert 1
				t.Error("put test failed: expected result 1, actual: " + strconv.Itoa(ret) + ", key: " + key)
			}
			val, ok := d.GetWithLock(key)
			if ok {
				intVal, _ := val.(int)
				if intVal != i {
					t.Error("put test failed: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal) + ", key: " + key)
				}
			} else {
				_, ok := d.GetWithLock(key)
				t.Error("put test failed: expected true, actual: false, key: " + key + ", retry: " + strconv.FormatBool(ok))
			}
			wg.Done()
			d.RWUnLocks(keys, nil)
		}(i)
	}
	wg.Wait()
}

func TestConcurrentPutIfAbsent(t *testing.T) {
	d := MakeConcurrent(0)
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
				_, ok := d.Get(key)
				t.Error("put test failed: expected true, actual: false, key: " + key + ", retry: " + strconv.FormatBool(ok))
			}

			// update
			ret = d.PutIfAbsent(key, i*10)
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
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestConcurrentPutIfAbsentWithLock(t *testing.T) {
	d := MakeConcurrent(0)
	count := 100
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		go func(i int) {
			// insert
			key := "k" + strconv.Itoa(i)
			keys := []string{key}
			d.RWLocks(keys, nil)
			ret := d.PutIfAbsentWithLock(key, i)
			if ret != 1 { // insert 1
				t.Error("put test failed: expected result 1, actual: " + strconv.Itoa(ret) + ", key: " + key)
			}
			val, ok := d.GetWithLock(key)
			if ok {
				intVal, _ := val.(int)
				if intVal != i {
					t.Error("put test failed: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal) +
						", key: " + key)
				}
			} else {
				_, ok := d.GetWithLock(key)
				t.Error("put test failed: expected true, actual: false, key: " + key + ", retry: " + strconv.FormatBool(ok))
			}

			// update
			ret = d.PutIfAbsentWithLock(key, i*10)
			if ret != 0 { // no update
				t.Error("put test failed: expected result 0, actual: " + strconv.Itoa(ret))
			}
			val, ok = d.GetWithLock(key)
			if ok {
				intVal, _ := val.(int)
				if intVal != i {
					t.Error("put test failed: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal) + ", key: " + key)
				}
			} else {
				t.Error("put test failed: expected true, actual: false, key: " + key)
			}
			d.RWUnLocks(keys, nil)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestConcurrentPutIfExists(t *testing.T) {
	d := MakeConcurrent(0)
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
			d.PutIfExists(key, 10*i)
			val, ok := d.Get(key)
			if ok {
				intVal, _ := val.(int)
				if intVal != 10*i {
					t.Error("put test failed: expected " + strconv.Itoa(10*i) + ", actual: " + strconv.Itoa(intVal))
				}
			} else {
				_, ok := d.Get(key)
				t.Error("put test failed: expected true, actual: false, key: " + key + ", retry: " + strconv.FormatBool(ok))
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestConcurrentPutIfExistsWithLock(t *testing.T) {
	d := MakeConcurrent(0)
	count := 100
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		go func(i int) {
			// insert
			key := "k" + strconv.Itoa(i)
			keys := []string{key}
			d.RWLocks(keys, nil)
			// insert
			ret := d.PutIfExistsWithLock(key, i)
			if ret != 0 { // insert
				t.Error("put test failed: expected result 0, actual: " + strconv.Itoa(ret))
			}
			d.PutWithLock(key, i)
			d.PutIfExistsWithLock(key, 10*i)
			val, ok := d.GetWithLock(key)
			if ok {
				intVal, _ := val.(int)
				if intVal != 10*i {
					t.Error("put test failed: expected " + strconv.Itoa(10*i) + ", actual: " + strconv.Itoa(intVal))
				}
			} else {
				_, ok := d.GetWithLock(key)
				t.Error("put test failed: expected true, actual: false, key: " + key + ", retry: " + strconv.FormatBool(ok))
			}
			d.RWUnLocks(keys, nil)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestConcurrentRemove(t *testing.T) {
	d := MakeConcurrent(0)
	totalCount := 100
	// remove head node
	for i := 0; i < totalCount; i++ {
		// insert
		key := "k" + strconv.Itoa(i)
		d.Put(key, i)
	}
	if d.Len() != totalCount {
		t.Error("put test failed: expected len is 100, actual: " + strconv.Itoa(d.Len()))
	}
	for i := 0; i < totalCount; i++ {
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

		_, ret := d.Remove(key)
		if ret != 1 {
			t.Error("remove test failed: expected result 1, actual: " + strconv.Itoa(ret) + ", key:" + key)
		}
		if d.Len() != totalCount-i-1 {
			t.Error("put test failed: expected len is 99, actual: " + strconv.Itoa(d.Len()))
		}
		_, ok = d.Get(key)
		if ok {
			t.Error("remove test failed: expected true, actual false")
		}
		_, ret = d.Remove(key)
		if ret != 0 {
			t.Error("remove test failed: expected result 0 actual: " + strconv.Itoa(ret))
		}
		if d.Len() != totalCount-i-1 {
			t.Error("put test failed: expected len is 99, actual: " + strconv.Itoa(d.Len()))
		}
	}

	// remove tail node
	d = MakeConcurrent(0)
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

		_, ret := d.Remove(key)
		if ret != 1 {
			t.Error("remove test failed: expected result 1, actual: " + strconv.Itoa(ret))
		}
		_, ok = d.Get(key)
		if ok {
			t.Error("remove test failed: expected true, actual false")
		}
		_, ret = d.Remove(key)
		if ret != 0 {
			t.Error("remove test failed: expected result 0 actual: " + strconv.Itoa(ret))
		}
	}

	// remove middle node
	d = MakeConcurrent(0)
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

		_, ret := d.Remove(key)
		if ret != 1 {
			t.Error("remove test failed: expected result 1, actual: " + strconv.Itoa(ret))
		}
		_, ok = d.Get(key)
		if ok {
			t.Error("remove test failed: expected true, actual false")
		}
		_, ret = d.Remove(key)
		if ret != 0 {
			t.Error("remove test failed: expected result 0 actual: " + strconv.Itoa(ret))
		}
	}
}

func TestConcurrentRemoveWithLock(t *testing.T) {
	d := MakeConcurrent(0)
	totalCount := 100
	// remove head node
	for i := 0; i < totalCount; i++ {
		// insert
		key := "k" + strconv.Itoa(i)
		d.PutWithLock(key, i)
	}
	if d.Len() != totalCount {
		t.Error("put test failed: expected len is 100, actual: " + strconv.Itoa(d.Len()))
	}
	for i := 0; i < totalCount; i++ {
		key := "k" + strconv.Itoa(i)

		val, ok := d.GetWithLock(key)
		if ok {
			intVal, _ := val.(int)
			if intVal != i {
				t.Error("put test failed: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal))
			}
		} else {
			t.Error("put test failed: expected true, actual: false")
		}

		_, ret := d.RemoveWithLock(key)
		if ret != 1 {
			t.Error("remove test failed: expected result 1, actual: " + strconv.Itoa(ret) + ", key:" + key)
		}
		if d.Len() != totalCount-i-1 {
			t.Error("put test failed: expected len is 99, actual: " + strconv.Itoa(d.Len()))
		}
		_, ok = d.GetWithLock(key)
		if ok {
			t.Error("remove test failed: expected true, actual false")
		}
		_, ret = d.RemoveWithLock(key)
		if ret != 0 {
			t.Error("remove test failed: expected result 0 actual: " + strconv.Itoa(ret))
		}
		if d.Len() != totalCount-i-1 {
			t.Error("put test failed: expected len is 99, actual: " + strconv.Itoa(d.Len()))
		}
	}

	// remove tail node
	d = MakeConcurrent(0)
	for i := 0; i < 100; i++ {
		// insert
		key := "k" + strconv.Itoa(i)
		d.PutWithLock(key, i)
	}
	for i := 9; i >= 0; i-- {
		key := "k" + strconv.Itoa(i)

		val, ok := d.GetWithLock(key)
		if ok {
			intVal, _ := val.(int)
			if intVal != i {
				t.Error("put test failed: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal))
			}
		} else {
			t.Error("put test failed: expected true, actual: false")
		}

		_, ret := d.RemoveWithLock(key)
		if ret != 1 {
			t.Error("remove test failed: expected result 1, actual: " + strconv.Itoa(ret))
		}
		_, ok = d.GetWithLock(key)
		if ok {
			t.Error("remove test failed: expected true, actual false")
		}
		_, ret = d.RemoveWithLock(key)
		if ret != 0 {
			t.Error("remove test failed: expected result 0 actual: " + strconv.Itoa(ret))
		}
	}

	// remove middle node
	d = MakeConcurrent(0)
	d.Put("head", 0)
	for i := 0; i < 10; i++ {
		// insert
		key := "k" + strconv.Itoa(i)
		d.PutWithLock(key, i)
	}
	d.PutWithLock("tail", 0)
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

		_, ret := d.RemoveWithLock(key)
		if ret != 1 {
			t.Error("remove test failed: expected result 1, actual: " + strconv.Itoa(ret))
		}
		_, ok = d.GetWithLock(key)
		if ok {
			t.Error("remove test failed: expected true, actual false")
		}
		_, ret = d.RemoveWithLock(key)
		if ret != 0 {
			t.Error("remove test failed: expected result 0 actual: " + strconv.Itoa(ret))
		}
	}
}

//change t.Error remove->forEach
func TestConcurrentForEach(t *testing.T) {
	d := MakeConcurrent(0)
	size := 100
	for i := 0; i < size; i++ {
		// insert
		key := "k" + strconv.Itoa(i)
		d.Put(key, i)
	}
	i := 0
	d.ForEach(func(key string, value interface{}) bool {
		intVal, _ := value.(int)
		expectedKey := "k" + strconv.Itoa(intVal)
		if key != expectedKey {
			t.Error("forEach test failed: expected " + expectedKey + ", actual: " + key)
		}
		i++
		return true
	})
	if i != size {
		t.Error("forEach test failed: expected " + strconv.Itoa(size) + ", actual: " + strconv.Itoa(i))
	}
}

func TestConcurrentRandomKey(t *testing.T) {
	d := MakeConcurrent(0)
	count := 100
	for i := 0; i < count; i++ {
		key := "k" + strconv.Itoa(i)
		d.Put(key, i)
	}
	fetchSize := 10
	result := d.RandomKeys(fetchSize)
	if len(result) != fetchSize {
		t.Errorf("expect %d random keys acturally %d", fetchSize, len(result))
	}
	result = d.RandomDistinctKeys(fetchSize)
	distinct := make(map[string]struct{})
	for _, key := range result {
		distinct[key] = struct{}{}
	}
	if len(result) != fetchSize {
		t.Errorf("expect %d random keys acturally %d", fetchSize, len(result))
	}
	if len(result) > len(distinct) {
		t.Errorf("get duplicated keys in result")
	}
}

func TestConcurrentDict_Keys(t *testing.T) {
	d := MakeConcurrent(0)
	size := 10
	for i := 0; i < size; i++ {
		d.Put(utils.RandString(5), utils.RandString(5))
	}
	if len(d.Keys()) != size {
		t.Errorf("expect %d keys, actual: %d", size, len(d.Keys()))
	}
}
