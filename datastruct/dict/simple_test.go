package dict

import (
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/lib/wildcard"
	"sort"
	"testing"
)

func TestSimpleDict_Keys(t *testing.T) {
	d := MakeSimple()
	size := 10
	var expectKeys []string
	for i := 0; i < size; i++ {
		str := utils.RandString(5)
		d.Put(str, str)
		expectKeys = append(expectKeys, str)
	}
	sort.Slice(expectKeys, func(i, j int) bool {
		return expectKeys[i] > expectKeys[j]
	})
	keys := d.Keys()
	if len(keys) != size {
		t.Errorf("expect %d keys, actual: %d", size, len(d.Keys()))
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] > keys[j]
	})
	for i, k := range keys {
		if k != expectKeys[i] {
			t.Errorf("expect %s actual %s", expectKeys[i], k)
		}
	}
}

func TestSimpleDict_PutIfExists(t *testing.T) {
	d := MakeSimple()
	key := utils.RandString(5)
	val := key + "1"
	ret := d.PutIfExists(key, val)
	if ret != 0 {
		t.Error("expect 0")
		return
	}
	d.Put(key, val)
	val = key + "2"
	ret = d.PutIfExists(key, val)
	if ret != 1 {
		t.Error("expect 1")
		return
	}
	if v, _ := d.Get(key); v != val {
		t.Error("wrong value")
		return
	}
}

func TestSimpleDict_ScanKeys(t *testing.T) {
	count := 1
	d := MakeSimple()
	for i := 0; i < 5; i++ {
		value := utils.RandString(5)
		key := "k" + value
		ret := d.PutIfExists(key, value)
		if ret != 0 {
			t.Error("expect 0")
			return
		}
	}
	result, _ := d.ScanKeys(0, count, "k*")

	pattern, err := wildcard.CompilePattern("k*")
	if err != nil {
		t.Error(err)
		return
	}

	for _, s := range result {
		if !pattern.IsMatch(s) {
			t.Error("Scan command execution error")
		}
	}

}
