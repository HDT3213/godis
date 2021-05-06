package dict

import (
	"github.com/hdt3213/godis/lib/utils"
	"testing"
)

func TestSimpleDict_Keys(t *testing.T) {
	d := MakeSimple()
	size := 10
	for i := 0; i < size; i++ {
		d.Put(utils.RandString(5), utils.RandString(5))
	}
	if len(d.Keys()) != size {
		t.Errorf("expect %d keys, actual: %d", size, len(d.Keys()))
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
