package set

import (
	"github.com/hdt3213/godis/lib/utils"
	"strconv"
	"testing"
)

func TestSet(t *testing.T) {
	size := 10
	set := Make()
	for i := 0; i < size; i++ {
		set.Add(strconv.Itoa(i))
	}
	for i := 0; i < size; i++ {
		ok := set.Has(strconv.Itoa(i))
		if !ok {
			t.Error("expected true actual false, key: " + strconv.Itoa(i))
		}
	}
	for i := 0; i < size; i++ {
		ok := set.Remove(strconv.Itoa(i))
		if ok != 1 {
			t.Error("expected true actual false, key: " + strconv.Itoa(i))
		}
	}
	for i := 0; i < size; i++ {
		ok := set.Has(strconv.Itoa(i))
		if ok {
			t.Error("expected false actual true, key: " + strconv.Itoa(i))
		}
	}
}

func TestSetScan(t *testing.T) {
	set := Make()
	size := 10
	for i := 0; i < size; i++ {
		str := "a" + utils.RandString(5)
		set.Add(str)
	}
	keys, nextCursor := set.SetScan(0, size, "*")
	if len(keys) != size {
		t.Errorf("expect %d keys, actual: %d", size, len(keys))
		return
	}
	if nextCursor != 0 {
		t.Errorf("expect 0, actual: %d", nextCursor)
		return
	}
	for i := 0; i < size; i++ {
		str := "b" + utils.RandString(5)
		set.Add(str)
	}
	keys, _ = set.SetScan(0, size*2, "a*")
	if len(keys) != size {
		t.Errorf("expect %d keys, actual: %d", size, len(keys))
		return
	}
}
