package set

import (
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
