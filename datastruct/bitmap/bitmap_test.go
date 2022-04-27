package bitmap

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestSetBit(t *testing.T) {
	// generate distinct rand offsets
	size := 1000
	offsets := make([]int64, size)
	for i := 0; i < size; i++ {
		offsets[i] = int64(i)
	}
	rand.Shuffle(size, func(i, j int) {
		offsets[i], offsets[j] = offsets[j], offsets[i]
	})
	offsets = offsets[0 : size/5]
	// set bit
	offsetMap := make(map[int64]struct{})
	bm := New()
	for _, offset := range offsets {
		offsetMap[offset] = struct{}{}
		bm.SetBit(offset, 1)
	}
	// get bit
	for i := 0; i < bm.BitSize(); i++ {
		offset := int64(i)
		_, expect := offsetMap[offset]
		actual := bm.GetBit(offset) > 0
		if expect != actual {
			t.Errorf("wrong value at %d", offset)
		}
	}

	bm2 := New()
	bm2.SetBit(15, 1)
	if bm2.GetBit(15) != 1 {
		t.Error("wrong value")
	}
	if bm2.GetBit(16) != 0 {
		t.Error("wrong value")
	}
}

func TestFromBytes(t *testing.T) {
	bs := []byte{0xff, 0xff}
	bm := FromBytes(bs)
	bm.SetBit(8, 0)
	expect := []byte{0xff, 0xfe}
	if !bytes.Equal(bs, expect) {
		t.Error("wrong value")
	}
	ret := bm.ToBytes()
	if !bytes.Equal(ret, expect) {
		t.Error("wrong value")
	}
}

func TestForEachBit(t *testing.T) {
	bm := New()
	for i := 0; i < 1000; i++ {
		if i%2 == 0 {
			bm.SetBit(int64(i), 1)
		}
	}
	expectOffset := int64(100)
	count := 0
	bm.ForEachBit(100, 200, func(offset int64, val byte) bool {
		if offset != expectOffset {
			t.Error("wrong offset")
		}
		expectOffset++
		if offset%2 == 0 && val == 0 {
			t.Error("expect 1")
		}
		count++
		return true
	})
	if count != 100 {
		t.Error("wrong count")
	}
	bm = New()
	size := 1000
	offsets := make([]int64, size)
	for i := 0; i < size; i++ {
		offsets[i] = int64(i)
	}
	rand.Shuffle(size, func(i, j int) {
		offsets[i], offsets[j] = offsets[j], offsets[i]
	})
	offsets = offsets[0 : size/5]
	offsetMap := make(map[int64]struct{})
	for _, offset := range offsets {
		bm.SetBit(offset, 1)
		offsetMap[offset] = struct{}{}
	}
	bm.ForEachBit(int64(size/20+1), 0, func(offset int64, val byte) bool {
		_, expect := offsetMap[offset]
		actual := bm.GetBit(offset) > 0
		if expect != actual {
			t.Errorf("wrong value at %d", offset)
		}
		return true
	})
	count = 0
	bm.ForEachBit(0, 0, func(offset int64, val byte) bool {
		count++
		return false
	})
	if count != 1 {
		t.Error("break failed")
	}
}

func TestBitMap_ForEachByte(t *testing.T) {
	bm := New()
	for i := 0; i < 1000; i++ {
		if i%16 == 0 {
			bm.SetBit(int64(i), 1)
		}
	}
	bm.ForEachByte(0, 0, func(offset int64, val byte) bool {
		if offset%2 == 0 {
			if val != 1 {
				t.Error("wrong value")
			}
		} else {
			if val != 0 {
				t.Error("wrong value")
			}
		}
		return true
	})
	bm.ForEachByte(0, 2000, func(offset int64, val byte) bool {
		if offset%2 == 0 {
			if val != 1 {
				t.Error("wrong value")
			}
		} else {
			if val != 0 {
				t.Error("wrong value")
			}
		}
		return true
	})
	bm.ForEachByte(0, 500, func(offset int64, val byte) bool {
		if offset%2 == 0 {
			if val != 1 {
				t.Error("wrong value")
			}
		} else {
			if val != 0 {
				t.Error("wrong value")
			}
		}
		return true
	})
	count := 0
	bm.ForEachByte(0, 0, func(offset int64, val byte) bool {
		count++
		return false
	})
	if count != 1 {
		t.Error("break failed")
	}
}
