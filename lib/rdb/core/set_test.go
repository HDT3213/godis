package core

import (
	"bytes"
	"testing"

	"github.com/hdt3213/godis/lib/rdb/model"
)

func TestSetEncoding(t *testing.T) {
	setMap := map[string][][]byte{
		"a": {
			[]byte("a"), []byte("b"), []byte("c"), []byte("d"),
		},
		"1": {
			[]byte("1"), []byte("2"), []byte("3"), []byte("4"), // int set encoding need ascending order
		},
		"2": {
			[]byte("-32767"), []byte("32767"),
		},
		"4": {
			[]byte("-2147483647"), []byte("2147483647"),
		},
		"8": {
			[]byte("-9222147483647"), []byte("9222147483647"),
		},
	}
	buf := bytes.NewBuffer(nil)
	enc := NewEncoder(buf)
	err := enc.WriteHeader()
	if err != nil {
		t.Error(err)
		return
	}
	err = enc.WriteDBHeader(0, uint64(len(setMap)), 0)
	if err != nil {
		t.Error(err)
		return
	}
	for k, v := range setMap {
		err = enc.WriteSetObject(k, v)
		if err != nil {
			t.Error(err)
			return
		}
	}
	err = enc.WriteEnd()
	if err != nil {
		t.Error(err)
		return
	}
	dec := NewDecoder(buf).WithSpecialOpCode()
	err = dec.Parse(func(object model.RedisObject) bool {
		switch o := object.(type) {
		case *model.SetObject:
			expect := setMap[o.GetKey()]
			if len(expect) != o.GetElemCount() {
				t.Errorf("set %s has wrong element count", o.GetKey())
				return true
			}
			for i, expectV := range expect {
				actualV := o.Members[i]
				if !bytes.Equal(expectV, actualV) {
					t.Errorf("set %s has element at index %d", o.GetKey(), i)
					return true
				}
			}
		}
		return true
	})
	if err != nil {
		t.Error(err)
	}
}
