package core

import (
	"bytes"
	"testing"

	"github.com/hdt3213/rdb/model"
)

func TestHashEncoding(t *testing.T) {
	mapMap := map[string]map[string][]byte{
		"a": {
			"a": []byte("foo bar"),
		},
		"1": {
			"233": []byte("114514"),
		},
		"2": {
			"n": []byte(RandString(128)),
			"o": []byte(RandString(128)),
		},
	}
	m := make(map[string][]byte)
	for i := 0; i < 1024; i++ {
		m[RandString(64)] = []byte(RandString(128))
	}
	mapMap["large"] = m
	buf := bytes.NewBuffer(nil)
	enc := NewEncoder(buf).SetHashZipListOpt(64, 64)
	err := enc.WriteHeader()
	if err != nil {
		t.Error(err)
		return
	}
	err = enc.WriteDBHeader(0, uint64(len(mapMap)), 0)
	if err != nil {
		t.Error(err)
		return
	}
	for k, v := range mapMap {
		err = enc.WriteHashMapObject(k, v)
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
	dec := NewDecoder(buf)
	err = dec.Parse(func(object model.RedisObject) bool {
		switch o := object.(type) {
		case *model.HashObject:
			expect := mapMap[o.GetKey()]
			if len(expect) != o.GetElemCount() {
				t.Errorf("hash %s has wrong element count", o.GetKey())
				return true
			}
			for field, expectV := range expect {
				actualV := o.Hash[field]
				if !bytes.Equal(expectV, actualV) {
					t.Errorf("hash %s has element at field %s", o.GetKey(), field)
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
