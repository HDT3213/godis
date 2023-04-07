package core

import (
	"bytes"
	"testing"

	"github.com/hdt3213/godis/lib/rdb/model"
)

func TestZSetEncoding(t *testing.T) {
	var entries []*model.ZSetEntry
	for i := 0; i < 1024; i++ {
		entries = append(entries, &model.ZSetEntry{
			Member: RandString(32),
			Score:  float64(i),
		})
	}
	zSetMap := map[string][]*model.ZSetEntry{
		"a": {
			{
				Member: "1",
				Score:  3.14,
			},
			{
				Member: "b",
				Score:  3.15,
			},
		},
		"1": {
			{
				Member: "1",
				Score:  2.71828,
			},
			{
				Member: "zxcv",
				Score:  2.8,
			},
		},
		"large": entries,
	}
	buf := bytes.NewBuffer(nil)
	enc := NewEncoder(buf).SetZSetZipListOpt(64, 64)
	err := enc.WriteHeader()
	if err != nil {
		t.Error(err)
		return
	}
	err = enc.WriteDBHeader(0, uint64(len(zSetMap)), 0)
	if err != nil {
		t.Error(err)
		return
	}
	for k, v := range zSetMap {
		err = enc.WriteZSetObject(k, v)
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
		case *model.ZSetObject:
			expect := zSetMap[o.GetKey()]
			if len(expect) != o.GetElemCount() {
				t.Errorf("zset %s has wrong element count", o.GetKey())
				return true
			}
			for i, expectV := range expect {
				actualV := o.Entries[i]
				if actualV.Member != expectV.Member {
					t.Errorf("zset %s has wrong member", o.GetKey())
					return true
				}
				if actualV.Score != expectV.Score {
					t.Errorf("zset %s has wrong score of %s", o.GetKey(), actualV.Member)
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
