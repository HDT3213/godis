package database

import (
	"github.com/hdt3213/godis/datastruct/dict"
)

func makeTestDB() *DB {
	return &DB{
		data:           dict.MakeConcurrent(dataDictSize),
		versionMap:     dict.MakeConcurrent(dataDictSize),
		ttlMap:         dict.MakeConcurrent(ttlDictSize),
    evictionMap:    dict.MakeConcurrent(dataDictSize),
		evictionPolicy: makeEvictionPolicy(),
		addAof:         func(line CmdLine) {},
	}
}
