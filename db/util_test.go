package db

import (
	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/datastruct/lock"
)

func makeTestDB() *DB {
	return &DB{
		data:   dict.MakeConcurrent(1),
		ttlMap: dict.MakeConcurrent(ttlDictSize),
		locker: lock.Make(lockerSize),
	}
}

