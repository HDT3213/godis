package database

import (
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/datastruct/dict"
	List "github.com/hdt3213/godis/datastruct/list"
	SortedSet "github.com/hdt3213/godis/datastruct/sortedset"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/rdb/core"
	rdb "github.com/hdt3213/rdb/parser"
	"os"
)

func loadRdbFile(mdb *MultiDB) {
	rdbFile, err := os.Open(config.Properties.RDBFilename)
	if err != nil {
		logger.Error("open rdb file failed " + err.Error())
		return
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	decoder := rdb.NewDecoder(rdbFile)
	err = dumpRDB(decoder, mdb)
	if err != nil {
		logger.Error("dump rdb file failed " + err.Error())
		return
	}
}

func dumpRDB(dec *core.Decoder, mdb *MultiDB) error {
	return dec.Parse(func(o rdb.RedisObject) bool {
		db := mdb.selectDB(o.GetDBIndex())
		switch o.GetType() {
		case rdb.StringType:
			str := o.(*rdb.StringObject)
			db.PutEntity(o.GetKey(), &database.DataEntity{
				Data: str.Value,
			})
		case rdb.ListType:
			listObj := o.(*rdb.ListObject)
			list := &List.LinkedList{}
			for _, v := range listObj.Values {
				list.Add(v)
			}
			db.PutEntity(o.GetKey(), &database.DataEntity{
				Data: list,
			})
		case rdb.HashType:
			hashObj := o.(*rdb.HashObject)
			hash := dict.MakeSimple()
			for k, v := range hashObj.Hash {
				hash.Put(k, v)
			}
			db.PutEntity(o.GetKey(), &database.DataEntity{
				Data: hash,
			})
		case rdb.ZSetType:
			zsetObj := o.(*rdb.ZSetObject)
			zSet := SortedSet.Make()
			for _, e := range zsetObj.Entries {
				zSet.Add(e.Member, e.Score)
			}
			db.PutEntity(o.GetKey(), &database.DataEntity{
				Data: zSet,
			})
		}
		if o.GetExpiration() != nil {
			db.Expire(o.GetKey(), *o.GetExpiration())
		}
		return true
	})
}
