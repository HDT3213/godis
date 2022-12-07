package database

import (
	"github.com/hdt3213/godis/aof"
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
	err = importRDB(decoder, mdb)
	if err != nil {
		logger.Error("dump rdb file failed " + err.Error())
		return
	}
}

func importRDB(dec *core.Decoder, mdb *MultiDB) error {
	return dec.Parse(func(o rdb.RedisObject) bool {
		db := mdb.mustSelectDB(o.GetDBIndex())
		var entity *database.DataEntity
		switch o.GetType() {
		case rdb.StringType:
			str := o.(*rdb.StringObject)
			entity = &database.DataEntity{
				Data: str.Value,
			}
		case rdb.ListType:
			listObj := o.(*rdb.ListObject)
			list := List.NewQuickList()
			for _, v := range listObj.Values {
				list.Add(v)
			}
			entity = &database.DataEntity{
				Data: list,
			}
		case rdb.HashType:
			hashObj := o.(*rdb.HashObject)
			hash := dict.MakeSimple()
			for k, v := range hashObj.Hash {
				hash.Put(k, v)
			}
			entity = &database.DataEntity{
				Data: hash,
			}
		case rdb.ZSetType:
			zsetObj := o.(*rdb.ZSetObject)
			zSet := SortedSet.Make()
			for _, e := range zsetObj.Entries {
				zSet.Add(e.Member, e.Score)
			}
			entity = &database.DataEntity{
				Data: zSet,
			}
		}
		if entity != nil {
			db.PutEntity(o.GetKey(), entity)
			if o.GetExpiration() != nil {
				db.Expire(o.GetKey(), *o.GetExpiration())
			}
			db.addAof(aof.EntityToCmd(o.GetKey(), entity).Args)
		}
		return true
	})
}
