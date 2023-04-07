package model

import (
	"encoding/json"
	"time"
)

const (
	// StringType is redis string
	StringType = "string"
	// ListType is redis list
	ListType = "list"
	// SetType is redis set
	SetType = "set"
	// HashType is redis hash
	HashType = "hash"
	// ZSetType is redis sorted set
	ZSetType = "zset"
	// AuxType is redis metadata key-value pair
	AuxType = "aux"
	// DBSizeType is for RDB_OPCODE_RESIZEDB
	DBSizeType = "dbsize"
)

// CallbackFunc process redis object
type CallbackFunc func(object RedisObject) bool

// RedisObject is interface for a redis object
type RedisObject interface {
	// GetType returns redis type of object: string/list/set/hash/zset
	GetType() string
	// GetKey returns key of object
	GetKey() string
	// GetDBIndex returns db index of object
	GetDBIndex() int
	// GetExpiration returns expiration time, expiration of persistent object is nil
	GetExpiration() *time.Time
	// GetSize returns rdb value size in Byte
	GetSize() int
	// GetElemCount returns number of elements in list/set/hash/zset
	GetElemCount() int
}

// BaseObject is basement of redis object
type BaseObject struct {
	DB         int        `json:"db"`                   // DB is db index of redis object
	Key        string     `json:"key"`                  // Key is key of redis object
	Expiration *time.Time `json:"expiration,omitempty"` // Expiration is expiration time, expiration of persistent object is nil
	Size       int        `json:"size"`                 // Size is rdb value size in Byte
	Type       string     `json:"type"`
}

// GetKey returns key of object
func (o *BaseObject) GetKey() string {
	return o.Key
}

// GetDBIndex returns db index of object
func (o *BaseObject) GetDBIndex() int {
	return o.DB
}

// GetExpiration returns expiration time, expiration of persistent object is nil
func (o *BaseObject) GetExpiration() *time.Time {
	return o.Expiration
}

// GetSize  returns rdb value size in Byte
func (o *BaseObject) GetSize() int {
	return o.Size
}

// GetElemCount returns number of elements in list/set/hash/zset
func (o *BaseObject) GetElemCount() int {
	return 0
}

// StringObject stores a string object
type StringObject struct {
	*BaseObject
	Value []byte
}

// GetType returns redis object type
func (o *StringObject) GetType() string {
	return StringType
}

// MarshalJSON marshal []byte as string
func (o *StringObject) MarshalJSON() ([]byte, error) {
	o2 := struct {
		*BaseObject
		Value string `json:"value"`
	}{
		BaseObject: o.BaseObject,
		Value:      string(o.Value),
	}
	return json.Marshal(o2)
}

// ListObject stores a list object
type ListObject struct {
	*BaseObject
	Values [][]byte
}

// GetType returns redis object type
func (o *ListObject) GetType() string {
	return ListType
}

// GetElemCount returns number of elements in list/set/hash/zset
func (o *ListObject) GetElemCount() int {
	return len(o.Values)
}

// MarshalJSON marshal []byte as string
func (o *ListObject) MarshalJSON() ([]byte, error) {
	values := make([]string, len(o.Values))
	for i, v := range o.Values {
		values[i] = string(v)
	}
	o2 := struct {
		*BaseObject
		Values []string `json:"values"`
	}{
		BaseObject: o.BaseObject,
		Values:     values,
	}
	return json.Marshal(o2)
}

// HashObject stores a hash object
type HashObject struct {
	*BaseObject
	Hash map[string][]byte
}

// GetType returns redis object type
func (o *HashObject) GetType() string {
	return HashType
}

// GetElemCount returns number of elements in list/set/hash/zset
func (o *HashObject) GetElemCount() int {
	return len(o.Hash)
}

// MarshalJSON marshal []byte as string
func (o *HashObject) MarshalJSON() ([]byte, error) {
	m := make(map[string]string)
	for k, v := range o.Hash {
		m[k] = string(v)
	}
	o2 := struct {
		*BaseObject
		Hash map[string]string `json:"hash"`
	}{
		BaseObject: o.BaseObject,
		Hash:       m,
	}
	return json.Marshal(o2)
}

// SetObject stores a set object
type SetObject struct {
	*BaseObject
	Members [][]byte
}

// GetType returns redis object type
func (o *SetObject) GetType() string {
	return SetType
}

// GetElemCount returns number of elements in list/set/hash/zset
func (o *SetObject) GetElemCount() int {
	return len(o.Members)
}

// MarshalJSON marshal []byte as string
func (o *SetObject) MarshalJSON() ([]byte, error) {
	values := make([]string, len(o.Members))
	for i, v := range o.Members {
		values[i] = string(v)
	}
	o2 := struct {
		*BaseObject
		Members []string `json:"members"`
	}{
		BaseObject: o.BaseObject,
		Members:    values,
	}
	return json.Marshal(o2)
}

// ZSetEntry is a key-score in sorted set
type ZSetEntry struct {
	Member string  `json:"member"`
	Score  float64 `json:"score"`
}

// ZSetObject stores a sorted set object
type ZSetObject struct {
	*BaseObject
	Entries []*ZSetEntry `json:"entries"`
}

// GetType returns redis object type
func (o *ZSetObject) GetType() string {
	return ZSetType
}

// GetElemCount returns number of elements in list/set/hash/zset
func (o *ZSetObject) GetElemCount() int {
	return len(o.Entries)
}

// AuxObject stores redis metadata
type AuxObject struct {
	*BaseObject
	Value string
}

// GetType returns redis object type
func (o *AuxObject) GetType() string {
	return AuxType
}

// MarshalJSON marshal []byte as string
func (o *AuxObject) MarshalJSON() ([]byte, error) {
	o2 := struct {
		*BaseObject
		Value string `json:"value"`
	}{
		BaseObject: o.BaseObject,
		Value:      string(o.Value),
	}
	return json.Marshal(o2)
}

// DBSizeObject stores db size metadata
type DBSizeObject struct {
	*BaseObject
	KeyCount uint64
	TTLCount uint64
}

// GetType returns redis object type
func (o *DBSizeObject) GetType() string {
	return DBSizeType
}
