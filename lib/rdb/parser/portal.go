// Package parser is interface for parser
package parser

import (
	"github.com/hdt3213/godis/lib/rdb/core"
	"github.com/hdt3213/godis/lib/rdb/model"
)

const (
	// StringType is redis string
	StringType = model.StringType
	// ListType is redis list
	ListType = model.ListType
	// SetType is redis set
	SetType = model.SetType
	// HashType is redis hash
	HashType = model.HashType
	// ZSetType is redis sorted set
	ZSetType = model.ZSetType
	// AuxType is redis metadata key-value pair
	AuxType = model.AuxType
	// DBSizeType is for RDB_OPCODE_RESIZEDB
	DBSizeType = model.DBSizeType
)

type (
	// RedisObject is interface for a redis object
	RedisObject = model.RedisObject
	// StringObject stores a string object
	StringObject = model.StringObject
	// ListObject stores a list object
	ListObject = model.ListObject
	// SetObject stores a set object
	SetObject = model.SetObject
	// HashObject stores a hash object
	HashObject = model.HashObject
	// ZSetObject stores a sorted set object
	ZSetObject = model.ZSetObject
	// AuxObject stores redis metadata
	AuxObject = model.AuxObject
	// DBSizeObject stores db size metadata
	DBSizeObject = model.DBSizeObject
)

var (
	// NewDecoder creates a new RDB decoder
	NewDecoder = core.NewDecoder
)
