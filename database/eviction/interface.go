package eviction

type MaxmemoryPolicy interface {
	MakeMark() int32
	UpdateMark(int32) int32
	Eviction([]KeyMark) string
	IsAllKeys() bool
}

type KeyMark struct {
	Key  string
	Mark int32
}
