package eviction

import (
	"github.com/hdt3213/godis/interface/eviction"
	"time"
)

type LRUPolicy struct {
	AllKeys bool
}

func (policy *LRUPolicy) IsAllKeys() bool {
	return policy.AllKeys
}
func (policy *LRUPolicy) MakeMark() (lfu int32) {
	return LRUGetTimeInSecond()
}

func (policy *LRUPolicy) UpdateMark(lfu int32) int32 {
	return LRUGetTimeInSecond()
}

func (policy *LRUPolicy) Eviction(marks []eviction.KeyMark) string {
	key := marks[0].Key
	min := marks[0].Mark
	for i := 1; i < len(marks); i++ {
		if min > marks[i].Mark {
			key = marks[i].Key
			min = marks[i].Mark
		}
	}
	return key
}

func LRUGetTimeInSecond() int32 {
	return int32(time.Now().Unix() & 0xffffffff)
}
