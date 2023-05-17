package eviction

import (
	"github.com/hdt3213/godis/config"
	"math/rand"
	"time"
)

type LFUPolicy struct {
	AllKeys bool
}

func (policy *LFUPolicy) IsAllKeys() bool {
	return policy.AllKeys
}

//MakeMark create a new mark
func (policy *LFUPolicy) MakeMark() (lfu int32) {
	lfu = lfuGetTimeInMinutes()<<8 | config.Properties.LfuLogFactor
	return lfu
}

//UpdateMark when read a key ,update the key's mark
func (policy *LFUPolicy) UpdateMark(lfu int32) int32 {
	counter := GetLFUCounter(lfu)
	decr := lfuDecrAndReturn(lfu)
	incr := LFULogIncr(counter - uint8(decr))
	return lfuGetTimeInMinutes()<<8 | int32(incr)
}

//Eviction choose a key for eviction
func (policy *LFUPolicy) Eviction(marks []KeyMark) string {
	key := marks[0].Key
	min := GetLFUCounter(marks[0].Mark)
	for i := 1; i < len(marks); i++ {
		counter := GetLFUCounter(marks[i].Mark)
		if min > counter {
			key = marks[i].Key
			min = counter
		}
	}
	return key
}

func GetLFUCounter(lfu int32) uint8 {
	return uint8(lfu & 0xff)
}

// LFULogIncr counter increase
func LFULogIncr(counter uint8) uint8 {
	if counter == 255 {
		return 255
	}
	r := rand.Float64()
	baseval := float64(counter - config.Properties.LfuInitVal)

	if baseval < 0 {
		baseval = 0
	}

	p := 1.0 / (baseval*float64(config.Properties.LfuLogFactor) + 1)

	if r < p {
		counter++
	}
	return counter
}

//LFUDecrAndReturn counter decr
func lfuDecrAndReturn(lfu int32) int32 {
	ldt := lfu >> 8

	counter := lfu & 0xff

	var numPeriods int32
	if config.Properties.LfuDecayTime > 0 {
		numPeriods = lfuTimeElapsed(ldt) / config.Properties.LfuDecayTime
	} else {
		numPeriods = 0
	}

	if numPeriods > 0 {
		if numPeriods > counter {
			counter = 0
		} else {
			counter = counter - numPeriods
		}
	}
	return counter
}

// LFUTimeElapsed Obtaining the time difference from the last time
func lfuTimeElapsed(ldt int32) int32 {
	now := lfuGetTimeInMinutes()
	if now >= ldt {
		return now - ldt
	}
	return 65535 - ldt + now
}

// LFUGetTimeInMinutes Accurate to the minute
func lfuGetTimeInMinutes() int32 {
	return int32(time.Now().Unix()/60) & 65535
}
