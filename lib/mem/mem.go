package mem

import (
	"github.com/hdt3213/godis/config"
	"runtime"
	"sync"
)

var Lock sync.Mutex

func UsedMemory() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc / 1024 / 1024
}

func GetMaxMemoryState(toFree interface{}) bool {
	if config.Properties.Maxmemory == 0 {
		return false
	}
	if UsedMemory() > config.Properties.Maxmemory {
		toFree, ok := toFree.(*uint64)
		if ok && toFree != nil {
			*toFree = UsedMemory() - config.Properties.Maxmemory
		}
		return true
	}
	return false
}
