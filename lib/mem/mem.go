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

func OutOfMemory() bool {
	if config.Properties.Maxmemory == 0 {
		return false
	}
	if UsedMemory() > config.Properties.Maxmemory {
		return true
	}
	return false
}
