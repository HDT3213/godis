package idgenerator

import (
	"hash/fnv"
	"log"
	"sync"
	"time"
)

const (
	workerIdBits     int64 = 5
	datacenterIdBits int64 = 5
	sequenceBits     int64 = 12

	maxWorkerId     int64 = -1 ^ (-1 << uint64(workerIdBits))
	maxDatacenterId int64 = -1 ^ (-1 << uint64(datacenterIdBits))
	maxSequence     int64 = -1 ^ (-1 << uint64(sequenceBits))

	timeLeft uint8 = 22
	dataLeft uint8 = 17
	workLeft uint8 = 12

	twepoch int64 = 1525705533000
)

type IdGenerator struct {
	mu           *sync.Mutex
	lastStamp    int64
	workerId     int64
	dataCenterId int64
	sequence     int64
}

func MakeGenerator(cluster string, node string) *IdGenerator {
	fnv64 := fnv.New64()
	_, _ = fnv64.Write([]byte(cluster))
	dataCenterId := int64(fnv64.Sum64())

	fnv64.Reset()
	_, _ = fnv64.Write([]byte(node))
	workerId := int64(fnv64.Sum64())

	return &IdGenerator{
		mu:           &sync.Mutex{},
		lastStamp:    -1,
		dataCenterId: dataCenterId,
		workerId:     workerId,
		sequence:     1,
	}
}

func (w *IdGenerator) getCurrentTime() int64 {
	return time.Now().UnixNano() / 1e6
}

func (w *IdGenerator) NextId() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	timestamp := w.getCurrentTime()
	if timestamp < w.lastStamp {
		log.Fatal("can not generate id")
	}
	if w.lastStamp == timestamp {
		w.sequence = (w.sequence + 1) & maxSequence
		if w.sequence == 0 {
			for timestamp <= w.lastStamp {
				timestamp = w.getCurrentTime()
			}
		}
	} else {
		w.sequence = 0
	}
	w.lastStamp = timestamp

	return ((timestamp - twepoch) << timeLeft) | (w.dataCenterId << dataLeft) | (w.workerId << workLeft) | w.sequence
}

func (w *IdGenerator) tilNextMillis() int64 {
	timestamp := w.getCurrentTime()
	if timestamp <= w.lastStamp {
		timestamp = w.getCurrentTime()
	}
	return timestamp
}
