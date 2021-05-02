package idgenerator

import (
	"hash/fnv"
	"log"
	"sync"
	"time"
)

const (
	// Epoch is set to the twitter snowflake epoch of Nov 04 2010 01:42:54 UTC in milliseconds
	// You may customize this to set a different epoch for your application.
	Epoch       int64 = 1288834974657
	maxSequence int64 = -1 ^ (-1 << uint64(nodeLeft))
	timeLeft    uint8 = 22
	nodeLeft    uint8 = 10
	nodeMask    int64 = -1 ^ (-1 << uint64(timeLeft-nodeLeft))
)

type IdGenerator struct {
	mu        *sync.Mutex
	lastStamp int64
	workerId  int64
	sequence  int64
	epoch     time.Time
}

func MakeGenerator(node string) *IdGenerator {
	fnv64 := fnv.New64()
	_, _ = fnv64.Write([]byte(node))
	nodeId := int64(fnv64.Sum64()) & nodeMask

	var curTime = time.Now()
	epoch := curTime.Add(time.Unix(Epoch/1000, (Epoch%1000)*1000000).Sub(curTime))

	return &IdGenerator{
		mu:        &sync.Mutex{},
		lastStamp: -1,
		workerId:  nodeId,
		sequence:  1,
		epoch:     epoch,
	}
}

func (w *IdGenerator) NextId() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	timestamp := time.Since(w.epoch).Nanoseconds() / 1000000
	if timestamp < w.lastStamp {
		log.Fatal("can not generate id")
	}
	if w.lastStamp == timestamp {
		w.sequence = (w.sequence + 1) & maxSequence
		if w.sequence == 0 {
			for timestamp <= w.lastStamp {
				timestamp = time.Since(w.epoch).Nanoseconds() / 1000000
			}
		}
	} else {
		w.sequence = 0
	}
	w.lastStamp = timestamp
	id := (timestamp << timeLeft) | (w.workerId << nodeLeft) | w.sequence
	//fmt.Printf("%d %d %d\n", timestamp, w.sequence, id)
	return id
}
