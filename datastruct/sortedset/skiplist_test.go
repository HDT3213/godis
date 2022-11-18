package sortedset

import "testing"

func TestRandomLevel(t *testing.T) {
	m := make(map[int16]int)
	for i := 0; i < 10000; i++ {
		level := randomLevel()
		m[level]++
	}
	for i := 0; i <= maxLevel; i++ {
		t.Logf("level %d, count %d", i, m[int16(i)])
	}
}
