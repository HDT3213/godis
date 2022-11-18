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

func TestSortedSet_GetByRank(t *testing.T) {
	var list = makeSkiplist()
	node := list.getByRank(1)
	if node != nil {
		t.Failed()
	}
	list.insert("m1", 1)
	list.insert("m2", 2)
	list.insert("m3", 3)
	node = list.getByRank(3)
	if node.Element.Member != "m3" {
		t.Failed()
	}
	node = list.getByRank(2)
	if node.Element.Member != "m2" {
		t.Failed()
	}
	node = list.getByRank(1)
	if node.Element.Member != "m1" {
		t.Failed()
	}
	node = list.getByRank(5)
	if node != nil {
		t.Failed()
	}
}
