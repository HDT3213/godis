package sortedset

import "testing"

func TestSortedSet_PopMin(t *testing.T) {
	var set = Make()
	set.Add("s1", 1)
	set.Add("s2", 2)
	set.Add("s3", 3)
	set.Add("s4", 4)

	var results = set.PopMin(2)
	if results[0].Member != "s1" || results[1].Member != "s2" {
		t.Fail()
	}
}
