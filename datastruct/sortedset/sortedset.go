package sortedset

import (
	"strconv"

	"github.com/hdt3213/godis/lib/wildcard"
)

// SortedSet is a set which keys sorted by bound score
type SortedSet struct {
	dict     map[string]*Element
	skiplist *skiplist
}

// Make makes a new SortedSet
func Make() *SortedSet {
	return &SortedSet{
		dict:     make(map[string]*Element),
		skiplist: makeSkiplist(),
	}
}

// Add puts member into set,  and returns whether it has inserted new node
func (sortedSet *SortedSet) Add(member string, score float64) bool {
	element, ok := sortedSet.dict[member]
	sortedSet.dict[member] = &Element{
		Member: member,
		Score:  score,
	}
	if ok {
		if score != element.Score {
			sortedSet.skiplist.remove(member, element.Score)
			sortedSet.skiplist.insert(member, score)
		}
		return false
	}
	sortedSet.skiplist.insert(member, score)
	return true
}

// Len returns number of members in set
func (sortedSet *SortedSet) Len() int64 {
	return int64(len(sortedSet.dict))
}

// Get returns the given member
func (sortedSet *SortedSet) Get(member string) (element *Element, ok bool) {
	element, ok = sortedSet.dict[member]
	if !ok {
		return nil, false
	}
	return element, true
}

// Remove removes the given member from set
func (sortedSet *SortedSet) Remove(member string) bool {
	v, ok := sortedSet.dict[member]
	if ok {
		sortedSet.skiplist.remove(member, v.Score)
		delete(sortedSet.dict, member)
		return true
	}
	return false
}

// GetRank returns the rank of the given member, sort by ascending order, rank starts from 0
func (sortedSet *SortedSet) GetRank(member string, desc bool) (rank int64) {
	element, ok := sortedSet.dict[member]
	if !ok {
		return -1
	}
	r := sortedSet.skiplist.getRank(member, element.Score)
	if desc {
		r = sortedSet.skiplist.length - r
	} else {
		r--
	}
	return r
}

// ForEachByRank visits each member which rank within [start, stop), sort by ascending order, rank starts from 0
func (sortedSet *SortedSet) ForEachByRank(start int64, stop int64, desc bool, consumer func(element *Element) bool) {
	size := sortedSet.Len()
	if start < 0 || start >= size {
		panic("illegal start " + strconv.FormatInt(start, 10))
	}
	if stop < start || stop > size {
		panic("illegal end " + strconv.FormatInt(stop, 10))
	}

	// find start node
	var node *node
	if desc {
		node = sortedSet.skiplist.tail
		if start > 0 {
			node = sortedSet.skiplist.getByRank(size - start)
		}
	} else {
		node = sortedSet.skiplist.header.level[0].forward
		if start > 0 {
			node = sortedSet.skiplist.getByRank(start + 1)
		}
	}

	sliceSize := int(stop - start)
	for i := 0; i < sliceSize; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}
}

// RangeByRank returns members which rank within [start, stop), sort by ascending order, rank starts from 0
func (sortedSet *SortedSet) RangeByRank(start int64, stop int64, desc bool) []*Element {
	sliceSize := int(stop - start)
	slice := make([]*Element, sliceSize)
	i := 0
	sortedSet.ForEachByRank(start, stop, desc, func(element *Element) bool {
		slice[i] = element
		i++
		return true
	})
	return slice
}

// RangeCount returns the number of  members which score or member within the given border
func (sortedSet *SortedSet) RangeCount(min Border, max Border) int64 {
	var i int64 = 0
	// ascending order
	sortedSet.ForEachByRank(0, sortedSet.Len(), false, func(element *Element) bool {
		gtMin := min.less(element) // greater than min
		if !gtMin {
			// has not into range, continue foreach
			return true
		}
		ltMax := max.greater(element) // less than max
		if !ltMax {
			// break through score border, break foreach
			return false
		}
		// gtMin && ltMax
		i++
		return true
	})
	return i
}

// ForEach visits members which score or member within the given border
func (sortedSet *SortedSet) ForEach(min Border, max Border, offset int64, limit int64, desc bool, consumer func(element *Element) bool) {
	// find start node
	var node *node
	if desc {
		node = sortedSet.skiplist.getLastInRange(min, max)
	} else {
		node = sortedSet.skiplist.getFirstInRange(min, max)
	}

	for node != nil && offset > 0 {
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		offset--
	}

	// A negative limit returns all elements from the offset
	for i := 0; (i < int(limit) || limit < 0) && node != nil; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		if node == nil {
			break
		}
		gtMin := min.less(&node.Element) // greater than min
		ltMax := max.greater(&node.Element)
		if !gtMin || !ltMax {
			break // break through score border
		}
	}
}

// Range returns members which score or member within the given border
// param limit: <0 means no limit
func (sortedSet *SortedSet) Range(min Border, max Border, offset int64, limit int64, desc bool) []*Element {
	if limit == 0 || offset < 0 {
		return make([]*Element, 0)
	}
	slice := make([]*Element, 0)
	sortedSet.ForEach(min, max, offset, limit, desc, func(element *Element) bool {
		slice = append(slice, element)
		return true
	})
	return slice
}

// RemoveRange removes members which score or member within the given border
func (sortedSet *SortedSet) RemoveRange(min Border, max Border) int64 {
	removed := sortedSet.skiplist.RemoveRange(min, max, 0)
	for _, element := range removed {
		delete(sortedSet.dict, element.Member)
	}
	return int64(len(removed))
}

func (sortedSet *SortedSet) PopMin(count int) []*Element {
	first := sortedSet.skiplist.getFirstInRange(scoreNegativeInfBorder, scorePositiveInfBorder)
	if first == nil {
		return nil
	}
	border := &ScoreBorder{
		Value:   first.Score,
		Exclude: false,
	}
	removed := sortedSet.skiplist.RemoveRange(border, scorePositiveInfBorder, count)
	for _, element := range removed {
		delete(sortedSet.dict, element.Member)
	}
	return removed
}

// RemoveByRank removes member ranking within [start, stop)
// sort by ascending order and rank starts from 0
func (sortedSet *SortedSet) RemoveByRank(start int64, stop int64) int64 {
	removed := sortedSet.skiplist.RemoveRangeByRank(start+1, stop+1)
	for _, element := range removed {
		delete(sortedSet.dict, element.Member)
	}
	return int64(len(removed))
}

func (sortedSet *SortedSet) ZSetScan(cursor int, count int, pattern string) ([][]byte, int) {
	result := make([][]byte, 0)
	matchKey, err := wildcard.CompilePattern(pattern)
	if err != nil {
		return result, -1
	}
	for k := range sortedSet.dict {
		if pattern == "*" || matchKey.IsMatch(k) {
			elem, exists := sortedSet.dict[k]
			if !exists {
				continue
			}
			result = append(result, []byte(k))
			result = append(result, []byte(strconv.FormatFloat(elem.Score, 'f', 10, 64)))
		}
	}
	return result, 0
}
