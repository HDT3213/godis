package set

import "github.com/hdt3213/godis/datastruct/dict"

// Set is a set of elements based on hash table
type Set struct {
	dict dict.Dict
}

// Make creates a new set
func Make(members ...string) *Set {
	set := &Set{
		dict: dict.MakeSimple(),
	}
	for _, member := range members {
		set.Add(member)
	}
	return set
}

// Add adds member into set
func (set *Set) Add(val string) int {
	return set.dict.Put(val, nil)
}

// Remove removes member from set
func (set *Set) Remove(val string) int {
	return set.dict.Remove(val)
}

// Has returns true if the val exists in the set
func (set *Set) Has(val string) bool {
	_, exists := set.dict.Get(val)
	return exists
}

// Len returns number of members in the set
func (set *Set) Len() int {
	return set.dict.Len()
}

// ToSlice convert set to []string
func (set *Set) ToSlice() []string {
	slice := make([]string, set.Len())
	i := 0
	set.dict.ForEach(func(key string, val interface{}) bool {
		if i < len(slice) {
			slice[i] = key
		} else {
			// set extended during traversal
			slice = append(slice, key)
		}
		i++
		return true
	})
	return slice
}

// ForEach visits each member in the set
func (set *Set) ForEach(consumer func(member string) bool) {
	set.dict.ForEach(func(key string, val interface{}) bool {
		return consumer(key)
	})
}

// Intersect intersects two sets
func (set *Set) Intersect(another *Set) *Set {
	if set == nil {
		panic("set is nil")
	}

	result := Make()
	another.ForEach(func(member string) bool {
		if set.Has(member) {
			result.Add(member)
		}
		return true
	})
	return result
}

// Union adds two sets
func (set *Set) Union(another *Set) *Set {
	if set == nil {
		panic("set is nil")
	}
	result := Make()
	another.ForEach(func(member string) bool {
		result.Add(member)
		return true
	})
	set.ForEach(func(member string) bool {
		result.Add(member)
		return true
	})
	return result
}

// Diff subtracts two sets
func (set *Set) Diff(another *Set) *Set {
	if set == nil {
		panic("set is nil")
	}

	result := Make()
	set.ForEach(func(member string) bool {
		if !another.Has(member) {
			result.Add(member)
		}
		return true
	})
	return result
}

// RandomMembers randomly returns keys of the given number, may contain duplicated key
func (set *Set) RandomMembers(limit int) []string {
	return set.dict.RandomKeys(limit)
}

// RandomDistinctMembers randomly returns keys of the given number, won't contain duplicated key
func (set *Set) RandomDistinctMembers(limit int) []string {
	return set.dict.RandomDistinctKeys(limit)
}
