package cluster

import "testing"

func TestMarshalSlotIds(t *testing.T) {
	slots := []*Slot{
		{ID: 1},
		{ID: 3},
		{ID: 4},
		{ID: 5},
		{ID: 7},
		{ID: 9},
		{ID: 10},
	}
	enc := marshalSlotIds(slots)
	println(enc)
}
