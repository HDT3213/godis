package consistenthash

import "testing"

func TestHash(t *testing.T) {
	m := New(3, nil)
	m.AddNode("a", "b", "c", "d")
	if m.PickNode("zxc") != "a" {
		t.Error("wrong answer")
	}
	if m.PickNode("123{abc}") != "b" {
		t.Error("wrong answer")
	}
	if m.PickNode("abc") != "b" {
		t.Error("wrong answer")
	}
}
