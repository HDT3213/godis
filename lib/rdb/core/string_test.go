package core

import (
	"bytes"
	"strings"
	"testing"
)

func TestLengthEncoding(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	enc := NewEncoder(buf)
	lens := []uint64{1 << 5, 1 << 13, 1 << 31, 1 << 63}
	for _, v := range lens {
		err := enc.writeLength(v)
		if err != nil {
			t.Error(err)
			return
		}
	}
	dec := NewDecoder(buf)
	for _, expect := range lens {
		actual, special, err := dec.readLength()
		if err != nil {
			t.Error(err)
			return
		}
		if special {
			t.Error("expect normal actual special")
			return
		}
		if actual != expect {
			t.Errorf("expect %d, actual %d", expect, actual)
		}
	}
}

func TestStringEncoding(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	enc := NewEncoder(buf)
	strList := []string{
		"abc",
		"12",
		"32766",
		"2147483647",
	}
	for _, str := range strList {
		err := enc.writeString(str)
		if err != nil {
			t.Error(err)
			return
		}
	}
	dec := NewDecoder(buf)
	for _, expect := range strList {
		actual, err := dec.readString()
		if err != nil {
			t.Error(err)
			continue
		}
		if string(actual) != expect {
			t.Errorf("expect %s, actual %s", expect, string(actual))
		}
	}
}

func TestLZFString(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	enc := NewEncoder(buf).EnableCompress()
	var strList []string
	for i := 0; i < 10; i++ {
		strList = append(strList, strings.Repeat(RandString(128), 10))
	}
	for _, str := range strList {
		err := enc.writeString(str)
		if err != nil {
			t.Error(err)
			return
		}
	}
	dec := NewDecoder(buf)
	for _, expect := range strList {
		actual, err := dec.readString()
		if err != nil {
			t.Error(err)
			continue
		}
		if string(actual) != expect {
			t.Errorf("expect %s, actual %s", expect, string(actual))
		}
	}
}
