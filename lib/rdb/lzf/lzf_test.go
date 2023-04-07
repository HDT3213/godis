package lzf

import (
	"math/rand"
	"strings"
	"testing"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// RandString create a random string no longer than n
func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TestLzf(t *testing.T) {
	for i := 0; i < 10; i++ {
		str := strings.Repeat(RandString(128), 10)
		compressed, err := Compress([]byte(str))
		if err != nil {
			t.Error(err)
			return
		}
		decompressed, err := Decompress(compressed, len(compressed), len(str))
		if err != nil {
			t.Error(err)
			return
		}
		if str != string(decompressed) {
			t.Error("wrong decompressed")
			return
		}
	}
}
