package db

import (
	"github.com/HDT3213/godis/src/datastruct/dict"
	"github.com/HDT3213/godis/src/datastruct/lock"
	"math/rand"
	"time"
)

func makeTestDB() *DB {
	return &DB{
		Data:     dict.MakeConcurrent(1),
		TTLMap:   dict.MakeConcurrent(ttlDictSize),
		Locker:   lock.Make(lockerSize),
		interval: 5 * time.Second,
	}
}

func toArgs(cmd ...string) [][]byte {
	args := make([][]byte, len(cmd))
	for i, s := range cmd {
		args[i] = []byte(s)
	}
	return args
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
