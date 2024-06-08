package utils

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

// Function test of GetItoaLen

func TestGetIntCnvStrLenWithRandom(t *testing.T) {
	type args struct {
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{"随机数测试", args{}},
	}
	for _, tt := range tests {
		r := rand.NewSource(time.Now().UnixNano())
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 1000000; i++ {
				num := r.Int63()
				if got := GetItoaLen(int(num)); got != len(strconv.Itoa(int(num))) {
					t.Errorf("FastItoa() = %v, want %v", got, len(strconv.Itoa(int(num))))
				}
			}
		})
	}
}

func TestGetIntCnvStrLenWithRandomNegative(t *testing.T) {
	type args struct {
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{"随机负数测试", args{}},
	}
	for _, tt := range tests {
		r := rand.NewSource(time.Now().UnixNano())
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 1000000; i++ {
				num := r.Int63()
				if got := GetItoaLen(int(0 - num)); got != len(strconv.Itoa(int(0-num))) {
					t.Errorf("FastItoa() = %v, want %v", got, len(strconv.Itoa(int(num))))
				}
			}
		})
	}
}

// Benchmark  strconv.Itoa vs  GetItoaLen

func Benchmark_Itoa_20bit(b *testing.B) {
	//strconv.Itoa(argLen)
	type args struct {
		i int
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{"20 bit", args{-2381934732381934731}},
	}
	for _, tt := range tests {
		for i := 0; i < b.N; i++ {
			_ = len(strconv.Itoa(tt.args.i))
		}
	}
}

func Benchmark_GetIotaLen_20bit(b *testing.B) {
	//strconv.Itoa(argLen)
	type args struct {
		i int
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{"20 bit", args{-2381934732381934731}},
	}
	for _, tt := range tests {
		for i := 0; i < b.N; i++ {
			_ = GetItoaLen(tt.args.i)
		}
	}
}

func Benchmark_Itoa_10bit(b *testing.B) {
	//strconv.Itoa(argLen)
	type args struct {
		i int
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{"10 bit", args{-238193473}},
	}
	for _, tt := range tests {
		for i := 0; i < b.N; i++ {
			_ = len(strconv.Itoa(tt.args.i))
		}
	}
}

func Benchmark_GetIotaLen_10bit(b *testing.B) {
	//strconv.Itoa(argLen)
	type args struct {
		i int
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{"10 bit", args{-238193473}},
	}
	for _, tt := range tests {
		for i := 0; i < b.N; i++ {
			_ = GetItoaLen(tt.args.i)
		}
	}
}
