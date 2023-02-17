package geohash

import (
	"encoding/base32"
	"encoding/binary"
)

var bits = []uint8{128, 64, 32, 16, 8, 4, 2, 1}
var enc = base32.NewEncoding("0123456789bcdefghjkmnpqrstuvwxyz").WithPadding(base32.NoPadding)

const defaultBitSize = 64 // 32 bits for latitude, another 32 bits for longitude

// return: geohash, box
func encode0(latitude, longitude float64, bitSize uint) ([]byte, [2][2]float64) {
	box := [2][2]float64{
		{-180, 180}, // lng
		{-90, 90},   // lat
	}
	pos := [2]float64{longitude, latitude}
	hashLen := bitSize >> 3
	if bitSize&7 > 0 {
		hashLen++
	}
	hash := make([]byte, hashLen)
	var precision uint = 0
	for precision < bitSize {
		for direction, val := range pos {
			mid := (box[direction][0] + box[direction][1]) / 2
			if val < mid {
				box[direction][1] = mid
			} else {
				box[direction][0] = mid
				hash[precision>>3] |= 1 << (7 - precision&7)
			}
			precision++
			if precision == bitSize {
				break
			}
		}
	}

	return hash, box
}

// Encode converts latitude and longitude to uint64 geohash code
func Encode(latitude, longitude float64) uint64 {
	buf, _ := encode0(latitude, longitude, defaultBitSize)
	return binary.BigEndian.Uint64(buf)
}

func decode0(hash []byte) [][]float64 {
	box := [][]float64{
		{-180, 180},
		{-90, 90},
	}
	direction := 0
	for i := 0; i < len(hash); i++ {
		code := hash[i]
		for j := 0; j < len(bits); j++ {
			mid := (box[direction][0] + box[direction][1]) / 2
			mask := bits[j]
			if mask&code > 0 {
				box[direction][0] = mid
			} else {
				box[direction][1] = mid
			}
			direction = (direction + 1) % 2
		}
	}
	return box
}

// Decode converts uint64 geohash code to latitude and longitude
func Decode(code uint64) (float64, float64) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, code)
	box := decode0(buf)
	lng := float64(box[0][0]+box[0][1]) / 2
	lat := float64(box[1][0]+box[1][1]) / 2
	return lat, lng
}

// ToString converts bytes geohash code to base32 string
func ToString(buf []byte) string {
	return enc.EncodeToString(buf)
}

// ToInt converts bytes geohash code to uint64 code
func ToInt(buf []byte) uint64 {
	// padding
	if len(buf) < 8 {
		buf2 := make([]byte, 8)
		copy(buf2, buf)
		return binary.BigEndian.Uint64(buf2)
	}
	return binary.BigEndian.Uint64(buf)
}

// FromInt converts uint64 geohash code to bytes
func FromInt(code uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, code)
	return buf
}
