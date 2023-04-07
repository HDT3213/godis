package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
)

func (dec *Decoder) readListPack() ([][]byte, error) {
	buf, err := dec.readString()
	if err != nil {
		return nil, err
	}
	cursor := 0
	size := readListPackLength(buf, &cursor)
	entries := make([][]byte, 0, size)
	for i := 0; i < size; i++ {
		entry, err := dec.readListPackEntry(buf, &cursor)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func readListPackLength(buf []byte, cursor *int) int {
	start := *cursor + 4
	end := start + 2
	// list pack buf: [0, 4] -> total bytes, [4:6] -> entry count
	size := int(binary.LittleEndian.Uint16(buf[start:end]))
	*cursor += 6
	return size
}

func readVarInt(buf []byte, cursor *int) uint32 {
	var v uint32
	shift := 0
	for *cursor < len(buf) {
		x := buf[*cursor]
		*cursor++
		v |= uint32(x&0x7f) << shift
		shift += 7
		if x&0x80 == 0 {
			break
		}
	}
	return v
}

func (dec *Decoder) readListPackEntry(buf []byte, cursor *int) ([]byte, error) {
	header, err := readByte(buf, cursor)
	if err != nil {
		return nil, err
	}
	var result []byte
	switch header >> 6 {
	case 0, 1: // 0xxx xxxx -> uint7 [0, 127]
		result = []byte(strconv.FormatInt(int64(int8(header)), 10))
		readVarInt(buf, cursor) // read element length
		return result, nil
	case 2: // 10xx xxxx -> str, len<= 63
		length := int(header & 0x3f)
		result, err := readBytes(buf, cursor, length)
		if err != nil {
			return nil, err
		}
		readVarInt(buf, cursor) // read element length
		return result, nil
	}
	// assert header == 11xx xxxx
	switch header >> 4 {
	case 12, 13: // 110x xxxx -> int13
		// see https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/fba43c524524cbdb54955a28af228b513420d78d/src/listpack.c#L586
		next, err := readByte(buf, cursor)
		if err != nil {
			return nil, err
		}
		val := ((uint(header) & 0x1F) << 8) | uint(next)
		if val >= uint(1<<12) {
			val = -(8191 - val) - 1 // val is uint, must use -(8191 - val), val - 8191 will cause overflow
		}
		result = []byte(strconv.FormatInt(int64(val), 10))
		readVarInt(buf, cursor) // read element length
		return result, nil
	case 14: // 1110 xxxx -> str, type(len) == uint12
		dec.buffer[0] = header & 0x0f
		dec.buffer[1], err = readByte(buf, cursor)
		length := binary.LittleEndian.Uint32(dec.buffer[:2])
		result, err := readBytes(buf, cursor, int(length))
		if err != nil {
			return nil, err
		}
		readVarInt(buf, cursor) // read element length
		return result, nil
	}
	// assert header == 1111 xxxx
	switch header & 0x0f {
	case 0: // 1111 0000 -> str, 4 bytes len
		var lenBytes []byte
		lenBytes, err = readBytes(buf, cursor, 4)
		if err != nil {
			return nil, err
		}
		length := int(binary.BigEndian.Uint32(lenBytes))
		result, err := readBytes(buf, cursor, length)
		if err != nil {
			return nil, err
		}
		readVarInt(buf, cursor) // read element length
		return result, nil
	case 1: // 1111 0001 -> int16
		var bs []byte
		bs, err = readBytes(buf, cursor, 2)
		if err != nil {
			return nil, err
		}
		result = []byte(strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(bs))), 10))
		readVarInt(buf, cursor)
		return result, nil
	case 2: // 1111 0010 -> int24
		var bs []byte
		bs, err = readBytes(buf, cursor, 3)
		if err != nil {
			return nil, err
		}
		bs = append([]byte{0}, bs...)
		result = []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(bs))>>8), 10))
		readVarInt(buf, cursor)
		return result, nil
	case 3: // 1111 0011 -> int32
		var bs []byte
		bs, err = readBytes(buf, cursor, 4)
		if err != nil {
			return nil, err
		}
		result = []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(bs))), 10))
		readVarInt(buf, cursor)
		return result, nil
	case 4: // 1111 0100 -> int64
		var bs []byte
		bs, err = readBytes(buf, cursor, 8)
		if err != nil {
			return nil, err
		}
		result = []byte(strconv.FormatInt(int64(binary.LittleEndian.Uint64(bs)), 10))
		readVarInt(buf, cursor)
		return result, nil
	case 15: // 1111 1111 -> end
		return nil, errors.New("unexpected end")
	}
	return nil, fmt.Errorf("unknown entry header")
}
