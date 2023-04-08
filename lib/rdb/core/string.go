package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/hdt3213/rdb/lzf"
)

const (
	len6Bit      = 0
	len14Bit     = 1
	len32or64Bit = 2
	lenSpecial   = 3
	len32Bit     = 0x80
	len64Bit     = 0x81

	encodeInt8  = 0
	encodeInt16 = 1
	encodeInt32 = 2
	encodeLZF   = 3

	maxUint6  = 1<<6 - 1
	maxUint14 = 1<<14 - 1
	minInt24  = -1 << 23
	maxInt24  = 1<<23 - 1

	len14BitMask      byte = 0b01000000
	encodeInt8Prefix       = lenSpecial<<6 | encodeInt8
	encodeInt16Prefix      = lenSpecial<<6 | encodeInt16
	encodeInt32Prefix      = lenSpecial<<6 | encodeInt32
	encodeLZFPrefix        = lenSpecial<<6 | encodeLZF
)

// readLength parse Length Encoding
// see: https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format#length-encoding
func (dec *Decoder) readLength() (uint64, bool, error) {
	firstByte, err := dec.readByte()
	if err != nil {
		return 0, false, fmt.Errorf("read length failed: %v", err)
	}
	lenType := (firstByte & 0xc0) >> 6 // get first 2 bits
	var length uint64
	special := false
	switch lenType {
	case len6Bit:
		length = uint64(firstByte) & 0x3f
	case len14Bit:
		nextByte, err := dec.readByte()
		if err != nil {
			return 0, false, fmt.Errorf("read len14Bit failed: %v", err)
		}
		length = (uint64(firstByte)&0x3f)<<8 | uint64(nextByte)
	case len32or64Bit:
		if firstByte == len32Bit {
			err = dec.readFull(dec.buffer[0:4])
			if err != nil {
				return 0, false, fmt.Errorf("read len32Bit failed: %v", err)
			}
			length = uint64(binary.BigEndian.Uint32(dec.buffer))
		} else if firstByte == len64Bit {
			err = dec.readFull(dec.buffer)
			if err != nil {
				return 0, false, fmt.Errorf("read len64Bit failed: %v", err)
			}
			length = binary.BigEndian.Uint64(dec.buffer)
		} else {
			return 0, false, fmt.Errorf("illegal length encoding: %x", firstByte)
		}
	case lenSpecial:
		special = true
		length = uint64(firstByte) & 0x3f
	}
	return length, special, nil
}

func (dec *Decoder) readString() ([]byte, error) {
	length, special, err := dec.readLength()
	if err != nil {
		return nil, err
	}

	if special {
		switch length {
		case encodeInt8:
			b, err := dec.readByte()
			return []byte(strconv.Itoa(int(int8(b)))), err
		case encodeInt16:
			b, err := dec.readUint16()
			return []byte(strconv.Itoa(int(int16(b)))), err
		case encodeInt32:
			b, err := dec.readUint32()
			return []byte(strconv.Itoa(int(int32(b)))), err
		case encodeLZF:
			return dec.readLZF()
		default:
			return []byte{}, errors.New("Unknown string encode type ")
		}
	}

	res := make([]byte, length)
	err = dec.readFull(res)
	return res, err
}

func (dec *Decoder) readUint16() (uint16, error) {
	err := dec.readFull(dec.buffer[:2])
	if err != nil {
		return 0, fmt.Errorf("read uint16 error: %v", err)
	}

	i := binary.LittleEndian.Uint16(dec.buffer[:2])
	return i, nil
}

func (dec *Decoder) readUint32() (uint32, error) {
	err := dec.readFull(dec.buffer[:4])
	if err != nil {
		return 0, fmt.Errorf("read uint16 error: %v", err)
	}

	i := binary.LittleEndian.Uint32(dec.buffer[:4])
	return i, nil
}

func (dec *Decoder) readLiteralFloat() (float64, error) {
	first, err := dec.readByte()
	if err != nil {
		return 0, err
	}
	if first == 0xff {
		return math.Inf(-1), nil
	} else if first == 0xfe {
		return math.Inf(1), nil
	} else if first == 0xfd {
		return math.NaN(), nil
	}
	buf := make([]byte, first)
	err = dec.readFull(buf)
	if err != nil {
		return 0, err
	}
	str := unsafeBytes2Str(buf)
	val, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return 0, fmt.Errorf("")
	}
	return val, err
}

func (dec *Decoder) readFloat() (float64, error) {
	err := dec.readFull(dec.buffer)
	if err != nil {
		return 0, err
	}
	bits := binary.LittleEndian.Uint64(dec.buffer)
	return math.Float64frombits(bits), nil
}

func (dec *Decoder) readLZF() ([]byte, error) {
	inLen, _, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	outLen, _, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	val := make([]byte, inLen)
	err = dec.readFull(val)
	if err != nil {
		return nil, err
	}
	return lzf.Decompress(val, int(inLen), int(outLen))
}

func (enc *Encoder) writeLength(value uint64) error {
	var buf []byte
	if value <= maxUint6 {
		// 00 + 6 bits of data
		enc.buffer[0] = byte(value)
		buf = enc.buffer[0:1]
	} else if value <= maxUint14 {
		enc.buffer[0] = byte(value>>8) | len14BitMask // high 6 bit and mask(0x40)
		enc.buffer[1] = byte(value)                   // low 8 bit
		buf = enc.buffer[0:2]
	} else if value <= math.MaxUint32 {
		buf = make([]byte, 5)
		buf[0] = len32Bit
		binary.BigEndian.PutUint32(buf[1:], uint32(value))
	} else {
		buf = make([]byte, 9)
		buf[0] = len64Bit
		binary.BigEndian.PutUint64(buf[1:], value)
	}
	return enc.write(buf)
}

func (enc *Encoder) writeSimpleString(s string) error {
	err := enc.writeLength(uint64(len(s)))
	if err != nil {
		return err
	}
	return enc.write([]byte(s))
}

func (enc *Encoder) tryWriteIntString(s string) (bool, error) {
	intVal, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		// is not a integer
		return false, nil
	}
	if intVal >= math.MinInt8 && intVal <= math.MaxInt8 {
		err = enc.write([]byte{encodeInt8Prefix, byte(int8(intVal))})
	} else if intVal >= math.MinInt16 && intVal <= math.MaxInt16 {
		buf := enc.buffer[0:3]
		buf[0] = encodeInt16Prefix
		binary.LittleEndian.PutUint16(buf[1:], uint16(int16(intVal)))
		err = enc.write(buf)
	} else if intVal >= math.MinInt32 && intVal <= math.MaxInt32 {
		buf := enc.buffer[0:5]
		buf[0] = encodeInt32Prefix
		binary.LittleEndian.PutUint32(buf[1:], uint32(int32(intVal)))
		err = enc.write(buf)
	}
	if err != nil {
		return true, err
	}
	return true, nil
}

func (enc *Encoder) writeLZFString(s string) error {
	out, err := lzf.Compress([]byte(s))
	if err != nil {
		return err
	}
	err = enc.write([]byte{encodeLZFPrefix})
	if err != nil {
		return err
	}
	// write compressed length
	err = enc.writeLength(uint64(len(out)))
	if err != nil {
		return err
	}
	// write uncompressed length
	err = enc.writeLength(uint64(len(s)))
	if err != nil {
		return err
	}
	return enc.write(out)
}

func (enc *Encoder) writeString(s string) error {
	isInt, err := enc.tryWriteIntString(s)
	if err != nil {
		return err
	}
	if isInt {
		return nil
	}
	// Try LZF compression - under 20 bytes it's unable to compress even so skip it
	// see rdbSaveRawString at [rdb.c](https://github.com/redis/redis/blob/unstable/src/rdb.c#L449)
	if enc.compress && len(s) > 20 {
		err = enc.writeLZFString(s)
		if err == nil { // lzf may failed, while out > in
			return nil
		}
	}
	return enc.writeSimpleString(s)
}

// write string without try int string. for tryWriteIntSetEncoding, writeZipList
func (enc *Encoder) writeNanString(s string) error {
	if enc.compress && len(s) > 20 {
		err := enc.writeLZFString(s)
		if err == nil { // lzf may failed, while out > in
			return nil
		}
	}
	return enc.writeSimpleString(s)
}

func (enc *Encoder) WriteStringObject(key string, value []byte, options ...interface{}) error {
	err := enc.beforeWriteObject(options...)
	if err != nil {
		return err
	}
	err = enc.write([]byte{typeString})
	if err != nil {
		return err
	}
	err = enc.writeString(key)
	if err != nil {
		return err
	}
	err = enc.writeString(unsafeBytes2Str(value))
	if err != nil {
		return err
	}
	enc.state = writtenObjectState
	return nil
}

func (enc *Encoder) writeFloat64(f float64) error {
	bin := math.Float64bits(f)
	binary.LittleEndian.PutUint64(enc.buffer, bin)
	return enc.write(enc.buffer)
}
