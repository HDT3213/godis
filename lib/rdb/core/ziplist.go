package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
)

func (dec *Decoder) readZipList() ([][]byte, error) {
	buf, err := dec.readString()
	if err != nil {
		return nil, err
	}
	cursor := 0
	size := readZipListLength(buf, &cursor)
	entries := make([][]byte, 0, size)
	for i := 0; i < size; i++ {
		entry, err := dec.readZipListEntry(buf, &cursor)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (dec *Decoder) readZipListEntry(buf []byte, cursor *int) (result []byte, err error) {
	prevLen := buf[*cursor]
	*cursor++
	if prevLen == zipBigPrevLen {
		*cursor += 4
	}
	header := buf[*cursor]
	*cursor++
	typ := header >> 6
	switch typ {
	case zipStr06B:
		length := int(header & 0x3f)
		result, err = readBytes(buf, cursor, length)
		return
	case zipStr14B:
		b := buf[*cursor]
		*cursor++
		length := (int(header&0x3f) << 8) | int(b)
		result, err = readBytes(buf, cursor, length)
		return
	case zipStr32B:
		var lenBytes []byte
		lenBytes, err = readBytes(buf, cursor, 4)
		if err != nil {
			return
		}
		length := int(binary.BigEndian.Uint32(lenBytes))
		result, err = readBytes(buf, cursor, length)
		return
	}
	switch header {
	case zipInt08B:
		var b byte
		b, err = readByte(buf, cursor)
		if err != nil {
			return
		}
		result = []byte(strconv.FormatInt(int64(int8(b)), 10))
		return
	case zipInt16B:
		var bs []byte
		bs, err = readBytes(buf, cursor, 2)
		if err != nil {
			return
		}
		result = []byte(strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(bs))), 10))
		return
	case zipInt32B:
		var bs []byte
		bs, err = readBytes(buf, cursor, 4)
		if err != nil {
			return
		}
		result = []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(bs))), 10))
		return
	case zipInt64B:
		var bs []byte
		bs, err = readBytes(buf, cursor, 8)
		if err != nil {
			return
		}
		result = []byte(strconv.FormatInt(int64(binary.LittleEndian.Uint64(bs)), 10))
		return
	case zipInt24B:
		var bs []byte
		bs, err = readBytes(buf, cursor, 3)
		if err != nil {
			return
		}
		bs = append([]byte{0}, bs...)
		result = []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(bs))>>8), 10))
		return
	}
	if header>>4 == zipInt04B {
		result = []byte(strconv.FormatInt(int64(header&0x0f)-1, 10))
		return
	}
	return nil, fmt.Errorf("unknown entry header")
}

func encodeZipListEntry(prevLen uint32, val string) []byte {
	buf := bytes.NewBuffer(nil)
	// encode prevLen
	if prevLen < zipBigPrevLen {
		buf.Write([]byte{byte(prevLen)})
	} else {
		buf.Write([]byte{0xfe})
		buf0 := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf0, prevLen)
		buf.Write(buf0)
	}
	// try int encoding
	intVal, err := strconv.ParseInt(val, 10, 64)
	if err == nil {
		// use int encoding
		if intVal >= 0 && intVal <= 12 {
			buf.Write([]byte{0xf0 | byte(intVal+1)})
		} else if intVal >= math.MinInt8 && intVal <= math.MaxInt8 {
			// bytes.Buffer never failed
			buf.Write([]byte{byte(zipInt08B), byte(intVal)})
		} else if intVal >= minInt24 && intVal <= maxInt24 {
			buffer := make([]byte, 4)
			binary.LittleEndian.PutUint32(buffer, uint32(intVal))
			buf.Write([]byte{byte(zipInt24B)})
			buf.Write(buffer[0:3])
		} else if intVal >= math.MinInt32 && intVal <= math.MaxInt32 {
			buffer := make([]byte, 4)
			binary.LittleEndian.PutUint32(buffer, uint32(intVal))
			buf.Write([]byte{byte(zipInt32B)})
			buf.Write(buffer)
		} else {
			buffer := make([]byte, 8)
			binary.LittleEndian.PutUint64(buffer, uint64(intVal))
			buf.Write([]byte{byte(zipInt64B)})
			buf.Write(buffer)
		}
		return buf.Bytes()
	}
	// use string encoding
	if len(val) <= maxUint6 {
		buf.Write([]byte{byte(len(val))}) // 00 + xxxxxx
	} else if len(val) <= maxUint14 {
		buf.Write([]byte{byte(len(val)>>8) | len14BitMask, byte(len(val))})
	} else if len(val) <= math.MaxUint32 {
		buffer := make([]byte, 8)
		binary.LittleEndian.PutUint32(buffer, uint32(len(val)))
		buf.Write([]byte{0x80})
		buf.Write(buffer)
	} else {
		panic("too large string")
	}
	buf.Write([]byte(val))
	return buf.Bytes()
}
