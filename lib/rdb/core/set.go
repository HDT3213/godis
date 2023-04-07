package core

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strconv"
)

func (dec *Decoder) readSet() ([][]byte, error) {
	size64, _, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	size := int(size64)
	values := make([][]byte, 0, size)
	for i := 0; i < size; i++ {
		val, err := dec.readString()
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

func (dec *Decoder) readIntSet() (result [][]byte, err error) {
	var buf []byte
	buf, err = dec.readString()
	if err != nil {
		return nil, err
	}
	sizeBytes := buf[0:4]
	intSize := int(binary.LittleEndian.Uint32(sizeBytes))
	if intSize != 2 && intSize != 4 && intSize != 8 {
		return nil, fmt.Errorf("unknown intset encoding: %d", intSize)
	}
	lenBytes := buf[4:8]
	cardinality := binary.LittleEndian.Uint32(lenBytes)
	cursor := 8
	result = make([][]byte, 0, cardinality)
	for i := uint32(0); i < cardinality; i++ {
		var intBytes []byte
		intBytes, err = readBytes(buf, &cursor, intSize)
		if err != nil {
			return
		}
		var intString string
		switch intSize {
		case 2:
			intString = strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)
		case 4:
			intString = strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)
		case 8:
			intString = strconv.FormatInt(int64(int64(binary.LittleEndian.Uint64(intBytes))), 10)
		}
		result = append(result, []byte(intString))
	}
	return
}

func (enc *Encoder) WriteSetObject(key string, values [][]byte, options ...interface{}) error {
	err := enc.beforeWriteObject(options...)
	if err != nil {
		return err
	}
	ok, err := enc.tryWriteIntSetEncoding(key, values)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	err = enc.writeSetEncoding(key, values)
	if err != nil {
		return err
	}
	enc.state = writtenObjectState
	return nil
}

func (enc *Encoder) writeSetEncoding(key string, values [][]byte) error {
	err := enc.write([]byte{typeSet})
	if err != nil {
		return err
	}
	err = enc.writeString(key)
	if err != nil {
		return err
	}
	err = enc.writeLength(uint64(len(values)))
	if err != nil {
		return err
	}
	for _, value := range values {
		err = enc.writeString(unsafeBytes2Str(value))
		if err != nil {
			return err
		}
	}
	return nil
}

func (enc *Encoder) tryWriteIntSetEncoding(key string, values [][]byte) (bool, error) {
	max := int64(math.MinInt64)
	min := int64(math.MaxInt64)
	intList := make([]int64, len(values))
	for i, v := range values {
		str := unsafeBytes2Str(v)
		intV, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return false, nil
		}
		if intV < min {
			min = intV
		} else if intV > max {
			max = intV
		}
		intList[i] = intV
	}
	intSize := uint32(8)
	if min >= math.MinInt16 && max <= math.MaxInt16 {
		intSize = 2
	} else if min >= math.MinInt32 && max <= math.MaxInt32 {
		intSize = 4
	}
	sort.Slice(intList, func(i, j int) bool {
		return intList[i] < intList[j]
	})

	err := enc.write([]byte{typeSetIntSet})
	if err != nil {
		return true, err
	}
	err = enc.writeString(key)
	if err != nil {
		return true, err
	}
	buf := make([]byte, 8, 8+int(intSize)*len(values))
	binary.LittleEndian.PutUint32(buf[0:4], intSize)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(values)))

	for _, value := range intList {
		switch intSize {
		case 2:
			binary.LittleEndian.PutUint16(enc.buffer[0:2], uint16(value))
			buf = append(buf, enc.buffer[0:2]...)
		case 4:
			binary.LittleEndian.PutUint32(enc.buffer[0:4], uint32(value))
			buf = append(buf, enc.buffer[0:4]...)
		case 8:
			binary.LittleEndian.PutUint64(enc.buffer, uint64(value))
			buf = append(buf, enc.buffer...)
		}
	}
	err = enc.writeNanString(unsafeBytes2Str(buf))
	if err != nil {
		return true, err
	}
	return true, nil
}
