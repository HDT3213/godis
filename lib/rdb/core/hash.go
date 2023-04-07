package core

import (
	"encoding/binary"
	"errors"
)

/*
	if hlen <= ZIPMAP_VALUE_MAX_FREE
*/

func (dec *Decoder) readHashMap() (map[string][]byte, error) {
	size, _, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	m := make(map[string][]byte)
	for i := 0; i < int(size); i++ {
		field, err := dec.readString()
		if err != nil {
			return nil, err
		}
		value, err := dec.readString()
		if err != nil {
			return nil, err
		}
		m[unsafeBytes2Str(field)] = value
	}
	return m, nil
}

func (dec *Decoder) readZipMapHash() (map[string][]byte, error) {
	buf, err := dec.readString()
	if err != nil {
		return nil, err
	}
	cursor := 0
	bLen, err := readByte(buf, &cursor)
	if err != nil {
		return nil, err
	}
	length := int(bLen)
	if bLen > 254 {
		//todo: scan once
		cursor0 := cursor // record current cursor
		length, err = countZipMapEntries(buf, &cursor)
		if err != nil {
			return nil, err
		}
		length /= 2
		cursor = cursor0 // recover cursor at begin position of first zip map entry
	}
	m := make(map[string][]byte)
	for i := 0; i < length; i++ {
		fieldB, err := readZipMapEntry(buf, &cursor, false)
		if err != nil {
			return nil, err
		}
		field := unsafeBytes2Str(fieldB)
		value, err := readZipMapEntry(buf, &cursor, true)
		if err != nil {
			return nil, err
		}
		m[field] = value
	}
	return m, nil
}

// return: len, free, error
func readZipMapEntryLen(buf []byte, cursor *int, readFree bool) (int, int, error) {
	b, err := readByte(buf, cursor)
	if err != nil {
		return 0, 0, err
	}
	switch b {
	case 253:
		bs, err := readBytes(buf, cursor, 5)
		if err != nil {
			return 0, 0, err
		}
		length := int(binary.BigEndian.Uint32(bs))
		free := int(bs[4])
		return length, free, nil
	case 254:
		return 0, 0, errors.New("illegal zip map item length")
	case 255:
		return -1, 0, nil
	default:
		var free byte
		if readFree {
			free, err = readByte(buf, cursor)
		}
		return int(b), int(free), err
	}
}

func readZipMapEntry(buf []byte, cursor *int, readFree bool) ([]byte, error) {
	length, free, err := readZipMapEntryLen(buf, cursor, readFree)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	value, err := readBytes(buf, cursor, length)
	if err != nil {
		return nil, err
	}
	*cursor += free // skip free bytes
	return value, nil
}

func countZipMapEntries(buf []byte, cursor *int) (int, error) {
	n := 0
	for {
		readFree := n%2 != 0
		length, free, err := readZipMapEntryLen(buf, cursor, readFree)
		if err != nil {
			return 0, err
		}
		if length == -1 {
			break
		}
		*cursor += length + free
		n++
	}
	*cursor = 0 // reset cursor
	return n, nil
}

func (dec *Decoder) readZipListHash() (map[string][]byte, error) {
	buf, err := dec.readString()
	if err != nil {
		return nil, err
	}
	cursor := 0
	size := readZipListLength(buf, &cursor)
	m := make(map[string][]byte)
	for i := 0; i < size; i += 2 {
		key, err := dec.readZipListEntry(buf, &cursor)
		if err != nil {
			return nil, err
		}
		val, err := dec.readZipListEntry(buf, &cursor)
		if err != nil {
			return nil, err
		}
		m[unsafeBytes2Str(key)] = val
	}
	return m, nil
}

func (dec *Decoder) readListPackHash() (map[string][]byte, error) {
	buf, err := dec.readString()
	if err != nil {
		return nil, err
	}
	cursor := 0
	size := readListPackLength(buf, &cursor)
	m := make(map[string][]byte)
	for i := 0; i < size; i += 2 {
		key, err := dec.readListPackEntry(buf, &cursor)
		if err != nil {
			return nil, err
		}
		val, err := dec.readListPackEntry(buf, &cursor)
		if err != nil {
			return nil, err
		}
		m[unsafeBytes2Str(key)] = val
	}
	return m, nil
}

func (enc *Encoder) WriteHashMapObject(key string, hash map[string][]byte, options ...interface{}) error {
	err := enc.beforeWriteObject(options...)
	if err != nil {
		return err
	}
	ok, err := enc.tryWriteZipListHashMap(key, hash, options...)
	if err != nil {
		return err
	}
	if !ok {
		err = enc.writeHashEncoding(key, hash, options...)
		if err != nil {
			return err
		}
	}
	enc.state = writtenObjectState
	return nil
}

func (enc *Encoder) writeHashEncoding(key string, hash map[string][]byte, options ...interface{}) error {
	err := enc.write([]byte{typeHash})
	if err != nil {
		return err
	}
	err = enc.writeString(key)
	if err != nil {
		return err
	}
	err = enc.writeLength(uint64(len(hash)))
	if err != nil {
		return err
	}
	for field, value := range hash {
		err = enc.writeString(field)
		if err != nil {
			return err
		}
		err = enc.writeString(unsafeBytes2Str(value))
		if err != nil {
			return err
		}
	}
	return nil
}

func (enc *Encoder) tryWriteZipListHashMap(key string, hash map[string][]byte, options ...interface{}) (bool, error) {
	if len(hash) > enc.hashZipListOpt.getMaxEntries() {
		return false, nil
	}
	maxValue := enc.hashZipListOpt.getMaxValue()
	for _, v := range hash {
		if len(v) > maxValue {
			return false, nil
		}
	}
	err := enc.write([]byte{typeHashZipList})
	if err != nil {
		return true, err
	}
	err = enc.writeString(key)
	if err != nil {
		return true, err
	}
	entries := make([]string, 0, len(hash)*2)
	for k, v := range hash {
		entries = append(entries, k, unsafeBytes2Str(v))
	}
	err = enc.writeZipList(entries)
	if err != nil {
		return true, err
	}
	return true, nil
}
