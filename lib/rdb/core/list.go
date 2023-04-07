package core

import (
	"encoding/binary"
	"errors"
)

const (
	zipStr06B = 0
	zipStr14B = 1
	zipStr32B = 2

	zipInt04B = 0x0f        // high 4 bits of Int 04 encoding
	zipInt08B = 0xfe        // 11111110
	zipInt16B = 0xc0 | 0<<4 // 11000000
	zipInt24B = 0xc0 | 3<<4 // 11110000
	zipInt32B = 0xc0 | 1<<4 // 11010000
	zipInt64B = 0xc0 | 2<<4 //11100000

	zipBigPrevLen = 0xfe

	QuicklistNodeContainerPlain  = 1
	QuicklistNodeContainerPacked = 2
)

func (dec *Decoder) readList() ([][]byte, error) {
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

func (dec *Decoder) readQuickList() ([][]byte, error) {
	size, _, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	entries := make([][]byte, 0)
	for i := 0; i < int(size); i++ {
		page, err := dec.readZipList()
		if err != nil {
			return nil, err
		}
		entries = append(entries, page...)
	}
	return entries, nil
}

func (dec *Decoder) readQuickList2() ([][]byte, error) {
	size, _, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	entries := make([][]byte, 0)
	for i := 0; i < int(size); i++ {
		length, _, err := dec.readLength()
		if err != nil {
			return nil, err
		}
		if length == QuicklistNodeContainerPlain {
			entry, err := dec.readString()
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
		} else if length == QuicklistNodeContainerPacked {
			page, err := dec.readListPack()
			if err != nil {
				return nil, err
			}
			entries = append(entries, page...)
		} else {
			return nil, errors.New("unknown quicklist node type")
		}

	}
	return entries, nil
}

func (enc *Encoder) WriteListObject(key string, values [][]byte, options ...interface{}) error {
	err := enc.beforeWriteObject(options...)
	if err != nil {
		return err
	}
	ok, err := enc.tryWriteListZipList(key, values, options...)
	if err != nil {
		return err
	}
	if !ok {
		err = enc.writeQuickList(key, values, options...)
		if err != nil {
			return err
		}
	}
	enc.state = writtenObjectState
	return nil
}

func (enc *Encoder) tryWriteListZipList(key string, values [][]byte, options ...interface{}) (bool, error) {
	if len(values) > enc.listZipListOpt.getMaxEntries() {
		return false, nil
	}
	strList := make([]string, 0, len(values))
	maxValue := enc.listZipListOpt.getMaxValue()
	for _, v := range values {
		if len(v) > maxValue {
			return false, nil
		}
		strList = append(strList, unsafeBytes2Str(v))
	}
	err := enc.write([]byte{typeListZipList})
	if err != nil {
		return true, err
	}
	err = enc.writeString(key)
	if err != nil {
		return true, err
	}
	err = enc.writeZipList(strList)
	if err != nil {
		return true, err
	}
	return true, nil
}

func (enc *Encoder) writeQuickList(key string, values [][]byte, options ...interface{}) error {
	var pages [][]string
	pageSize := 0
	var curPage []string
	for _, value := range values {
		curPage = append(curPage, unsafeBytes2Str(value))
		pageSize += len(value)
		if pageSize >= enc.listZipListSize {
			pageSize = 0
			pages = append(pages, curPage)
			curPage = nil
		}
	}
	if len(curPage) > 0 {
		pages = append(pages, curPage)
	}
	err := enc.write([]byte{typeListQuickList})
	if err != nil {
		return err
	}
	err = enc.writeString(key)
	if err != nil {
		return err
	}
	err = enc.writeLength(uint64(len(pages)))
	if err != nil {
		return err
	}
	for _, page := range pages {
		err = enc.writeZipList(page)
		if err != nil {
			return err
		}
	}
	return nil
}

func (enc *Encoder) writeZipList(values []string) error {
	buf := make([]byte, 10) // reserve 10 bytes for zip list header
	zlBytes := 11           // header(10bytes) + zl end(1byte)
	zlTail := 10
	var prevLen uint32
	for i, value := range values {
		entry := encodeZipListEntry(prevLen, value)
		buf = append(buf, entry...)
		prevLen = uint32(len(entry))
		zlBytes += len(entry)
		if i < len(values)-1 {
			zlTail += len(entry)
		}
	}
	buf = append(buf, 0xff)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(zlBytes))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(zlTail))
	binary.LittleEndian.PutUint16(buf[8:10], uint16(len(values)))
	return enc.writeNanString(unsafeBytes2Str(buf))
}
