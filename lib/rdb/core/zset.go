package core

import (
	"strconv"

	"github.com/hdt3213/godis/lib/rdb/model"
)

func (dec *Decoder) readZSet(zset2 bool) ([]*model.ZSetEntry, error) {
	length, _, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	entries := make([]*model.ZSetEntry, 0, int(length))
	for i := uint64(0); i < length; i++ {
		member, err := dec.readString()
		if err != nil {
			return nil, err
		}
		var score float64
		if zset2 {
			score, err = dec.readFloat()
		} else {
			score, err = dec.readLiteralFloat()
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, &model.ZSetEntry{
			Member: unsafeBytes2Str(member),
			Score:  score,
		})
	}
	return entries, nil
}

func (dec *Decoder) readZipListZSet() ([]*model.ZSetEntry, error) {
	buf, err := dec.readString()
	if err != nil {
		return nil, err
	}
	cursor := 0
	size := readZipListLength(buf, &cursor)
	entries := make([]*model.ZSetEntry, 0, size)
	for i := 0; i < size; i += 2 {
		member, err := dec.readZipListEntry(buf, &cursor)
		if err != nil {
			return nil, err
		}
		scoreLiteral, err := dec.readZipListEntry(buf, &cursor)
		if err != nil {
			return nil, err
		}
		score, err := strconv.ParseFloat(unsafeBytes2Str(scoreLiteral), 64)
		if err != nil {
			return nil, err
		}
		entries = append(entries, &model.ZSetEntry{
			Member: unsafeBytes2Str(member),
			Score:  score,
		})
	}
	return entries, nil
}

func (dec *Decoder) readListPackZSet() ([]*model.ZSetEntry, error) {
	buf, err := dec.readString()
	if err != nil {
		return nil, err
	}
	cursor := 0
	size := readListPackLength(buf, &cursor)
	entries := make([]*model.ZSetEntry, 0, size)
	for i := 0; i < size; i += 2 {
		member, err := dec.readListPackEntry(buf, &cursor)
		if err != nil {
			return nil, err
		}
		scoreLiteral, err := dec.readListPackEntry(buf, &cursor)
		if err != nil {
			return nil, err
		}
		score, err := strconv.ParseFloat(unsafeBytes2Str(scoreLiteral), 64)
		if err != nil {
			return nil, err
		}
		entries = append(entries, &model.ZSetEntry{
			Member: unsafeBytes2Str(member),
			Score:  score,
		})
	}
	return entries, nil
}

func (enc *Encoder) WriteZSetObject(key string, entries []*model.ZSetEntry, options ...interface{}) error {
	err := enc.beforeWriteObject(options...)
	if err != nil {
		return err
	}
	ok, err := enc.tryWriteZipListZSet(key, entries)
	if err != nil {
		return err
	}
	if !ok {
		err = enc.writeZSet2Encoding(key, entries)
		if err != nil {
			return err
		}
	}
	enc.state = writtenObjectState
	return nil
}

func (enc *Encoder) writeZSet2Encoding(key string, entries []*model.ZSetEntry) error {
	err := enc.write([]byte{typeZset2})
	if err != nil {
		return err
	}
	err = enc.writeString(key)
	if err != nil {
		return err
	}
	err = enc.writeLength(uint64(len(entries)))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		err = enc.writeString(entry.Member)
		if err != nil {
			return err
		}
		err = enc.writeFloat64(entry.Score)
		if err != nil {
			return err
		}
	}
	return nil
}

func (enc *Encoder) tryWriteZipListZSet(key string, entries []*model.ZSetEntry) (bool, error) {
	if len(entries) > enc.zsetZipListOpt.getMaxEntries() {
		return false, nil
	}
	maxValue := enc.zsetZipListOpt.getMaxValue()
	for _, entry := range entries {
		if len(entry.Member) > maxValue {
			return false, nil
		}
	}
	err := enc.write([]byte{typeZsetZipList})
	if err != nil {
		return true, err
	}
	err = enc.writeString(key)
	if err != nil {
		return true, err
	}
	zlElements := make([]string, 0, len(entries)*2)
	for _, entry := range entries {
		scoreStr := strconv.FormatFloat(entry.Score, 'f', -1, 64)
		zlElements = append(zlElements, entry.Member, scoreStr)
	}
	err = enc.writeZipList(zlElements)
	if err != nil {
		return true, err
	}
	return true, nil
}
