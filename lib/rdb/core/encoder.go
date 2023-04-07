package core

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
)

// Encoder is used to generate RDB file
type Encoder struct {
	writer   io.Writer
	buffer   []byte
	crc      hash.Hash64
	existDB  map[uint]struct{} // store exist db size to avoid duplicate db
	compress bool
	state    string

	listZipListOpt  *zipListOpt
	hashZipListOpt  *zipListOpt
	zsetZipListOpt  *zipListOpt
	listZipListSize int
}

type zipListOpt struct {
	maxValue   int // if any value is larger than maxValue, abort zip list encoding
	maxEntries int // if number of entries is larger than maxEntries, abort list encoding
}

const (
	defaultZipListMaxValue   = 64
	defaultZipListMaxEntries = 512
)

func (zop *zipListOpt) getMaxValue() int {
	if zop == nil || zop.maxValue == 0 {
		return defaultZipListMaxValue
	}
	return zop.maxValue
}

func (zop *zipListOpt) getMaxEntries() int {
	if zop == nil || zop.maxEntries == 0 {
		return defaultZipListMaxEntries
	}
	return zop.maxEntries
}

const (
	startState           = "Start"
	writtenHeaderState   = "WrittenHeader"
	writtenDBHeaderState = "writtenHeader"
	writtenAuxState      = "WrittenAux"
	writtenTTLState      = "WrittenTTL"
	writtenObjectState   = "WrittenObject"
	writtenEndState      = "WritingEnd"
)

var placeholder = struct{}{}

var stateChanges = map[string]map[string]struct{}{ // state -> allow next states
	startState: {
		writtenHeaderState: placeholder,
	},
	writtenHeaderState: {
		writtenAuxState:      placeholder,
		writtenDBHeaderState: placeholder,
		writtenEndState:      placeholder,
	},
	writtenAuxState: {
		writtenAuxState:      placeholder,
		writtenDBHeaderState: placeholder,
		writtenEndState:      placeholder,
	},
	writtenDBHeaderState: { // do not allow empty db
		writtenTTLState:    placeholder,
		writtenObjectState: placeholder,
	},
	writtenTTLState: {
		writtenObjectState: placeholder,
	},
	writtenObjectState: {
		writtenTTLState:      placeholder,
		writtenObjectState:   placeholder,
		writtenDBHeaderState: placeholder, // start another db
		writtenEndState:      placeholder,
	},
	writtenEndState: {},
}

// NewEncoder creates an encoder instance
func NewEncoder(writer io.Writer) *Encoder {
	crcTab := crc64.MakeTable(crc64.ISO)
	return &Encoder{
		writer:          writer,
		crc:             crc64.New(crcTab),
		buffer:          make([]byte, 8),
		state:           startState,
		existDB:         make(map[uint]struct{}),
		listZipListSize: 4 * 1024,
	}
}

// SetListZipListOpt sets list-max-ziplist-value and list-max-ziplist-entries
func (enc *Encoder) SetListZipListOpt(maxValue, maxEntries int) *Encoder {
	enc.listZipListOpt = &zipListOpt{
		maxValue:   maxValue,
		maxEntries: maxEntries,
	}
	return enc
}

// SetHashZipListOpt sets hash-max-ziplist-value and hash-max-ziplist-entries
func (enc *Encoder) SetHashZipListOpt(maxValue, maxEntries int) *Encoder {
	enc.hashZipListOpt = &zipListOpt{
		maxValue:   maxValue,
		maxEntries: maxEntries,
	}
	return enc
}

// SetZSetZipListOpt sets zset-max-ziplist-value and zset-max-ziplist-entries
func (enc *Encoder) SetZSetZipListOpt(maxValue, maxEntries int) *Encoder {
	enc.zsetZipListOpt = &zipListOpt{
		maxValue:   maxValue,
		maxEntries: maxEntries,
	}
	return enc
}

// remain unfixed bugs, don't open
func (enc *Encoder) EnableCompress() *Encoder {
	enc.compress = true
	return enc
}

func (enc *Encoder) write(p []byte) error {
	_, err := enc.writer.Write(p)
	if err != nil {
		return fmt.Errorf("write data failed: %v", err)
	}
	_, err = enc.crc.Write(p)
	if err != nil {
		return fmt.Errorf("update crc table failed: %v", err)
	}
	return nil
}

var rdbHeader = []byte("REDIS0003")

func (enc *Encoder) validateStateChange(toState string) bool {
	_, ok := stateChanges[enc.state][toState]
	return ok
}

func (enc *Encoder) WriteHeader() error {
	if !enc.validateStateChange(writtenHeaderState) {
		return fmt.Errorf("cannot writing header at state: %s", enc.state)
	}
	err := enc.write(rdbHeader)
	if err != nil {
		return err
	}
	enc.state = writtenHeaderState
	return nil
}

// WriteAux writes aux object
func (enc *Encoder) WriteAux(key, value string) error {
	if !enc.validateStateChange(writtenAuxState) {
		return fmt.Errorf("cannot writing aux at state: %s", enc.state)
	}
	err := enc.write([]byte{opCodeAux})
	if err != nil {
		return err
	}
	err = enc.writeString(key)
	if err != nil {
		return err
	}
	err = enc.writeString(value)
	if err != nil {
		return err
	}
	enc.state = writtenAuxState
	return nil
}

// WriteDBHeader write db index and resize db into rdb file
func (enc *Encoder) WriteDBHeader(dbIndex uint, keyCount, ttlCount uint64) error {
	if !enc.validateStateChange(writtenDBHeaderState) {
		return fmt.Errorf("cannot writing db header at state: %s", enc.state)
	}
	if _, ok := enc.existDB[dbIndex]; ok {
		return fmt.Errorf("db %d existed", dbIndex)
	}
	enc.existDB[dbIndex] = struct{}{}
	err := enc.write([]byte{opCodeSelectDB})
	if err != nil {
		return err
	}
	err = enc.writeLength(uint64(dbIndex))
	if err != nil {
		return err
	}
	err = enc.write([]byte{opCodeResizeDB})
	if err != nil {
		return err
	}
	err = enc.writeLength(keyCount)
	if err != nil {
		return err
	}
	err = enc.writeLength(ttlCount)
	if err != nil {
		return err
	}
	enc.state = writtenDBHeaderState
	return nil
}

// WriteEnd writes EOF and crc sum
func (enc *Encoder) WriteEnd() error {
	if !enc.validateStateChange(writtenEndState) {
		return fmt.Errorf("cannot writing end at state: %s", enc.state)
	}
	err := enc.write([]byte{opCodeEOF})
	if err != nil {
		return err
	}
	checkSum := enc.crc.Sum(nil)
	_, err = enc.writer.Write(checkSum)
	if err != nil {
		return fmt.Errorf("write crc sum failed: %v", err)
	}
	enc.writer.Write([]byte{0x0a}) // write LF
	enc.state = writtenEndState
	return nil
}

func (enc *Encoder) writeTTL(expiration uint64) error {
	if !enc.validateStateChange(writtenTTLState) {
		return fmt.Errorf("cannot write string object at state: %s", enc.state)
	}
	err := enc.write([]byte{opCodeExpireTimeMs})
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(enc.buffer, expiration)
	err = enc.write(enc.buffer)
	if err != nil {
		return err
	}
	enc.state = writtenTTLState
	return nil
}

// TTLOption specific expiration timestamp for object
type TTLOption uint64

// WithTTL specific expiration timestamp for object
func WithTTL(expirationMs uint64) TTLOption {
	return TTLOption(expirationMs)
}

func (enc *Encoder) beforeWriteObject(options ...interface{}) error {
	if !enc.validateStateChange(writtenObjectState) {
		return fmt.Errorf("cannot write object at state: %s", enc.state)
	}
	for _, opt := range options {
		switch o := opt.(type) {
		case TTLOption:
			err := enc.writeTTL(uint64(o))
			if err != nil {
				return err
			}
		}
	}
	return nil
}
