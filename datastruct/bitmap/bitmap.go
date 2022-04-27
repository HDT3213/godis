package bitmap

type BitMap []byte

func New() *BitMap {
	b := BitMap(make([]byte, 0))
	return &b
}

func toByteSize(bitSize int64) int64 {
	if bitSize%8 == 0 {
		return bitSize / 8
	}
	return bitSize/8 + 1
}

func (b *BitMap) grow(bitSize int64) {
	byteSize := toByteSize(bitSize)
	gap := byteSize - int64(len(*b))
	if gap <= 0 {
		return
	}
	*b = append(*b, make([]byte, gap)...)
}

func (b *BitMap) BitSize() int {
	return len(*b) * 8
}

func FromBytes(bytes []byte) *BitMap {
	bm := BitMap(bytes)
	return &bm
}

func (b *BitMap) ToBytes() []byte {
	return *b
}

func (b *BitMap) SetBit(offset int64, val byte) {
	byteIndex := offset / 8
	bitOffset := offset % 8
	mask := byte(1 << bitOffset)
	b.grow(offset + 1)
	if val > 0 {
		// set bit
		(*b)[byteIndex] |= mask
	} else {
		// clear bit
		(*b)[byteIndex] &^= mask
	}
}

func (b *BitMap) GetBit(offset int64) byte {
	byteIndex := offset / 8
	bitOffset := offset % 8
	if byteIndex >= int64(len(*b)) {
		return 0
	}
	return ((*b)[byteIndex] >> bitOffset) & 0x01
}

type Callback func(offset int64, val byte) bool

func (b *BitMap) ForEachBit(begin int64, end int64, cb Callback) {
	offset := begin
	byteIndex := offset / 8
	bitOffset := offset % 8
	for byteIndex < int64(len(*b)) {
		b := (*b)[byteIndex]
		for bitOffset < 8 {
			bit := byte(b >> bitOffset & 0x01)
			if !cb(offset, bit) {
				return
			}
			bitOffset++
			offset++
		}
		byteIndex++
		bitOffset = 0
		if end > 0 && offset == end {
			break
		}
	}
}

func (b *BitMap) ForEachByte(begin int, end int, cb Callback) {
	if end == 0 {
		end = len(*b)
	} else if end > len(*b) {
		end = len(*b)
	}
	for i := begin; i < end; i++ {
		if !cb(int64(i), (*b)[i]) {
			return
		}
	}
}
