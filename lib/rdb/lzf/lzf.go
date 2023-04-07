package lzf

import "errors"

const (
	htabLog  uint32 = 14
	htabSize uint32 = 1 << htabLog
	maxLit          = 1 << 5
	maxOff          = 1 << 13
	maxRef          = (1 << 8) + (1 << 3)
)

var (
	errInsufficientBuffer = errors.New("insufficient buffer")
	errDataCorruption     = errors.New("data corruption")
)

// using https://github.com/zhuyie/golzf according to MIT license
// Decompress decompress lzf compressed data
func Decompress(input []byte, inLen int, outLen int) ([]byte, error) {
	input = input[:inLen]
	output := make([]byte, outLen)
	var inputIndex, outputIndex int

	inputLength := len(input)
	outputLength := len(output)
	if inputLength == 0 {
		return nil, nil
	}

	for inputIndex < inputLength {
		ctrl := int(input[inputIndex])
		inputIndex++

		if ctrl < (1 << 5) { /* literal run */
			ctrl++

			if outputIndex+ctrl > outputLength {
				return nil, errInsufficientBuffer
			}

			if inputIndex+ctrl > inputLength {
				return nil, errDataCorruption
			}

			copy(output[outputIndex:outputIndex+ctrl], input[inputIndex:inputIndex+ctrl])
			inputIndex += ctrl
			outputIndex += ctrl

		} else { /* back reference */
			length := ctrl >> 5
			ref := outputIndex - ((ctrl & 0x1f) << 8) - 1

			if inputIndex >= inputLength {
				return nil, errDataCorruption
			}

			if length == 7 {
				length += int(input[inputIndex])
				inputIndex++

				if inputIndex >= inputLength {
					return nil, errDataCorruption
				}
			}

			ref -= int(input[inputIndex])
			inputIndex++

			if outputIndex+length+2 > outputLength {
				return nil, errDataCorruption
			}

			if ref < 0 {
				return nil, errDataCorruption
			}

			// Can't use copy(...) here, because it has special handling when source and destination overlap.
			for i := 0; i < length+2; i++ {
				output[outputIndex+i] = output[ref+i]
			}
			outputIndex += length + 2
		}
	}

	return output[:outputIndex], nil
}

// Compress compress data using lzf algorithm
func Compress(input []byte) ([]byte, error) {
	var hval, ref, hslot, off uint32
	var inputIndex, outputIndex, lit int
	output := make([]byte, len(input))

	htab := make([]uint32, htabSize)
	inputLength := len(input)
	if inputLength == 0 {
		return nil, nil
	}
	outputLength := len(output)
	if outputLength == 0 {
		return nil, errInsufficientBuffer
	}

	lit = 0 /* start run */
	outputIndex++

	hval = uint32(input[inputIndex])<<8 | uint32(input[inputIndex+1])
	for inputIndex < inputLength-2 {
		hval = (hval << 8) | uint32(input[inputIndex+2])
		hslot = ((hval >> (3*8 - htabLog)) - hval*5) & (htabSize - 1)
		ref = htab[hslot]
		htab[hslot] = uint32(inputIndex)
		off = uint32(inputIndex) - ref - 1

		if off < maxOff &&
			(ref > 0) &&
			(input[ref] == input[inputIndex]) &&
			(input[ref+1] == input[inputIndex+1]) &&
			(input[ref+2] == input[inputIndex+2]) {

			/* match found at *ref++ */
			len := 2
			maxLen := inputLength - inputIndex - len
			if maxLen > maxRef {
				maxLen = maxRef
			}

			if outputIndex+3+1 >= outputLength { /* first a faster conservative test */
				nlit := 0
				if lit == 0 {
					nlit = 1
				}
				if outputIndex-nlit+3+1 >= outputLength { /* second the exact but rare test */
					return nil, errInsufficientBuffer
				}
			}

			output[outputIndex-lit-1] = byte(lit - 1) /* stop run */
			if lit == 0 {
				outputIndex-- /* undo run if length is zero */
			}

			for {
				len++
				if (len >= maxLen) || (input[int(ref)+len] != input[inputIndex+len]) {
					break
				}
			}

			len -= 2 /* len is now #octets - 1 */
			inputIndex++

			if len < 7 {
				output[outputIndex] = byte((off >> 8) + uint32(len<<5))
				outputIndex++
			} else {
				output[outputIndex] = byte((off >> 8) + (7 << 5))
				output[outputIndex+1] = byte(len - 7)
				outputIndex += 2
			}

			output[outputIndex] = byte(off)
			outputIndex += 2
			lit = 0 /* start run */

			inputIndex += len + 1

			if inputIndex >= inputLength-2 {
				break
			}

			inputIndex -= 2

			hval = uint32(input[inputIndex])<<8 | uint32(input[inputIndex+1])
			hval = (hval << 8) | uint32(input[inputIndex+2])
			hslot = ((hval >> (3*8 - htabLog)) - (hval * 5)) & (htabSize - 1)
			htab[hslot] = uint32(inputIndex)
			inputIndex++

			hval = (hval << 8) | uint32(input[inputIndex+2])
			hslot = ((hval >> (3*8 - htabLog)) - (hval * 5)) & (htabSize - 1)
			htab[hslot] = uint32(inputIndex)
			inputIndex++

		} else {
			/* one more literal byte we must copy */
			if outputIndex >= outputLength {
				return nil, errInsufficientBuffer
			}

			lit++
			output[outputIndex] = input[inputIndex]
			outputIndex++
			inputIndex++

			if lit == maxLit {
				output[outputIndex-lit-1] = byte(lit - 1) /* stop run */
				lit = 0                                   /* start run */
				outputIndex++
			}
		}
	}

	if outputIndex+3 >= outputLength { /* at most 3 bytes can be missing here */
		return nil, errInsufficientBuffer
	}

	for inputIndex < inputLength {
		lit++
		output[outputIndex] = input[inputIndex]
		outputIndex++
		inputIndex++

		if lit == maxLit {
			output[outputIndex-lit-1] = byte(lit - 1) /* stop run */
			lit = 0                                   /* start run */
			outputIndex++
		}
	}

	output[outputIndex-lit-1] = byte(lit - 1) /* end run */
	if lit == 0 {                             /* undo run if length is zero */
		outputIndex--
	}

	return output[:outputIndex], nil
}
