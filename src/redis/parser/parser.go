package parser

func Parse(lines [][]byte)[][]byte {
    lineCount := len(lines) // must be even
    args := make([][]byte, lineCount / 2)
    for i := 0; i * 2 + 1 < lineCount; i++ {
        args[i] = lines[i * 2 + 1]
    }
    return args
}