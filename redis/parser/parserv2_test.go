package parser

import (
	"bytes"
	"fmt"
	"testing"
)

func BenchmarkParseSETCommand(b *testing.B) {
	valueSizes := []int{10, 100, 1000, 10000}

	for _, size := range valueSizes {
		value := bytes.Repeat([]byte("a"), size)
		cmd := []byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$%d\r\n%s\r\n", len(value), value))

		b.Run("value_size_"+formatSize(size), func(subB *testing.B) {
			subB.ResetTimer()

			for i := 0; i < subB.N; i++ {
				reader := bytes.NewReader(cmd)
				_, err := ParseV2(reader)
				if err != nil {
					subB.Fatalf("parse failed: %v", err)
				}
			}
		})
	}
}

func TestParseV2(t *testing.T) {
	value := bytes.Repeat([]byte("a"), 100)
	data := []byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$%d\r\n%s\r\n", len(value), value))
	cmdLine, err := ParseV2(bytes.NewBuffer(data))
	if err != nil {
		t.Error(err)
		return
	}
	if len(cmdLine) != 3 || string(cmdLine[0]) != "SET" || string(cmdLine[1]) != "key" || string(cmdLine[2]) != string(value) {
		t.Error("parse error")
		return
	}
}

func formatSize(size int) string {
	units := []string{"B", "KB", "MB"}
	unitIndex := 0
	floatSize := float64(size)

	for floatSize >= 1024 && unitIndex < len(units)-1 {
		floatSize /= 1024
		unitIndex++
	}

	return fmt.Sprintf("%.0f%s", floatSize, units[unitIndex])
}
