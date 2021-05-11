package utils

import (
	"errors"
	"io"
)

// LimitedReader implements io.Reader, but you can only read the given number of bytes
type LimitedReader struct {
	src   io.Reader
	n     int
	limit int
}

// NewLimitedReader wraps an io.Reader to LimitedReader
func NewLimitedReader(src io.Reader, limit int) *LimitedReader {
	return &LimitedReader{
		src:   src,
		limit: limit,
	}
}

// Read reads up to len(p) bytes into p. if meets EOF from src or reach limit, it returns EOF
func (r *LimitedReader) Read(p []byte) (n int, err error) {
	if r.src == nil {
		return 0, errors.New("no data source")
	}
	if r.limit > 0 && r.n >= r.limit {
		return 0, io.EOF
	}
	n, err = r.src.Read(p)
	if err != nil {
		return n, err
	}
	r.n += n
	return
}
