package util

import (
	"errors"
	"io"
	"math"
)

var errVarintOverflow = errors.New("varint overflow")

func readUVarint(r io.Reader) (uint64, error) {
	var (
		value uint64
		shift uint
		buf   = make([]byte, 1)
	)

	for i := 0; i < 10; i++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}

		b := buf[0]
		value |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return value, nil
		}
		shift += 7
	}

	return 0, errVarintOverflow
}

func zigzagDecode(u uint64) int64 {
	return int64((u >> 1) ^ uint64(-(u & 1)))
}

func ReadVarint32(r io.Reader) int32 {
	u, err := readUVarint(r)
	if err != nil {
		panic(err)
	}

	v := zigzagDecode(u)
	if v < math.MinInt32 || v > math.MaxInt32 {
		panic(errVarintOverflow)
	}

	return int32(v)
}

func ReadVarint64(r io.Reader) int64 {
	u, err := readUVarint(r)
	if err != nil {
		panic(err)
	}

	v := zigzagDecode(u)
	if v < math.MinInt64 || v > math.MaxInt64 {
		panic(errVarintOverflow)
	}

	return v
}
