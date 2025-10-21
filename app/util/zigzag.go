package util

import "encoding/binary"

func ZigzagDecodeInt8(b []byte) int8 {
	u, _ := binary.Uvarint(b)
	v := int64((u >> 1) ^ -(u & 1))
	if v < -128 || v > 127 {
		panic("int8 overflow")
	}
	return int8(v)
}
