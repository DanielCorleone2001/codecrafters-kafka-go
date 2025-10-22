package util

import (
	"fmt"
	"io"
	"os"
	"runtime/debug"
)

func ReadN(n int, reader io.Reader) []byte {
	b := make([]byte, n)
	read, err := reader.Read(b)
	if err != nil { //todo?
		fmt.Println(err)
		fmt.Println(string(debug.Stack()))
		os.Exit(1)
	}
	if n != read {
		panic(fmt.Sprintf("read want:%d, has:%d", n, read))
	}

	return b
}

func WriteBytes(b []byte, w io.Writer) {
	n, err := w.Write(b)
	if err != nil {
		panic(err)
	}
	if n != len(b) {
		panic(fmt.Sprintf("write want:%d, actual:%d", len(b), n))
	}
}
