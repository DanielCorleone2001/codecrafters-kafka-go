package communicate

import (
	"fmt"
	"io"
	"os"
)

func ReadLength(readLen int, reader io.Reader) []byte {
	b := make([]byte, readLen)
	n, err := reader.Read(b)
	if err != nil { //todo?
		fmt.Println(err)
		os.Exit(1)
	}
	if n != readLen {
		panic(fmt.Sprintf("read want:%d, has:%d", readLen, n))
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
