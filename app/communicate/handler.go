package communicate

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

func HandleConn(conn net.Conn) {
	defer conn.Close()

	b := make([]byte, 1024)
	_, err := conn.Read(b)
	if err != nil {
		fmt.Println("read err,err:", err.Error())
		return
	}

	r := newResponseBuilder()
	r.writeMessageSize([]byte{0x00, 0x00, 0x00, 0x00})
	r.writeCorrelationID(defaultCorrelationID())

	_, err = conn.Write(r.dump())
	if err != nil {
		fmt.Println("write err,err:", err.Error())
	}
}

func newResponseBuilder() *responseBuilder {
	b := make([]byte, 256)
	return &responseBuilder{
		buf: bytes.NewBuffer(b),
	}
}

type responseBuilder struct {
	buf *bytes.Buffer
}

func (r *responseBuilder) writeMessageSize(ms messageSize) {
	r.buf.WriteByte(ms[0])
	r.buf.WriteByte(ms[1])
	r.buf.WriteByte(ms[2])
	r.buf.WriteByte(ms[3])
}

func (r *responseBuilder) writeCorrelationID(c correlationID) {
	r.buf.WriteByte(c[0])
	r.buf.WriteByte(c[1])
	r.buf.WriteByte(c[2])
	r.buf.WriteByte(c[3])
}

func (r *responseBuilder) dump() []byte {
	return r.buf.Bytes()
}

type messageSize []byte
type correlationID []byte

func defaultCorrelationID() correlationID {
	return binary.BigEndian.AppendUint32([]byte{}, 7)
}
