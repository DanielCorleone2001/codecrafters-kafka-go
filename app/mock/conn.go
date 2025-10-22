package mock

import (
	"bufio"
	"io"
	"net"
	"time"
)

var _ net.Conn = &fakeConn{}

func NewFakeConn(reader io.Reader, writer io.Writer) net.Conn {
	return &fakeConn{rw: bufio.NewReadWriter(bufio.NewReader(reader), bufio.NewWriter(writer))}
}

type fakeConn struct {
	rw *bufio.ReadWriter
}

func (f *fakeConn) Read(b []byte) (n int, err error) {
	return f.rw.Read(b)
}

func (f *fakeConn) Write(b []byte) (n int, err error) {
	n, err = f.rw.Write(b)
	if err != nil {
		return n, err
	}
	err = f.rw.Flush()
	return n, err
}

func (f *fakeConn) Close() error { return nil }

func (f *fakeConn) LocalAddr() net.Addr { return nil }

func (f *fakeConn) RemoteAddr() net.Addr { return nil }

func (f *fakeConn) SetDeadline(t time.Time) error { return nil }

func (f *fakeConn) SetReadDeadline(t time.Time) error { return nil }

func (f *fakeConn) SetWriteDeadline(t time.Time) error { return nil }
