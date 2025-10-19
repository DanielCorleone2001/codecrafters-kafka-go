package communicate

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type connManager struct {
	conn net.Conn
}

func ConnManager(conn net.Conn) *connManager {
	return &connManager{
		conn: conn,
	}
}

func (m *connManager) Close() error {
	return m.conn.Close()
}

type requestMessage struct {
	MessageSize       uint32
	RequestAPIKey     uint16
	RequestAPIVersion uint16
	CorrelationID     uint32
	ClientID          string

	buf *bytes.Buffer
}

func (m *connManager) readMessageSize() uint32 {
	return binary.BigEndian.Uint32(m.readLength(4, m.conn))
}

func (m *connManager) readLength(readLen int, reader io.Reader) []byte {
	b := make([]byte, readLen)
	n, err := reader.Read(b)
	if err != nil {
		panic("read fail,err:" + err.Error())
	}
	if n != readLen {
		panic(fmt.Sprintf("read want:%d, has:%d", readLen, n))
	}

	return b
}

func (m *connManager) readRequestAPIKey(reader io.Reader) uint16 {
	return binary.BigEndian.Uint16(m.readLength(2, reader))
}

func (m *connManager) readRequestAPIVersion(reader io.Reader) uint16 {
	return binary.BigEndian.Uint16(m.readLength(2, reader))
}

func (m *connManager) readCorrelationID(reader io.Reader) uint32 {
	return binary.BigEndian.Uint32(m.readLength(4, reader))
}

func (m *connManager) readRequest() *requestMessage {
	messageSize := m.readMessageSize()
	buffer := bytes.NewBuffer(m.readLength(int(messageSize), m.conn))
	requestAPIKey := m.readRequestAPIKey(buffer)
	requestAPIVersion := m.readRequestAPIVersion(buffer)
	correlationID := m.readCorrelationID(buffer)

	return &requestMessage{
		RequestAPIKey:     requestAPIKey,
		RequestAPIVersion: requestAPIVersion,
		CorrelationID:     correlationID,
		ClientID:          "",
		buf:               buffer,
	}
}

type responseMessage struct {
	MessageSize   uint32
	CorrelationID uint32
}

func (m *connManager) writeResponseMessage(resp *responseMessage) {
	m.writeMessageSize(resp)
	m.writeCorrelationID(resp)
}

func (m *connManager) writeBytes(b []byte) {
	n, err := m.conn.Write(b)
	if err != nil {
		panic(err)
	}
	if n != len(b) {
		panic(fmt.Sprintf("write want:%d, actual:%d", len(b), n))
	}
}

func (m *connManager) writeMessageSize(resp *responseMessage) {
	m.writeBytes(binary.BigEndian.AppendUint32([]byte{}, resp.MessageSize))
}

func (m *connManager) writeCorrelationID(resp *responseMessage) {
	m.writeBytes(binary.BigEndian.AppendUint32([]byte{}, resp.CorrelationID))
}
func (m *connManager) onHandle() {
	req := m.readRequest()

	resp := &responseMessage{MessageSize: 0, CorrelationID: req.CorrelationID}
	m.writeResponseMessage(resp)
}
