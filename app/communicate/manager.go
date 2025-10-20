package communicate

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

const (
	APIType_APIVersions             uint16 = 18
	APIType_DescribeTopicPartitions uint16 = 75
)

type ConnManager struct {
	conn net.Conn
}

func NewConnManager(conn net.Conn) *ConnManager {
	return &ConnManager{
		conn: conn,
	}
}

type APICommonRequestHeader struct {
	RequestAPIKey     uint16
	RequestAPIVersion uint16
	CorrelationID     uint32
	ClientID          *RequestHeaderClientID
}

type RequestHeaderClientID struct {
	Length   uint16
	Contents []byte
}

func (m *ConnManager) readLength(readLen int, reader io.Reader) []byte {
	b := make([]byte, readLen)
	n, err := reader.Read(b)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if n != readLen {
		panic(fmt.Sprintf("read want:%d, has:%d", readLen, n))
	}

	return b
}
func (m *ConnManager) readMessageSize() uint32 {
	return binary.BigEndian.Uint32(m.readLength(4, m.conn))
}

func (m *ConnManager) readRequestAPIKey(reader io.Reader) uint16 {
	return binary.BigEndian.Uint16(m.readLength(2, reader))
}

func (m *ConnManager) readRequestAPIVersion(reader io.Reader) uint16 {
	return binary.BigEndian.Uint16(m.readLength(2, reader))
}

func (m *ConnManager) readCorrelationID(reader io.Reader) uint32 {
	return binary.BigEndian.Uint32(m.readLength(4, reader))
}

func (m *ConnManager) readRequestHeaderClientID(reader io.Reader) *RequestHeaderClientID {
	length := binary.BigEndian.Uint16(m.readLength(2, reader))
	content := m.readLength(int(length), reader)
	return &RequestHeaderClientID{
		Length:   length,
		Contents: content,
	}
}

func (m *ConnManager) readTagBuffer(reader io.Reader) {
	m.readLength(1, reader)
}

type RequestMetaInfo struct {
	MessageSize    uint32
	Header         *APICommonRequestHeader
	BodyDataSource io.Reader
}

type ResponseHeader struct {
	CorrelationID uint32
}

func (m *ConnManager) ParseRequestMetaInfo() *RequestMetaInfo {
	messageSize := m.readMessageSize()

	buffer := bytes.NewBuffer(m.readLength(int(messageSize), m.conn))

	requestAPIKey := m.readRequestAPIKey(buffer)
	requestAPIVersion := m.readRequestAPIVersion(buffer)
	correlationID := m.readCorrelationID(buffer)
	cid := m.readRequestHeaderClientID(buffer)
	m.readTagBuffer(buffer)

	return &RequestMetaInfo{
		MessageSize: messageSize,
		Header: &APICommonRequestHeader{
			RequestAPIKey:     requestAPIKey,
			RequestAPIVersion: requestAPIVersion,
			CorrelationID:     correlationID,
			ClientID:          cid,
		},
		BodyDataSource: buffer,
	}
}

func (m *ConnManager) BuildConnHandler(rm *RequestMetaInfo) APIHandler {
	switch rm.Header.RequestAPIKey {
	case APIType_APIVersions:
		return NewAPIVersionHandler(m.conn)
	case APIType_DescribeTopicPartitions:
		return &todoHandler{}
	default:
		return &todoHandler{}
	}
}

func HandleConn(conn net.Conn) {
	m := NewConnManager(conn)
	for {
		rm := m.ParseRequestMetaInfo()
		h := m.BuildConnHandler(rm)
		h.HandleAPIEvent(rm, conn)
	}
}
