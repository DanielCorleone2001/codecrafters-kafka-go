package api

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/codecrafters-io/kafka-starter-go/app/meta_data"
	"github.com/codecrafters-io/kafka-starter-go/app/util"
	"io"
	"net"
	"strings"
)

const (
	APIType_APIVersions             uint16 = 18
	APIType_DescribeTopicPartitions uint16 = 75
	APIType_Fetch                   uint16 = 1
)

type ConnManager struct {
	conn net.Conn
}

func NewConnManager(conn net.Conn) *ConnManager {
	return &ConnManager{
		conn: conn,
	}
}

func (r *CommonAPIRequestHeader) String() string {
	sb := &strings.Builder{}
	_, _ = fmt.Fprintf(sb, "api key:%d\n", r.RequestAPIKey)
	_, _ = fmt.Fprintf(sb, "api version:%d\n", r.RequestAPIVersion)
	_, _ = fmt.Fprintf(sb, "CorrelationID:%d\n", r.CorrelationID)
	_, _ = fmt.Fprintf(sb, "ClientID:%s\n", r.ClientID.Contents)
	return sb.String()
}

type CommonAPIRequestHeader struct {
	RequestAPIKey     uint16
	RequestAPIVersion uint16
	CorrelationID     uint32
	ClientID          *CommonAPIRequestHeaderClientID
}

func (r *CommonAPIRequestHeader) Equal(b *CommonAPIRequestHeader) bool {
	return r.RequestAPIKey == b.RequestAPIKey &&
		r.RequestAPIVersion == b.RequestAPIVersion &&
		r.CorrelationID == b.CorrelationID &&
		r.ClientID.Equal(b.ClientID)
}

type CommonAPIRequestHeaderClientID struct {
	Length   uint16
	Contents []byte
}

func (c *CommonAPIRequestHeaderClientID) Equal(b *CommonAPIRequestHeaderClientID) bool {
	return c.Length == b.Length && bytes.Equal(c.Contents, b.Contents)
}

func (c *CommonAPIRequestHeaderClientID) Decode(reader io.Reader) {
	c.Length = binary.BigEndian.Uint16(util.ReadN(2, reader))
	c.Contents = util.ReadN(int(c.Length), reader)
}

type RequestMetaInfo struct {
	MessageSize uint32
	Header      *CommonAPIRequestHeader
}

type ResponseHeader struct {
	CorrelationID uint32
}

func (r *CommonAPIRequestHeader) Decode(reader io.Reader) {
	r.RequestAPIKey = binary.BigEndian.Uint16(util.ReadN(2, reader))
	r.RequestAPIVersion = binary.BigEndian.Uint16(util.ReadN(2, reader))
	r.CorrelationID = binary.BigEndian.Uint32(util.ReadN(4, reader))

	cid := &CommonAPIRequestHeaderClientID{}
	cid.Decode(reader)
	r.ClientID = cid

	util.ReadN(1, reader)
}

func ParseRequestMetaInfo(reader io.Reader, messageSize uint32) (*RequestMetaInfo, io.Reader) {
	buffer := bytes.NewBuffer(util.ReadN(int(messageSize), reader))

	header := &CommonAPIRequestHeader{}
	header.Decode(buffer)

	return &RequestMetaInfo{
		MessageSize: messageSize,
		Header:      header,
	}, buffer
}

func (m *ConnManager) BuildConnHandler(rm *RequestMetaInfo) APIHandler {
	fmt.Println(rm.Header.RequestAPIKey)
	switch rm.Header.RequestAPIKey {
	case APIType_APIVersions:
		return NewAPIVersionHandler(m.conn)
	case APIType_DescribeTopicPartitions:
		return NewDescribeTopicPartitionHandler(meta_data.GetMetaDataService())
	default:
		return &todoHandler{}
	}
}

func HandleConn(conn net.Conn) {
	m := NewConnManager(conn)
	for {
		b := make([]byte, 4)
		if _, err := conn.Read(b); errors.Is(err, io.EOF) {
			conn.Close()
			fmt.Println("server read conn EOF,return")
			return
		}
		rm, bodyReader := ParseRequestMetaInfo(conn, binary.BigEndian.Uint32(b))
		h := m.BuildConnHandler(rm)
		h.HandleAPIEvent(rm, bodyReader, conn)
	}
}
