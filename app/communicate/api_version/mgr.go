package api_version

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
)

type APIVersionConnManager struct {
	conn net.Conn
}

func APIVersionManager(conn net.Conn) *APIVersionConnManager {
	return &APIVersionConnManager{
		conn: conn,
	}
}

func (m *APIVersionConnManager) Close() error {
	return m.conn.Close()
}

type APIVersionRequestMessage struct {
	MessageSize uint32
	Header      *APIVersionRequestHeader
	Body        *APIVersionRequestBody
}

type APIVersionRequestHeader struct {
	RequestAPIKey     uint16
	RequestAPIVersion uint16
	CorrelationID     uint32
	ClientID          *APIVersionRequestHeaderClientID
}

type APIVersionRequestBody struct {
	*APIVersionRequestBodyClientID
	*APIVersionClientSoftwareVersion
}

type APIVersionClientSoftwareVersion struct {
	Length   uint8
	Contents []byte
}

type APIVersionRequestHeaderClientID struct {
	Length   uint16
	Contents []byte
}

type APIVersionRequestBodyClientID struct {
	Length   uint8
	Contents []byte
}

func (m *APIVersionConnManager) readMessageSize() uint32 {
	return binary.BigEndian.Uint32(m.readLength(4, m.conn))
}

func (m *APIVersionConnManager) readLength(readLen int, reader io.Reader) []byte {
	b := make([]byte, readLen)
	_, err := reader.Read(b)
	if err != nil {
		panic("read fail,err:" + err.Error())
	}
	//if n != readLen {
	//	panic(fmt.Sprintf("read want:%d, has:%d", readLen, n))
	//}

	return b
}

func (m *APIVersionConnManager) readRequestAPIKey(reader io.Reader) uint16 {
	return binary.BigEndian.Uint16(m.readLength(2, reader))
}

func (m *APIVersionConnManager) readRequestAPIVersion(reader io.Reader) uint16 {
	return binary.BigEndian.Uint16(m.readLength(2, reader))
}

func (m *APIVersionConnManager) readCorrelationID(reader io.Reader) uint32 {
	return binary.BigEndian.Uint32(m.readLength(4, reader))
}

func (m *APIVersionConnManager) readRequestHeaderClientID(reader io.Reader) *APIVersionRequestHeaderClientID {
	length := binary.BigEndian.Uint16(m.readLength(2, reader))
	content := m.readLength(int(length), reader)
	return &APIVersionRequestHeaderClientID{
		Length:   length,
		Contents: content,
	}
}

func (m *APIVersionConnManager) readTagBuffer(reader io.Reader) {
	m.readLength(1, reader)
}

func (m *APIVersionConnManager) readRequestBodyClientID(reader io.Reader) *APIVersionRequestBodyClientID {
	length := m.readLength(1, reader)
	content := m.readLength(int(length[0]-1), reader)

	return &APIVersionRequestBodyClientID{
		Length:   length[0],
		Contents: content,
	}
}

func (m *APIVersionConnManager) readClientSoftwareVersion(reader io.Reader) *APIVersionClientSoftwareVersion {
	length := m.readLength(1, reader)
	contents := m.readLength(int(length[0]-1), reader)

	return &APIVersionClientSoftwareVersion{
		Length:   length[0],
		Contents: contents,
	}
}

func (m *APIVersionConnManager) readRequest() *APIVersionRequestMessage {
	messageSize := m.readMessageSize()

	buffer := bytes.NewBuffer(m.readLength(int(messageSize), m.conn))
	requestAPIKey := m.readRequestAPIKey(buffer)
	requestAPIVersion := m.readRequestAPIVersion(buffer)
	correlationID := m.readCorrelationID(buffer)
	cid := m.readRequestHeaderClientID(buffer)
	m.readTagBuffer(buffer)

	bodyClientID := m.readRequestBodyClientID(buffer)
	csv := m.readClientSoftwareVersion(buffer)
	m.readTagBuffer(buffer)

	req := &APIVersionRequestMessage{
		MessageSize: messageSize,
		Header: &APIVersionRequestHeader{
			RequestAPIKey:     requestAPIKey,
			RequestAPIVersion: requestAPIVersion,
			CorrelationID:     correlationID,
			ClientID:          cid,
		},
		Body: &APIVersionRequestBody{
			APIVersionRequestBodyClientID:   bodyClientID,
			APIVersionClientSoftwareVersion: csv,
		},
	}
	fmt.Println(req)
	return req
}

func (r *APIVersionRequestMessage) String() string {
	sb := &strings.Builder{}
	_, _ = fmt.Fprintf(sb, "message size:%d\n", r.MessageSize)
	_, _ = fmt.Fprintf(sb, "request api key:%d\n", r.Header.RequestAPIKey)
	_, _ = fmt.Fprintf(sb, "request api version:%d\n", r.Header.RequestAPIVersion)
	_, _ = fmt.Fprintf(sb, "request correlation id:%d\n", r.Header.CorrelationID)
	_, _ = fmt.Fprintf(sb, "request client id content:%s\n", string(r.Header.ClientID.Contents))
	_, _ = fmt.Fprintf(sb, "request body client id content:%s\n", string(r.Body.APIVersionRequestBodyClientID.Contents))
	_, _ = fmt.Fprintf(sb, "request body client software version:%s\n", string(r.Body.APIVersionClientSoftwareVersion.Contents))
	return sb.String()
}

const (
	ErrorCode_Success             uint16 = 0
	ErrorCode_UNSUPPORTED_VERSION uint16 = 35
)

type APIVersionResponseMessage struct {
	MessageSize uint32
	Header      *APIVersionResponseHeader
	Body        *APIVersionResponseBody
}

func (resp *APIVersionResponseMessage) calcMessageSize() {
	headerLen := 4
	bodyLen := 2 + 1 + 7*(len(resp.Body.APIVersions.VersionList)) + 4 + 1

	resp.MessageSize = uint32(headerLen + bodyLen)
}

type APIVersionResponseHeader struct {
	CorrelationID uint32
}

type APIVersionResponseBody struct {
	ErrorCode    uint16
	APIVersions  *APIVersionsArray
	ThrottleTime uint32
}

type APIVersionsArray struct {
	ArrayLength uint8
	VersionList []*APIVersion
}

type APIVersion struct {
	APIKey                 uint16
	MinSupportedAPIVersion uint16
	MaxSupportedAPIVersion uint16
}

func (m *APIVersionConnManager) writeResponseMessage(resp *APIVersionResponseMessage) {
	m.writeMessageSize(resp)
	m.writeHeader(resp)
	m.writeBody(resp)
}

func (m *APIVersionConnManager) writeBytes(b []byte) {
	n, err := m.conn.Write(b)
	if err != nil {
		panic(err)
	}
	if n != len(b) {
		panic(fmt.Sprintf("write want:%d, actual:%d", len(b), n))
	}
}

func (m *APIVersionConnManager) writeMessageSize(resp *APIVersionResponseMessage) {
	m.writeBytes(binary.BigEndian.AppendUint32([]byte{}, resp.MessageSize))
}

func (m *APIVersionConnManager) writeHeader(resp *APIVersionResponseMessage) {
	m.writeBytes(binary.BigEndian.AppendUint32([]byte{}, resp.Header.CorrelationID))
}

func (m *APIVersionConnManager) writeBody(resp *APIVersionResponseMessage) {
	m.writeErrorCode(resp)
	m.writeAPIVersionsArray(resp)
	m.writeThrottleTime(resp)
	m.writeTagBuffer()
}

func (m *APIVersionConnManager) writeTagBuffer() {
	m.writeBytes([]byte{0x00})
}

func (m *APIVersionConnManager) writeAPIVersionsArray(resp *APIVersionResponseMessage) {
	m.writeBytes([]byte{resp.Body.APIVersions.ArrayLength})

	for _, apiVersion := range resp.Body.APIVersions.VersionList {
		m.writeBytes(binary.BigEndian.AppendUint16([]byte{}, apiVersion.APIKey))
		m.writeBytes(binary.BigEndian.AppendUint16([]byte{}, apiVersion.MinSupportedAPIVersion))
		m.writeBytes(binary.BigEndian.AppendUint16([]byte{}, apiVersion.MaxSupportedAPIVersion))
		m.writeTagBuffer()
	}
}

func (m *APIVersionConnManager) writeThrottleTime(resp *APIVersionResponseMessage) {
	m.writeBytes(binary.BigEndian.AppendUint32([]byte{}, resp.Body.ThrottleTime))
}

func (m *APIVersionConnManager) writeErrorCode(resp *APIVersionResponseMessage) {
	m.writeBytes(binary.BigEndian.AppendUint16([]byte{}, resp.Body.ErrorCode))
}

func (m *APIVersionConnManager) buildAPIVersionResponseMessage(req *APIVersionRequestMessage) *APIVersionResponseMessage {
	resp := &APIVersionResponseMessage{
		Header: m.buildResponseHeader(req),
		Body:   m.buildResponseBody(req),
	}

	resp.calcMessageSize()
	return resp
}

func (m *APIVersionConnManager) buildResponseHeader(req *APIVersionRequestMessage) *APIVersionResponseHeader {
	return &APIVersionResponseHeader{
		CorrelationID: req.Header.CorrelationID,
	}
}

func (m *APIVersionConnManager) buildResponseBody(req *APIVersionRequestMessage) *APIVersionResponseBody {
	body := &APIVersionResponseBody{
		ErrorCode:    0,
		APIVersions:  nil,
		ThrottleTime: 0,
	}
	switch req.Header.RequestAPIVersion {
	case 0, 1, 2, 3, 4:
		body.ErrorCode = ErrorCode_Success
	default:
		body.ErrorCode = ErrorCode_UNSUPPORTED_VERSION
	}

	apiVersions := &APIVersionsArray{
		VersionList: []*APIVersion{
			{
				APIKey:                 req.Header.RequestAPIKey,
				MinSupportedAPIVersion: 0,
				MaxSupportedAPIVersion: 4,
			},
		},
	}
	apiVersions.ArrayLength = uint8(len(apiVersions.VersionList) + 1)

	body.APIVersions = apiVersions

	return body
}

func (m *APIVersionConnManager) OnHandle() {
	req := m.readRequest()
	resp := m.buildAPIVersionResponseMessage(req)
	m.writeResponseMessage(resp)
}
