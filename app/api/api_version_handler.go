package api

import (
	"encoding/binary"
	"github.com/codecrafters-io/kafka-starter-go/app/util"
	"net"
)

const (
	ErrorCode_Success             uint16 = 0
	ErrorCode_UNSUPPORTED_VERSION uint16 = 35
)

var _ APIHandler = &APIVersionHandler{}

func NewAPIVersionHandler(conn net.Conn) APIHandler {
	return &APIVersionHandler{
		conn: conn,
	}
}

type APIVersionHandler struct {
	conn net.Conn

	meta *RequestMetaInfo
}

type APIVersionRequestBodyClientID struct {
	Length   uint8
	Contents []byte
}

type APIVersionClientSoftwareVersion struct {
	Length   uint8
	Contents []byte
}

type APIVersionRequestBody struct {
	*APIVersionRequestBodyClientID
	*APIVersionClientSoftwareVersion
}

func (h *APIVersionHandler) ParseRequestBodyClientID() *APIVersionRequestBodyClientID {
	length := util.ReadLength(1, h.meta.BodyDataSource)
	content := util.ReadLength(int(length[0]-1), h.meta.BodyDataSource)

	return &APIVersionRequestBodyClientID{
		Length:   length[0],
		Contents: content,
	}
}

func (h *APIVersionHandler) ParseClientSoftwareVersion() *APIVersionClientSoftwareVersion {
	length := util.ReadLength(1, h.meta.BodyDataSource)
	contents := util.ReadLength(int(length[0]-1), h.meta.BodyDataSource)

	return &APIVersionClientSoftwareVersion{
		Length:   length[0],
		Contents: contents,
	}
}

func (h *APIVersionHandler) ParseTagBuffer() {
	util.ReadLength(1, h.meta.BodyDataSource)
}

func (h *APIVersionHandler) ParseRequestBody() *APIVersionRequestBody {
	return &APIVersionRequestBody{
		APIVersionRequestBodyClientID:   h.ParseRequestBodyClientID(),
		APIVersionClientSoftwareVersion: h.ParseClientSoftwareVersion(),
	}
}

type APIVersionRequestHeaderClientID struct {
	Length   uint16
	Contents []byte
}

type APIVersionRequestHeader struct {
	RequestAPIKey     uint16
	RequestAPIVersion uint16
	CorrelationID     uint32
	ClientID          *APIVersionRequestHeaderClientID
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

type APIVersionResponse struct {
	MessageSize uint32
	Header      *ResponseHeader
	Body        *APIVersionResponseBody
}

func (h *APIVersionHandler) BuildResponseHeader() *ResponseHeader {
	return &ResponseHeader{
		CorrelationID: h.meta.Header.CorrelationID,
	}
}

func (h *APIVersionHandler) BuildResponseBody() *APIVersionResponseBody {
	body := &APIVersionResponseBody{}
	switch h.meta.Header.RequestAPIVersion {
	case 0, 1, 2, 3, 4:
		body.ErrorCode = ErrorCode_Success
	default:
		body.ErrorCode = ErrorCode_UNSUPPORTED_VERSION
	}

	apiVersions := &APIVersionsArray{
		VersionList: []*APIVersion{
			{
				APIKey:                 APIType_APIVersions,
				MinSupportedAPIVersion: 0,
				MaxSupportedAPIVersion: 4,
			},
			{
				APIKey:                 APIType_DescribeTopicPartitions,
				MinSupportedAPIVersion: 0,
				MaxSupportedAPIVersion: 0,
			},
			{
				APIKey:                 APIType_Fetch,
				MinSupportedAPIVersion: 0,
				MaxSupportedAPIVersion: 16,
			},
		},
	}
	apiVersions.ArrayLength = uint8(len(apiVersions.VersionList) + 1)

	body.APIVersions = apiVersions

	return body
}

func (resp *APIVersionResponse) calcMessageSize() {
	headerLen := 4
	bodyLen := 2 + 1 + 7*(len(resp.Body.APIVersions.VersionList)) + 4 + 1

	resp.MessageSize = uint32(headerLen + bodyLen)
}

func (h *APIVersionHandler) BuildResponse() *APIVersionResponse {
	resp := &APIVersionResponse{
		Header: h.BuildResponseHeader(),
		Body:   h.BuildResponseBody(),
	}

	resp.calcMessageSize()
	return resp
}

func (h *APIVersionHandler) writeResponse(resp *APIVersionResponse) {
	h.writeMessageSize(resp)
	h.writeHeader(resp)
	h.writeBody(resp)
}

func (h *APIVersionHandler) writeMessageSize(resp *APIVersionResponse) {
	util.WriteBytes(binary.BigEndian.AppendUint32([]byte{}, resp.MessageSize), h.conn)
}

func (h *APIVersionHandler) writeHeader(resp *APIVersionResponse) {
	util.WriteBytes(binary.BigEndian.AppendUint32([]byte{}, resp.Header.CorrelationID), h.conn)
}

func (h *APIVersionHandler) writeErrorCode(resp *APIVersionResponse) {
	util.WriteBytes(binary.BigEndian.AppendUint16([]byte{}, resp.Body.ErrorCode), h.conn)
}

func (h *APIVersionHandler) writeTagBuffer() {
	util.WriteBytes([]byte{0x00}, h.conn)
}

func (h *APIVersionHandler) writeAPIVersionsArray(resp *APIVersionResponse) {
	util.WriteBytes([]byte{resp.Body.APIVersions.ArrayLength}, h.conn)
	for _, apiVersion := range resp.Body.APIVersions.VersionList {
		util.WriteBytes(binary.BigEndian.AppendUint16([]byte{}, apiVersion.APIKey), h.conn)
		util.WriteBytes(binary.BigEndian.AppendUint16([]byte{}, apiVersion.MinSupportedAPIVersion), h.conn)
		util.WriteBytes(binary.BigEndian.AppendUint16([]byte{}, apiVersion.MaxSupportedAPIVersion), h.conn)
		h.writeTagBuffer()
	}
}

func (h *APIVersionHandler) writeThrottleTime(resp *APIVersionResponse) {
	util.WriteBytes(binary.BigEndian.AppendUint32([]byte{}, resp.Body.ThrottleTime), h.conn)
}

func (h *APIVersionHandler) writeBody(resp *APIVersionResponse) {
	h.writeErrorCode(resp)
	h.writeAPIVersionsArray(resp)
	h.writeThrottleTime(resp)
	h.writeTagBuffer()
}

func (h *APIVersionHandler) HandleAPIEvent(reqMeta *RequestMetaInfo, conn net.Conn) {
	h.meta = reqMeta
	h.writeResponse(h.BuildResponse())
}
