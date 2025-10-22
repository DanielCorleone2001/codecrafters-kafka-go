package api

import (
	"bytes"
	"encoding/binary"
	"github.com/codecrafters-io/kafka-starter-go/app/util"
	"io"
	"net"
)

const (
	ErrorCode_Success             uint16 = 0
	ErrorCode_UNSUPPORTED_VERSION uint16 = 35
)

func (r *apiVersionRequest) Equal(b *apiVersionRequest) bool {
	return r.MessageSize == b.MessageSize &&
		r.Header.Equal(b.Header) &&
		r.Body.Equal(b.Body)
}

func (r *apiVersionRequest) Encode() []byte {
	buf := &bytes.Buffer{}
	buf.Write(binary.BigEndian.AppendUint32([]byte{}, r.MessageSize))
	buf.Write(binary.BigEndian.AppendUint16([]byte{}, r.Header.RequestAPIKey))
	buf.Write(binary.BigEndian.AppendUint16([]byte{}, r.Header.RequestAPIVersion))
	buf.Write(binary.BigEndian.AppendUint32([]byte{}, r.Header.CorrelationID))
	buf.Write(binary.BigEndian.AppendUint16([]byte{}, r.Header.ClientID.Length))
	buf.Write(r.Header.ClientID.Contents)
	buf.Write([]byte{0x00})
	buf.Write([]byte{r.Body.apiVersionRequestBodyClientID.Length})
	buf.Write(r.Body.apiVersionRequestBodyClientID.Contents)
	buf.Write([]byte{r.Body.apiVersionClientSoftwareVersion.Length})
	buf.Write(r.Body.apiVersionClientSoftwareVersion.Contents)
	buf.Write([]byte{0x00})

	return buf.Bytes()
}

func (r *apiVersionRequest) DecodeBytes(b []byte) {
	r.Decode(bytes.NewReader(b))
}

func (r *apiVersionRequest) Decode(reader io.Reader) {
	r.MessageSize = binary.BigEndian.Uint32(util.ReadN(4, reader))

	header := &CommonAPIRequestHeader{
		ClientID: &CommonAPIRequestHeaderClientID{},
	}
	header.RequestAPIKey = binary.BigEndian.Uint16(util.ReadN(2, reader))
	header.RequestAPIVersion = binary.BigEndian.Uint16(util.ReadN(2, reader))
	header.CorrelationID = binary.BigEndian.Uint32(util.ReadN(4, reader))
	header.ClientID.Length = binary.BigEndian.Uint16(util.ReadN(2, reader))
	header.ClientID.Contents = util.ReadN(int(header.ClientID.Length), reader)
	util.ReadN(1, reader)

	r.Header = header

	body := &apiVersionRequestBody{
		apiVersionRequestBodyClientID:   &apiVersionRequestBodyClientID{},
		apiVersionClientSoftwareVersion: &apiVersionClientSoftwareVersion{},
	}
	body.apiVersionRequestBodyClientID.Length = util.ReadN(1, reader)[0]
	body.apiVersionRequestBodyClientID.Contents = util.ReadN(int(body.apiVersionRequestBodyClientID.Length)-1, reader)
	body.apiVersionClientSoftwareVersion.Length = util.ReadN(1, reader)[0]
	body.apiVersionClientSoftwareVersion.Contents = util.ReadN(int(body.apiVersionClientSoftwareVersion.Length)-1, reader)
	util.ReadN(1, reader)

	r.Body = body
}

var _ APIHandler = &apiVersionHandler{}

func NewAPIVersionHandler(conn net.Conn) APIHandler {
	return &apiVersionHandler{
		conn: conn,
	}
}

type apiVersionHandler struct {
	conn net.Conn

	req *apiVersionRequest
}

func (c *apiVersionRequestBodyClientID) Equal(b *apiVersionRequestBodyClientID) bool {
	return c.Length == b.Length && bytes.Equal(c.Contents, b.Contents)
}

func (v *apiVersionClientSoftwareVersion) Equal(b *apiVersionClientSoftwareVersion) bool {
	return v.Length == b.Length && bytes.Equal(v.Contents, b.Contents)
}

func (r *apiVersionRequestBody) Encode() []byte {
	buf := &bytes.Buffer{}
	buf.Write([]byte{r.apiVersionRequestBodyClientID.Length})
	buf.Write(r.apiVersionRequestBodyClientID.Contents)
	buf.Write([]byte{r.apiVersionClientSoftwareVersion.Length})
	buf.Write(r.apiVersionClientSoftwareVersion.Contents)
	buf.Write([]byte{0x00})
	return buf.Bytes()
}

func (r *apiVersionRequestBody) Decode(reader io.Reader) {
	c := &apiVersionRequestBodyClientID{}
	c.Length = util.ReadN(1, reader)[0]
	c.Contents = util.ReadN(int(c.Length)-1, reader)
	r.apiVersionRequestBodyClientID = c

	v := &apiVersionClientSoftwareVersion{}
	v.Length = util.ReadN(1, reader)[0]
	v.Contents = util.ReadN(int(v.Length)-1, reader)
	util.ReadN(1, reader)
	r.apiVersionClientSoftwareVersion = v
}

func (r *apiVersionRequestBody) DecodeBytes(b []byte) {
	r.Decode(bytes.NewReader(b))
}

func (r *apiVersionRequestBody) Equal(b *apiVersionRequestBody) bool {
	return r.apiVersionRequestBodyClientID.Equal(b.apiVersionRequestBodyClientID) &&
		r.apiVersionClientSoftwareVersion.Equal(b.apiVersionClientSoftwareVersion)
}

func (resp *apiVersionResponse) Encode() []byte {
	buf := &bytes.Buffer{}

	buf.Write(binary.BigEndian.AppendUint32([]byte{}, resp.MessageSize))
	buf.Write(binary.BigEndian.AppendUint32([]byte{}, resp.Header.CorrelationID))
	buf.Write(binary.BigEndian.AppendUint16([]byte{}, resp.Body.ErrorCode))

	buf.Write([]byte{resp.Body.APIVersions.ArrayLength})
	for _, apiVersion := range resp.Body.APIVersions.VersionList {
		buf.Write(binary.BigEndian.AppendUint16([]byte{}, apiVersion.APIKey))
		buf.Write(binary.BigEndian.AppendUint16([]byte{}, apiVersion.MinSupportedAPIVersion))
		buf.Write(binary.BigEndian.AppendUint16([]byte{}, apiVersion.MaxSupportedAPIVersion))
		buf.Write([]byte{0x00})
	}

	buf.Write(binary.BigEndian.AppendUint32([]byte{}, resp.Body.ThrottleTime))
	buf.Write([]byte{0x00})

	return buf.Bytes()
}

func (resp *apiVersionResponse) Decode(reader io.Reader) {
	resp.MessageSize = binary.BigEndian.Uint32(util.ReadN(4, reader))

	header := &ResponseHeader{}
	header.CorrelationID = binary.BigEndian.Uint32(util.ReadN(4, reader))
	resp.Header = header

	body := &apiVersionResponseBody{}

	body.ErrorCode = binary.BigEndian.Uint16(util.ReadN(2, reader))
	vr := &apiVersionsArray{}
	vr.ArrayLength = util.ReadN(1, reader)[0]
	vr.VersionList = make([]*apiVersion, vr.ArrayLength-1)
	for i := 0; i < int(vr.ArrayLength-1); i++ {
		v := &apiVersion{}
		v.APIKey = binary.BigEndian.Uint16(util.ReadN(2, reader))
		v.MinSupportedAPIVersion = binary.BigEndian.Uint16(util.ReadN(2, reader))
		v.MaxSupportedAPIVersion = binary.BigEndian.Uint16(util.ReadN(2, reader))
		util.ReadN(1, reader)

		vr.VersionList[i] = v
	}
	body.APIVersions = vr

	body.ThrottleTime = binary.BigEndian.Uint32(util.ReadN(4, reader))
	util.ReadN(1, reader)

	resp.Body = body
}

func (h *apiVersionHandler) buildResponseHeader() *ResponseHeader {
	return &ResponseHeader{
		CorrelationID: h.req.Header.CorrelationID,
	}
}

func (h *apiVersionHandler) buildResponseBody() *apiVersionResponseBody {
	body := &apiVersionResponseBody{}
	switch h.req.Header.RequestAPIVersion {
	case 0, 1, 2, 3, 4:
		body.ErrorCode = ErrorCode_Success
	default:
		body.ErrorCode = ErrorCode_UNSUPPORTED_VERSION
	}

	apiVersions := &apiVersionsArray{
		VersionList: []*apiVersion{
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

func (resp *apiVersionResponse) calcMessageSize() {
	headerLen := 4
	bodyLen := 2 + 1 + 7*(len(resp.Body.APIVersions.VersionList)) + 4 + 1

	resp.MessageSize = uint32(headerLen + bodyLen)
}

func (h *apiVersionHandler) buildResponse() *apiVersionResponse {
	resp := &apiVersionResponse{
		Header: h.buildResponseHeader(),
		Body:   h.buildResponseBody(),
	}

	resp.calcMessageSize()
	return resp
}

func (h *apiVersionHandler) writeResponse(resp *apiVersionResponse) {
	_, _ = h.conn.Write(resp.Encode())
}

func (h *apiVersionHandler) buildReq(reqMeta *RequestMetaInfo, bodyReader io.Reader) *apiVersionRequest {
	r := &apiVersionRequest{
		MessageSize: reqMeta.MessageSize,
		Header:      reqMeta.Header,
	}
	body := &apiVersionRequestBody{}
	body.Decode(bodyReader)
	r.Body = body

	return r
}

func (h *apiVersionHandler) HandleAPIEvent(reqMeta *RequestMetaInfo, bodyReader io.Reader, conn net.Conn) {
	h.req = h.buildReq(reqMeta, bodyReader)
	h.writeResponse(h.buildResponse())
}
