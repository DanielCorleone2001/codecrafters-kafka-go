package api

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"github.com/codecrafters-io/kafka-starter-go/app/mock"
	"github.com/codecrafters-io/kafka-starter-go/app/util"
	"io"
	"testing"
)

func Test_APIVersionRequest(t *testing.T) {
	r := &apiVersionRequest{
		MessageSize: 35,
		Header: &CommonAPIRequestHeader{
			RequestAPIKey:     18,
			RequestAPIVersion: 4,
			CorrelationID:     7,
			ClientID: &CommonAPIRequestHeaderClientID{
				Length:   9,
				Contents: []byte("kafka-cli"),
			},
		},
		Body: &apiVersionRequestBody{
			apiVersionRequestBodyClientID: &apiVersionRequestBodyClientID{
				Length:   10,
				Contents: []byte("kafka-cli"),
			},
			apiVersionClientSoftwareVersion: &apiVersionClientSoftwareVersion{
				Length:   4,
				Contents: []byte("0.1"),
			},
		},
	}
	b := r.Encode()
	t.Logf("%s", hex.EncodeToString(b))
	rr := &apiVersionRequest{}
	rr.DecodeBytes(b)

	if !r.Equal(rr) {
		t.Fatal()
	}
}

func TestApiVersionHandler_HandleAPIEvent(t *testing.T) {
	req := &apiVersionRequest{
		MessageSize: 35,
		Header: &CommonAPIRequestHeader{
			RequestAPIKey:     0x12,
			RequestAPIVersion: 0x04,
			CorrelationID:     0x07,
			ClientID: &CommonAPIRequestHeaderClientID{
				Length:   0x0009,
				Contents: []byte("kafka-cli"),
			},
		},
		Body: &apiVersionRequestBody{
			apiVersionRequestBodyClientID: &apiVersionRequestBodyClientID{
				Length:   0x0A,
				Contents: []byte("kafka-cli"),
			},
			apiVersionClientSoftwareVersion: &apiVersionClientSoftwareVersion{
				Length:   0x04,
				Contents: []byte("0.1"),
			},
		},
	}

	dataSource := req.Encode()
	reader := bytes.NewReader(dataSource)
	writer := &bytes.Buffer{}

	conn := mock.NewFakeConn(reader, writer)

	meta, bodyReader := ParseRequestMetaInfo(reader, binary.BigEndian.Uint32(util.ReadN(4, reader)))
	h := NewAPIVersionHandler(conn)
	h.HandleAPIEvent(meta, bodyReader, conn)

	b, err := io.ReadAll(writer)
	if err != nil {
		t.Fatal(err)
	}
	r := &apiVersionResponse{}
	r.Decode(bytes.NewReader(b))

}
