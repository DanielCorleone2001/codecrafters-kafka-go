package communicate

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
)

type DescribeTopicPartitionHandler struct {
	meta    *RequestMetaInfo
	reqBody *DescribeTopicPartitionRequestBody
	conn    net.Conn
}

func (d DescribeTopicPartitionRequestBody) String() string {
	f, _ := json.Marshal(d)
	return string(f)
}

type DescribeTopicPartitionRequestBody struct {
	*RequestTopicArray

	ResponsePartitionLimit uint32
	Cursor                 uint8
}

type RequestTopicArray struct {
	ArrayLength uint8
	TopicList   []*RequestTopic
}

type RequestTopic struct {
	TopicNameLength uint8
	TopicName       []byte
}

func (d *DescribeTopicPartitionHandler) ParseTopicArray() *RequestTopicArray {
	arrayLength := ReadLength(1, d.meta.BodyDataSource)

	total := int(arrayLength[0] - 1)
	topicArray := &RequestTopicArray{
		ArrayLength: arrayLength[0],
		TopicList:   make([]*RequestTopic, total),
	}
	for total > 0 {
		topicArray.TopicList = append(topicArray.TopicList, d.ParseTopic())
		total--
	}

	return topicArray
}

func (d *DescribeTopicPartitionHandler) ParseTopic() *RequestTopic {
	topicNameLength := ReadLength(1, d.meta.BodyDataSource)
	topicName := ReadLength(int(topicNameLength[0]-1), d.meta.BodyDataSource)

	ReadLength(1, d.meta.BodyDataSource)

	return &RequestTopic{
		TopicNameLength: topicNameLength[0],
		TopicName:       topicName,
	}
}

func (d *DescribeTopicPartitionHandler) ParseResponsePartitionLimit() uint32 {
	return binary.BigEndian.Uint32(ReadLength(4, d.meta.BodyDataSource))
}

func (d *DescribeTopicPartitionHandler) ParseCursor() uint8 {
	return ReadLength(1, d.meta.BodyDataSource)[0]
}

func (d *DescribeTopicPartitionHandler) ParseTagBuffer() {
	ReadLength(1, d.meta.BodyDataSource)
}

func (d *DescribeTopicPartitionHandler) ParseRequestBody() *DescribeTopicPartitionRequestBody {
	topicArray := d.ParseTopicArray()
	l := d.ParseResponsePartitionLimit()
	cursor := d.ParseCursor()

	d.ParseTagBuffer()

	body := &DescribeTopicPartitionRequestBody{
		RequestTopicArray:      topicArray,
		ResponsePartitionLimit: l,
		Cursor:                 cursor,
	}
	fmt.Println(body)

	return body
}

type DescribePartitionResponse struct {
	MessageSize uint32
	Header      *ResponseHeader
	Body        *DescribePartitionResponseBody
}

//
//func (r *DescribePartitionResponse) String() string {
//	sb := &strings.Builder{}
//	_, _ = fmt.Fprintf(sb, "message size:%d\n", r.MessageSize)
//	_, _ = fmt.Fprintf(sb, "header:%d\n", r.Header.CorrelationID)
//	_, _ = fmt.Fprintf(sb, "body:")
//}

const (
	CorrelationIDBytes   = 4
	TagBufferBytes       = 1
	ThrottleTimeBytes    = 4
	NextCursorBytes      = 1
	ArrayLengthBytes     = 1
	ErrorCodeLenBytes    = 2
	TopicNameLengthBytes = 1
	TopicIDLenBytes      = 16
	IsInternalLenBytes   = 1

	PartitionArrayLenBytes        = 1
	TopicAuthorizedOptionLenBytes = 4
	TopicArrayLenBytes            = 1
)

func (r *DescribePartitionResponse) calcMessageSize() {
	headerBytes := CorrelationIDBytes + TagBufferBytes
	bodyBytes := ThrottleTimeBytes + TopicArrayLenBytes + NextCursorBytes + TagBufferBytes
	totalTopic := int(r.Body.TopicArray.ArrayLength - 1)

	for i := 0; i < totalTopic; i++ {
		bodyBytes += ErrorCodeLenBytes + TopicNameLengthBytes +
			int(r.Body.TopicArray.ResponseTopicList[i].TopicName.StringLength-1) +
			TopicIDLenBytes + IsInternalLenBytes + PartitionArrayLenBytes +
			TopicAuthorizedOptionLenBytes + TagBufferBytes
	}

	r.MessageSize = uint32(headerBytes + bodyBytes)
}

type DescribePartitionResponseBody struct {
	ThrottleTime uint32
	TopicArray   *ResponseTopicArray
	NextCursor   uint8
}

type ResponseTopicArray struct {
	ArrayLength       uint8
	ResponseTopicList []*ResponseTopic
}

type ResponseTopicName struct {
	StringLength  uint8
	StringContent []byte
}

type ResponseTopic struct {
	ErrorCode uint16
	TopicName *ResponseTopicName
	TopicID   [16]byte

	IsInternal uint8

	//PartitionArray        []*Partition
	PartitionArray        uint8
	TopicAuthorizedOption uint32
}

type Partition struct {
	ErrorCode       uint16
	PartitionIndex  uint32
	LeaderID        uint32
	LeaderEpoch     uint32
	ReplicaNodeInfo *ReplicaNodeInfo
}

type ReplicaNodeInfo struct {
	ArrayLength         uint8
	ReplicateNodeIDList []uint32
}

const (
	ErrCode_UNKNOWN_TOPIC = 0x0003
)

func (d *DescribeTopicPartitionHandler) buildHeader() *ResponseHeader {
	header := &ResponseHeader{
		CorrelationID: d.meta.Header.CorrelationID,
	}
	return header
}

func (d *DescribeTopicPartitionHandler) buildResponseTopicArray() *ResponseTopicArray {
	ta := &ResponseTopicArray{
		ResponseTopicList: []*ResponseTopic{
			{
				ErrorCode:             ErrCode_UNKNOWN_TOPIC,
				TopicName:             d.buildTopicName(),
				TopicID:               [16]byte{},
				IsInternal:            0,
				PartitionArray:        0x01,
				TopicAuthorizedOption: 0x00000df8,
			},
		},
	}
	ta.ArrayLength = uint8(len(ta.ResponseTopicList) + 1)
	return ta
}

func (d *DescribeTopicPartitionHandler) buildTopicName() *ResponseTopicName {
	topicName := &ResponseTopicName{}
	topicName.StringContent = []byte{'f', 'o', 'o'}
	topicName.StringLength = uint8(len(topicName.StringContent) + 1)
	return topicName
}

func (d *DescribeTopicPartitionHandler) buildUnknownTopicBody() *DescribePartitionResponseBody {
	body := &DescribePartitionResponseBody{
		ThrottleTime: 0,
		TopicArray:   d.buildResponseTopicArray(),
		NextCursor:   0xff,
	}

	return body
}

func (d *DescribeTopicPartitionHandler) BuildResponse() *DescribePartitionResponse {
	header := d.buildHeader()
	body := d.buildUnknownTopicBody()
	resp := &DescribePartitionResponse{
		Header: header,
		Body:   body,
	}

	resp.calcMessageSize()
	return resp
}

func (d *DescribeTopicPartitionHandler) HandleAPIEvent(req *RequestMetaInfo, conn net.Conn) {
	d.meta = req
	d.conn = conn
	d.reqBody = d.ParseRequestBody()
	resp := d.BuildResponse()

	d.WriteResponse(resp)
}

func (d *DescribeTopicPartitionHandler) writeMessageSize(resp *DescribePartitionResponse) {
	WriteBytes(binary.BigEndian.AppendUint32([]byte{}, resp.MessageSize), d.conn)
}

func (d *DescribeTopicPartitionHandler) writeTagBuffer() {
	WriteBytes([]byte{0x00}, d.conn)
}

func (d *DescribeTopicPartitionHandler) writeResponseHeader(resp *DescribePartitionResponse) {
	WriteBytes(binary.BigEndian.AppendUint32([]byte{}, resp.Header.CorrelationID), d.conn)
	d.writeTagBuffer()
}

func (d *DescribeTopicPartitionHandler) writeThrottleTime(resp *DescribePartitionResponse) {
	WriteBytes(binary.BigEndian.AppendUint32([]byte{}, resp.Body.ThrottleTime), d.conn)
}

func (d *DescribeTopicPartitionHandler) writeTopicArrayLength(resp *DescribePartitionResponse) {
	WriteBytes([]byte{resp.Body.TopicArray.ArrayLength}, d.conn)
}

func (d *DescribeTopicPartitionHandler) writeTopicArray(resp *DescribePartitionResponse) {
	d.writeTopicArrayLength(resp)
	for _, topic := range resp.Body.TopicArray.ResponseTopicList {
		d.writeTopic(topic)
	}
}

func (d *DescribeTopicPartitionHandler) writeNextCursor(resp *DescribePartitionResponse) {
	WriteBytes([]byte{resp.Body.NextCursor}, d.conn)
}

func (d *DescribeTopicPartitionHandler) writeResponseBody(resp *DescribePartitionResponse) {
	d.writeThrottleTime(resp)
	d.writeTopicArray(resp)
	d.writeNextCursor(resp)
	d.writeTagBuffer()
}

func (d *DescribeTopicPartitionHandler) WriteResponse(resp *DescribePartitionResponse) {
	d.writeMessageSize(resp)
	d.writeResponseHeader(resp)
	d.writeResponseBody(resp)
}

func (d *DescribeTopicPartitionHandler) writeTopic(topic *ResponseTopic) {
	WriteBytes(binary.BigEndian.AppendUint16([]byte{}, topic.ErrorCode), d.conn)
	WriteBytes([]byte{topic.TopicName.StringLength}, d.conn)
	WriteBytes(topic.TopicName.StringContent, d.conn)
	topicID := make([]byte, len(topic.TopicID))
	copy(topicID, topic.TopicID[:])
	WriteBytes(topicID, d.conn)
	WriteBytes([]byte{topic.IsInternal}, d.conn)

	d.writePartitionArray(topic)

	WriteBytes(binary.BigEndian.AppendUint32([]byte{}, topic.TopicAuthorizedOption), d.conn)
	d.writeTagBuffer()
}

func (d *DescribeTopicPartitionHandler) writePartitionArray(topic *ResponseTopic) {
	WriteBytes([]byte{topic.PartitionArray}, d.conn)
}

func NewDescribeTopicPartitionHandler() APIHandler {
	return &DescribeTopicPartitionHandler{}
}
