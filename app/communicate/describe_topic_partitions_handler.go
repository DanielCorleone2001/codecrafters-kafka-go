package communicate

import (
	"encoding/binary"
	"net"
)

type DescribeTopicPartitionHandler struct {
	meta *RequestMetaInfo
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

	return &DescribeTopicPartitionRequestBody{
		RequestTopicArray:      topicArray,
		ResponsePartitionLimit: l,
		Cursor:                 cursor,
	}
}

type DescribePartitionResponse struct {
	MessageSize uint32
	Header      *ResponseHeader
}

type DescribePartitionResponseBody struct {
	ThrottleTime uint32
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

func (d *DescribeTopicPartitionHandler) HandleAPIEvent(req *RequestMetaInfo, conn net.Conn) {
	//	body := d.ParseRequestBody()
}

func NewDescribeTopicPartitionHandler() APIHandler {
	return &DescribeTopicPartitionHandler{}
}
