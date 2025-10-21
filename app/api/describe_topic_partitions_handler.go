package api

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/codecrafters-io/kafka-starter-go/app/meta_data"
	"github.com/codecrafters-io/kafka-starter-go/app/util"
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
	arrayLength := util.ReadLength(1, d.meta.BodyDataSource)

	total := int(arrayLength[0] - 1)
	topicArray := &RequestTopicArray{
		ArrayLength: arrayLength[0],
		TopicList:   make([]*RequestTopic, 0, total),
	}
	for total > 0 {
		topicArray.TopicList = append(topicArray.TopicList, d.ParseTopic())
		total--
	}

	return topicArray
}

func (d *DescribeTopicPartitionHandler) ParseTopic() *RequestTopic {
	topicNameLength := util.ReadLength(1, d.meta.BodyDataSource)
	topicName := util.ReadLength(int(topicNameLength[0]-1), d.meta.BodyDataSource)

	util.ReadLength(1, d.meta.BodyDataSource)

	return &RequestTopic{
		TopicNameLength: topicNameLength[0],
		TopicName:       topicName,
	}
}

func (d *DescribeTopicPartitionHandler) ParseResponsePartitionLimit() uint32 {
	return binary.BigEndian.Uint32(util.ReadLength(4, d.meta.BodyDataSource))
}

func (d *DescribeTopicPartitionHandler) ParseCursor() uint8 {
	return util.ReadLength(1, d.meta.BodyDataSource)[0]
}

func (d *DescribeTopicPartitionHandler) ParseTagBuffer() {
	util.ReadLength(1, d.meta.BodyDataSource)
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

	PartitionErrorCodeBytes               = 2
	PartitionIndexBytes                   = 4
	LeaderIDBytes                         = 4
	LeaderEpochBytes                      = 4
	PartitionReplicaNodesArrayLengthBytes = 1
	PartitionReplicaNodeBytes             = 4
	PartitionISRNodesArrayLength          = 1
	PartitionISRNodeBytes                 = 4
	PartitionEligibleLeaderReplicasBytes  = 1
	PartitionLastKnownELRBytes            = 1
	PartitionLastOfflineReplicasBytes     = 1
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
		bodyBytes += PartitionArrayLenBytes
		bodyBytes += (PartitionErrorCodeBytes + PartitionIndexBytes +
			LeaderIDBytes + LeaderEpochBytes + PartitionReplicaNodesArrayLengthBytes +
			PartitionReplicaNodeBytes + PartitionISRNodesArrayLength + PartitionISRNodeBytes +
			PartitionEligibleLeaderReplicasBytes + PartitionLastKnownELRBytes +
			PartitionLastOfflineReplicasBytes + TagBufferBytes) *
			len(r.Body.TopicArray.ResponseTopicList[i].PartitionArray.Partitions)
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

	PartitionArray *PartitionArray

	TopicAuthorizedOption uint32
}

type PartitionArray struct {
	PartitionArrayLen uint8
	Partitions        []*Partition
}

type Partition struct {
	ErrorCode              uint16
	PartitionIndex         uint32
	LeaderID               uint32
	LeaderEpoch            uint32
	ReplicaNodeInfo        *ReplicaNodeInfo
	ISRNodes               *ISRNodes
	EligibleLeaderReplicas *EligibleLeaderReplicas
	LastKnownELR           *LastKnownELR
	OfflineReplicas        *OfflineReplicas
}

type OfflineReplicas struct {
	ArrayLength uint8
}
type EligibleLeaderReplicas struct {
	ArrayLength uint8
}

type LastKnownELR struct {
	ArrayLength uint8
}

type ISRNodes struct {
	ArrayLength uint8
	ISRNodeID   uint32
}

type ReplicaNodeInfo struct {
	ArrayLength   uint8
	ReplicateNode uint32
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
		ResponseTopicList: make([]*ResponseTopic, 0, len(d.reqBody.TopicList)),
	}
	ta.ArrayLength = uint8(len(ta.ResponseTopicList) + 1)
	for _, query := range d.reqBody.TopicList {
		topic, err := meta_data.GetMetaDataService().QueryTopic(query.TopicName)
		if err != nil { //topic not found
			ta.ResponseTopicList = append(ta.ResponseTopicList,
				&ResponseTopic{
					ErrorCode:  ErrCode_UNKNOWN_TOPIC,
					TopicName:  d.buildTopicName(query.TopicName),
					TopicID:    [16]byte{},
					IsInternal: 0,
					PartitionArray: &PartitionArray{
						PartitionArrayLen: 0x01,
					},
					TopicAuthorizedOption: 0x00000df8,
				})
			continue
		}
		partitions, _ := meta_data.GetMetaDataService().QueryTopicPartitions(query.TopicName)
		ta.ResponseTopicList = append(ta.ResponseTopicList,
			&ResponseTopic{
				ErrorCode:             0,
				TopicName:             d.buildTopicName(query.TopicName),
				TopicID:               topic.TopicUUID,
				IsInternal:            0,
				PartitionArray:        d.buildPartitionArray(partitions),
				TopicAuthorizedOption: 0x00000df8,
			})
	}
	ta.ArrayLength = uint8(len(ta.ResponseTopicList) + 1)
	return ta
}

func (d *DescribeTopicPartitionHandler) buildPartitionArray(partitions []*meta_data.PartitionRecord) *PartitionArray {
	pr := &PartitionArray{
		Partitions: make([]*Partition, 0, len(partitions)),
	}

	for _, p := range partitions {
		pr.Partitions = append(pr.Partitions, &Partition{
			ErrorCode:      0x0000,
			PartitionIndex: 0x00000001,
			LeaderID:       p.LeaderID,
			LeaderEpoch:    p.LeaderEpoch,
			ReplicaNodeInfo: &ReplicaNodeInfo{
				ArrayLength:   p.LengthOfReplicaArray,
				ReplicateNode: p.ReplicaArray,
			},
			ISRNodes: &ISRNodes{
				ArrayLength: p.LengthOfInSyncReplicaArray,
				ISRNodeID:   p.InSyncReplicaArray,
			},
			EligibleLeaderReplicas: &EligibleLeaderReplicas{
				ArrayLength: 0,
			},
			LastKnownELR: &LastKnownELR{
				ArrayLength: 0,
			},
			OfflineReplicas: &OfflineReplicas{
				ArrayLength: 0,
			},
		})
	}

	pr.PartitionArrayLen = uint8(len(pr.Partitions) + 1)

	return pr
}

func (d *DescribeTopicPartitionHandler) buildTopicName(name []byte) *ResponseTopicName {
	topicName := &ResponseTopicName{}
	topicName.StringContent = name
	topicName.StringLength = uint8(len(topicName.StringContent) + 1)
	return topicName
}

func (d *DescribeTopicPartitionHandler) buildBody() *DescribePartitionResponseBody {
	body := &DescribePartitionResponseBody{
		ThrottleTime: 0,
		TopicArray:   d.buildResponseTopicArray(),
		NextCursor:   0xff,
	}

	return body
}

func (d *DescribeTopicPartitionHandler) BuildResponse() *DescribePartitionResponse {
	header := d.buildHeader()
	body := d.buildBody()
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
	util.WriteBytes(binary.BigEndian.AppendUint32([]byte{}, resp.MessageSize), d.conn)
}

func (d *DescribeTopicPartitionHandler) writeTagBuffer() {
	util.WriteBytes([]byte{0x00}, d.conn)
}

func (d *DescribeTopicPartitionHandler) writeResponseHeader(resp *DescribePartitionResponse) {
	util.WriteBytes(binary.BigEndian.AppendUint32([]byte{}, resp.Header.CorrelationID), d.conn)
	d.writeTagBuffer()
}

func (d *DescribeTopicPartitionHandler) writeThrottleTime(resp *DescribePartitionResponse) {
	util.WriteBytes(binary.BigEndian.AppendUint32([]byte{}, resp.Body.ThrottleTime), d.conn)
}

func (d *DescribeTopicPartitionHandler) writeTopicArrayLength(resp *DescribePartitionResponse) {
	util.WriteBytes([]byte{resp.Body.TopicArray.ArrayLength}, d.conn)
}

func (d *DescribeTopicPartitionHandler) writeTopicArray(resp *DescribePartitionResponse) {
	d.writeTopicArrayLength(resp)
	for _, topic := range resp.Body.TopicArray.ResponseTopicList {
		d.writeTopic(topic)
	}
}

func (d *DescribeTopicPartitionHandler) writeNextCursor(resp *DescribePartitionResponse) {
	util.WriteBytes([]byte{resp.Body.NextCursor}, d.conn)
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
	util.WriteBytes(binary.BigEndian.AppendUint16([]byte{}, topic.ErrorCode), d.conn)
	util.WriteBytes([]byte{topic.TopicName.StringLength}, d.conn)
	util.WriteBytes(topic.TopicName.StringContent, d.conn)
	topicID := make([]byte, len(topic.TopicID))
	copy(topicID, topic.TopicID[:])
	util.WriteBytes(topicID, d.conn)
	util.WriteBytes([]byte{topic.IsInternal}, d.conn)

	d.writePartitionArray(topic)

	util.WriteBytes(binary.BigEndian.AppendUint32([]byte{}, topic.TopicAuthorizedOption), d.conn)
	d.writeTagBuffer()
}

func (d *DescribeTopicPartitionHandler) writePartitionArray(topic *ResponseTopic) {
	util.WriteBytes([]byte{topic.PartitionArray.PartitionArrayLen}, d.conn)
	for _, p := range topic.PartitionArray.Partitions {
		util.WriteBytes(binary.BigEndian.AppendUint16([]byte{}, p.ErrorCode), d.conn)
		util.WriteBytes(binary.BigEndian.AppendUint32([]byte{}, p.PartitionIndex), d.conn)
		util.WriteBytes(binary.BigEndian.AppendUint32([]byte{}, p.LeaderID), d.conn)
		util.WriteBytes(binary.BigEndian.AppendUint32([]byte{}, p.LeaderEpoch), d.conn)
		util.WriteBytes([]byte{p.ReplicaNodeInfo.ArrayLength}, d.conn)
		util.WriteBytes(binary.BigEndian.AppendUint32([]byte{}, p.ReplicaNodeInfo.ReplicateNode), d.conn)
		util.WriteBytes([]byte{p.ISRNodes.ArrayLength}, d.conn)
		util.WriteBytes([]byte{p.EligibleLeaderReplicas.ArrayLength}, d.conn)
		util.WriteBytes([]byte{p.LastKnownELR.ArrayLength}, d.conn)
		util.WriteBytes([]byte{p.OfflineReplicas.ArrayLength}, d.conn)
		d.writeTagBuffer()
	}
}

func NewDescribeTopicPartitionHandler() APIHandler {
	return &DescribeTopicPartitionHandler{}
}
