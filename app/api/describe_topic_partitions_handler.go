package api

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/codecrafters-io/kafka-starter-go/app/meta_data"
	"github.com/codecrafters-io/kafka-starter-go/app/util"
	"io"
	"net"
	"sort"
)

type describeTopicPartitionHandler struct {
	req  *describeTopicPartitionRequest
	conn net.Conn

	metadataSvc meta_data.MetaDataService
}

func (b *describeTopicPartitionRequestBody) String() string {
	f, _ := json.Marshal(b)
	return string(f)
}

func (b *describeTopicPartitionRequestBody) Decode(reader io.Reader) {
	topicArray := &requestTopicArray{}
	topicArray.Decode(reader)
	b.requestTopicArray = topicArray

	b.ResponsePartitionLimit = binary.BigEndian.Uint32(util.ReadN(4, reader))
	b.Cursor = util.ReadN(1, reader)[0]
	util.ReadN(1, reader)
}

func (r *requestTopicArray) Decode(reader io.Reader) {
	r.ArrayLength = util.ReadN(1, reader)[0]

	total := int(r.ArrayLength) - 1
	r.TopicList = make([]*requestTopic, 0, total)

	for total > 0 {
		topic := &requestTopic{}
		topic.Decode(reader)
		r.TopicList = append(r.TopicList, topic)

		total--
	}
}

func (r *requestTopic) Decode(reader io.Reader) {
	r.TopicNameLength = util.ReadN(1, reader)[0]
	r.TopicName = util.ReadN(int(r.TopicNameLength-1), reader)
	util.ReadN(1, reader)
}

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

func (r *describePartitionResponse) calcMessageSize() {
	headerBytes := CorrelationIDBytes + TagBufferBytes
	bodyBytes := ThrottleTimeBytes + TopicArrayLenBytes + NextCursorBytes + TagBufferBytes
	totalTopic := int(r.Body.TopicArray.ArrayLength - 1)

	for i := 0; i < totalTopic; i++ {
		bodyBytes += ErrorCodeLenBytes + TopicNameLengthBytes +
			int(r.Body.TopicArray.ResponseTopicList[i].TopicName.StringLength-1) +
			TopicIDLenBytes + IsInternalLenBytes + PartitionArrayLenBytes +
			TopicAuthorizedOptionLenBytes + TagBufferBytes
		bodyBytes += (PartitionErrorCodeBytes + PartitionIndexBytes +
			LeaderIDBytes + LeaderEpochBytes + PartitionReplicaNodesArrayLengthBytes +
			PartitionReplicaNodeBytes + PartitionISRNodesArrayLength + PartitionISRNodeBytes +
			PartitionEligibleLeaderReplicasBytes + PartitionLastKnownELRBytes +
			PartitionLastOfflineReplicasBytes + TagBufferBytes) *
			len(r.Body.TopicArray.ResponseTopicList[i].PartitionArray.Partitions)
	}

	r.MessageSize = uint32(headerBytes + bodyBytes)
}

const (
	ErrCode_UNKNOWN_TOPIC = 0x0003
)

func (d *describeTopicPartitionHandler) buildHeader() *ResponseHeader {
	header := &ResponseHeader{
		CorrelationID: d.req.Header.CorrelationID,
	}
	return header
}

func (d *describeTopicPartitionHandler) buildResponseTopicArray() *responseTopicArray {
	ta := &responseTopicArray{
		ResponseTopicList: make([]*responseTopic, 0, len(d.req.Body.TopicList)),
	}
	ta.ArrayLength = uint8(len(ta.ResponseTopicList) + 1)
	for _, query := range d.req.Body.TopicList {
		topic, err := d.metadataSvc.QueryTopic(query.TopicName)
		if err != nil { //topic not found
			ta.ResponseTopicList = append(ta.ResponseTopicList,
				&responseTopic{
					ErrorCode:  ErrCode_UNKNOWN_TOPIC,
					TopicName:  d.buildTopicName(query.TopicName),
					TopicID:    [16]byte{},
					IsInternal: 0,
					PartitionArray: &partitionArray{
						PartitionArrayLen: 0x01,
					},
					TopicAuthorizedOption: 0x00000df8,
				})
			continue
		}
		partitions, _ := d.metadataSvc.QueryTopicPartitions(query.TopicName)
		ta.ResponseTopicList = append(ta.ResponseTopicList,
			&responseTopic{
				ErrorCode:             0,
				TopicName:             d.buildTopicName(query.TopicName),
				TopicID:               topic.TopicUUID,
				IsInternal:            0,
				PartitionArray:        d.buildPartitionArray(partitions),
				TopicAuthorizedOption: 0x00000df8,
			})
	}
	ta.ArrayLength = uint8(len(ta.ResponseTopicList) + 1)

	sort.Slice(ta.ResponseTopicList, func(i, j int) bool {
		return bytes.Compare(
			ta.ResponseTopicList[i].TopicName.StringContent,
			ta.ResponseTopicList[j].TopicName.StringContent) < 0
	})
	return ta
}

func (d *describeTopicPartitionHandler) buildPartitionArray(partitions []*meta_data.PartitionRecord) *partitionArray {
	pr := &partitionArray{
		Partitions: make([]*partition, 0, len(partitions)),
	}

	for _, p := range partitions {
		pr.Partitions = append(pr.Partitions, &partition{
			ErrorCode:      0x0000,
			PartitionIndex: p.PartitionID,
			LeaderID:       p.LeaderID,
			LeaderEpoch:    p.LeaderEpoch,
			ReplicaNodeInfo: &replicaNodeInfo{
				ArrayLength:   p.LengthOfReplicaArray,
				ReplicateNode: p.ReplicaArray,
			},
			ISRNodes: &isrNodes{
				ArrayLength: p.LengthOfInSyncReplicaArray,
				ISRNodeID:   p.InSyncReplicaArray,
			},
			EligibleLeaderReplicas: &eligibleLeaderReplicas{
				ArrayLength: 0,
			},
			LastKnownELR: &lastKnownELR{
				ArrayLength: 0,
			},
			OfflineReplicas: &offlineReplicas{
				ArrayLength: 0,
			},
		})
	}

	pr.PartitionArrayLen = uint8(len(pr.Partitions) + 1)

	return pr
}

func (d *describeTopicPartitionHandler) buildTopicName(name []byte) *responseTopicName {
	topicName := &responseTopicName{}
	topicName.StringContent = name
	topicName.StringLength = uint8(len(topicName.StringContent) + 1)
	return topicName
}

func (d *describeTopicPartitionHandler) buildBody() *describePartitionResponseBody {
	body := &describePartitionResponseBody{
		ThrottleTime: 0,
		TopicArray:   d.buildResponseTopicArray(),
		NextCursor:   0xff,
	}

	return body
}

func (d *describeTopicPartitionHandler) buildResponse() *describePartitionResponse {
	header := d.buildHeader()
	body := d.buildBody()
	resp := &describePartitionResponse{
		Header: header,
		Body:   body,
	}

	resp.calcMessageSize()
	return resp
}

func (d *describeTopicPartitionHandler) buildReq(meta *RequestMetaInfo, bodyReader io.Reader) *describeTopicPartitionRequest {
	r := &describeTopicPartitionRequest{}

	r.MessageSize = meta.MessageSize
	r.Header = meta.Header

	body := &describeTopicPartitionRequestBody{}
	body.Decode(bodyReader)
	r.Body = body

	return r
}

func (d *describeTopicPartitionHandler) HandleAPIEvent(meta *RequestMetaInfo, bodyReader io.Reader, conn net.Conn) {
	d.req = d.buildReq(meta, bodyReader)
	d.conn = conn
	resp := d.buildResponse()

	d.writeResponse(resp)
}

func (r *responseTopicArray) Encode(buf *bytes.Buffer) {
	buf.Write([]byte{r.ArrayLength})
	for _, topic := range r.ResponseTopicList {
		topic.Encode(buf)
	}
}

func (r *describePartitionResponse) Encode(buf *bytes.Buffer) {
	buf.Write(binary.BigEndian.AppendUint32([]byte{}, r.MessageSize))
	buf.Write(binary.BigEndian.AppendUint32([]byte{}, r.Header.CorrelationID))
	buf.Write([]byte{0x00})

	buf.Write(binary.BigEndian.AppendUint32([]byte{}, r.Body.ThrottleTime))
	r.Body.TopicArray.Encode(buf)
	buf.Write([]byte{r.Body.NextCursor})
	buf.Write([]byte{0x00})
}

func (d *describeTopicPartitionHandler) writeResponse(resp *describePartitionResponse) {
	buf := &bytes.Buffer{}
	resp.Encode(buf)
	_, _ = d.conn.Write(buf.Bytes())
}

func (t *responseTopic) Encode(buf *bytes.Buffer) {
	buf.Write(binary.BigEndian.AppendUint16([]byte{}, t.ErrorCode))
	buf.Write([]byte{t.TopicName.StringLength})
	buf.Write(t.TopicName.StringContent)
	topicID := make([]byte, len(t.TopicID))
	copy(topicID, t.TopicID[:])
	buf.Write(topicID)
	buf.Write([]byte{t.IsInternal})

	buf.Write(t.PartitionArray.Encode())

	buf.Write(binary.BigEndian.AppendUint32([]byte{}, t.TopicAuthorizedOption))
	buf.Write([]byte{0x00})
}

func (pa *partitionArray) Encode() []byte {
	buf := &bytes.Buffer{}
	buf.Write([]byte{pa.PartitionArrayLen})
	for _, p := range pa.Partitions {
		buf.Write(p.Encode())
	}
	return buf.Bytes()
}

func (p *partition) Encode() []byte {
	buf := &bytes.Buffer{}

	buf.Write(binary.BigEndian.AppendUint16([]byte{}, p.ErrorCode))
	buf.Write(binary.BigEndian.AppendUint32([]byte{}, p.PartitionIndex))
	buf.Write(binary.BigEndian.AppendUint32([]byte{}, p.LeaderID))
	buf.Write(binary.BigEndian.AppendUint32([]byte{}, p.LeaderEpoch))
	buf.Write([]byte{p.ReplicaNodeInfo.ArrayLength})
	buf.Write(binary.BigEndian.AppendUint32([]byte{}, p.ReplicaNodeInfo.ReplicateNode))
	buf.Write([]byte{p.ISRNodes.ArrayLength})
	buf.Write(binary.BigEndian.AppendUint32([]byte{}, p.ISRNodes.ISRNodeID))
	buf.Write([]byte{p.EligibleLeaderReplicas.ArrayLength})
	buf.Write([]byte{p.LastKnownELR.ArrayLength})
	buf.Write([]byte{p.OfflineReplicas.ArrayLength})
	buf.Write([]byte{0x00})

	return buf.Bytes()
}

func (r *requestTopicArray) Encode() []byte {
	buf := &bytes.Buffer{}

	buf.Write([]byte{r.ArrayLength})

	for _, t := range r.TopicList {
		buf.Write(t.Encode())
	}

	return buf.Bytes()
}

func (r *requestTopic) Encode() []byte {
	buf := &bytes.Buffer{}

	buf.Write([]byte{r.TopicNameLength})
	buf.Write(r.TopicName)
	buf.Write([]byte{0x00})

	return buf.Bytes()
}
func (r *describeTopicPartitionRequest) Encode() []byte {
	buf := &bytes.Buffer{}
	buf.Write(binary.BigEndian.AppendUint32([]byte{}, r.MessageSize))

	buf.Write(r.Header.Encode())

	buf.Write(r.Body.requestTopicArray.Encode())

	buf.Write(binary.BigEndian.AppendUint32([]byte{}, r.Body.ResponsePartitionLimit))
	buf.Write([]byte{r.Body.Cursor})
	buf.Write([]byte{0x00})

	return buf.Bytes()
}

func NewDescribeTopicPartitionHandler(svc meta_data.MetaDataService) APIHandler {
	return &describeTopicPartitionHandler{
		metadataSvc: svc,
	}
}
