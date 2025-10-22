package api

type describeTopicPartitionRequestBody struct {
	*requestTopicArray

	ResponsePartitionLimit uint32
	Cursor                 uint8
}

type requestTopicArray struct {
	ArrayLength uint8
	TopicList   []*requestTopic
}

type requestTopic struct {
	TopicNameLength uint8
	TopicName       []byte
}

type describePartitionResponse struct {
	MessageSize uint32
	Header      *ResponseHeader
	Body        *describePartitionResponseBody
}

type describePartitionResponseBody struct {
	ThrottleTime uint32
	TopicArray   *responseTopicArray
	NextCursor   uint8
}

type responseTopicArray struct {
	ArrayLength       uint8
	ResponseTopicList []*responseTopic
}

type responseTopicName struct {
	StringLength  uint8
	StringContent []byte
}

type responseTopic struct {
	ErrorCode uint16
	TopicName *responseTopicName
	TopicID   [16]byte

	IsInternal uint8

	PartitionArray *partitionArray

	TopicAuthorizedOption uint32
}

type partitionArray struct {
	PartitionArrayLen uint8
	Partitions        []*partition
}

type partition struct {
	ErrorCode              uint16
	PartitionIndex         uint32
	LeaderID               uint32
	LeaderEpoch            uint32
	ReplicaNodeInfo        *replicaNodeInfo
	ISRNodes               *isrNodes
	EligibleLeaderReplicas *eligibleLeaderReplicas
	LastKnownELR           *lastKnownELR
	OfflineReplicas        *offlineReplicas
}

type offlineReplicas struct {
	ArrayLength uint8
}

type eligibleLeaderReplicas struct {
	ArrayLength uint8
}

type lastKnownELR struct {
	ArrayLength uint8
}

type isrNodes struct {
	ArrayLength uint8
	ISRNodeID   uint32
}

type replicaNodeInfo struct {
	ArrayLength   uint8
	ReplicateNode uint32
}

type describeTopicPartitionRequest struct {
	MessageSize uint32
	Header      *CommonAPIRequestHeader
	Body        *describeTopicPartitionRequestBody
}
