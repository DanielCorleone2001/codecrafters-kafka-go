package api

import (
	"encoding/hex"
	"testing"
)

func Test_DescribeTopicPartition(t *testing.T) {
	r := &describeTopicPartitionRequest{
		MessageSize: 0x20,
		Header: &CommonAPIRequestHeader{
			RequestAPIKey:     0x4b,
			RequestAPIVersion: 0,
			CorrelationID:     0x07,
			ClientID: &CommonAPIRequestHeaderClientID{
				Length:   0x0009,
				Contents: []byte("kafka-cli"),
			},
		},
		Body: &describeTopicPartitionRequestBody{
			requestTopicArray: &requestTopicArray{
				ArrayLength: 0x02,
				TopicList: []*requestTopic{
					{
						TopicNameLength: 0x04,
						TopicName:       []byte("foo"),
					},
				},
			},
			ResponsePartitionLimit: 0x00000064,
			Cursor:                 0xff,
		},
	}
	t.Logf("%s", hex.EncodeToString(r.Encode()))
}
