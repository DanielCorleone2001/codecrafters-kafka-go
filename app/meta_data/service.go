package meta_data

import (
	"bytes"
	"errors"
	"sync"
)

var (
	initOnce sync.Once
	s        = &svc{}
)

const (
	metaDataLogFilePath = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
)

type MetaDataService interface {
	QueryTopic(topicName []byte) (*TopicRecord, error)

	QueryTopicPartitions(topicName []byte) ([]*PartitionRecord, error)
}

var _ MetaDataService = &svc{}

type svc struct {
	dataSource []*LogRecordBatch
}

func (s *svc) QueryTopicPartitions(topicName []byte) ([]*PartitionRecord, error) {
	topic, err := s.QueryTopic(topicName)
	if err != nil {
		return nil, err
	}
	pr := make([]*PartitionRecord, 0)

	for _, batchRecord := range s.dataSource {
		for _, record := range batchRecord.RecordList {
			if record.Value.Type() == RecordType_PartitionRecord {
				p := record.Value.(*PartitionRecord)
				if p.TopicUUID == topic.TopicUUID {
					pr = append(pr, p)
				}
			}
		}
	}
	return pr, nil
}

var (
	TopicNotFound = errors.New("topic not found")
)

func GetMetaDataService() MetaDataService {
	initOnce.Do(func() {
		p := LogFileParser{}
		s.dataSource = p.ParseAllBatchRecord(metaDataLogFilePath)
	})
	return s
}

func (s *svc) QueryTopic(topicName []byte) (*TopicRecord, error) {
	for _, batchRecord := range s.dataSource {
		for _, record := range batchRecord.RecordList {
			if record.Value.Type() == RecordType_TopicRecord {
				t := record.Value.(*TopicRecord)
				if bytes.Equal(t.TopicName, topicName) {
					return t, nil
				}
			}
		}
	}
	return nil, TopicNotFound
}
