package meta_data

import (
	"github.com/codecrafters-io/kafka-starter-go/app/util"
	"io"
)

var _ RecordValue = &TopicRecord{}

type TopicRecord struct {
	FrameVersion uint8

	RecordType RecordType

	Version uint8

	NameLength uint8

	TopicName []byte

	TopicUUID [16]byte

	TaggedFieldsCount uint8
}

func (TopicRecord) Type() RecordType {
	return RecordType_TopicRecord
}

func decode2TopicRecord(frameVersion uint8, dataSource io.Reader) *TopicRecord {
	t := &TopicRecord{
		FrameVersion: frameVersion,
		RecordType:   RecordType_TopicRecord,
	}
	t.Version = util.ReadLength(1, dataSource)[0]
	t.NameLength = util.ReadLength(1, dataSource)[0]
	t.TopicName = util.ReadLength(int(t.NameLength-1), dataSource)
	copy(t.TopicUUID[:], util.ReadLength(16, dataSource))
	t.TaggedFieldsCount = util.ReadLength(1, dataSource)[0]
	if t.TaggedFieldsCount > 0 {
		util.ReadLength(int(t.TaggedFieldsCount), dataSource)
	}

	return t
}
