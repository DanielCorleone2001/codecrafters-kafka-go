package meta_data

import (
	"github.com/codecrafters-io/kafka-starter-go/app/util"
	"io"
)

type RecordType uint8

const (
	RecordType_FeatureLevel    RecordType = 0x0c
	RecordType_TopicRecord     RecordType = 0x02
	RecordType_PartitionRecord RecordType = 0x03
)

type RecordValue interface {
	Type() RecordType
}

type RecordValueDecoder struct{}

func Decode2RecordValue(dataSource io.Reader) RecordValue {
	frameVersion := util.ReadN(1, dataSource)[0]
	recordType := RecordType(util.ReadN(1, dataSource)[0])

	switch recordType {
	case RecordType_PartitionRecord:
		return decode2PartitionRecord(frameVersion, dataSource)
	case RecordType_TopicRecord:
		return decode2TopicRecord(frameVersion, dataSource)
	case RecordType_FeatureLevel:
		return decode2FeatureLevelRecord(frameVersion, dataSource)
	default:
		panic("unsupported record type")
	}
}
