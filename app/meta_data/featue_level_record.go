package meta_data

import (
	"encoding/binary"
	"github.com/codecrafters-io/kafka-starter-go/app/util"
	"io"
)

var _ RecordValue = &FeatureLevelRecord{}

type FeatureLevelRecord struct {
	FrameVersion uint8

	RecordType RecordType

	Version uint8

	NameLength uint8

	Name []byte

	FeatureLevel uint16

	TaggedFieldsCount uint8
}

func (f FeatureLevelRecord) Type() RecordType {
	return RecordType_FeatureLevel
}

func decode2FeatureLevelRecord(frameVersion uint8, dataSource io.Reader) *FeatureLevelRecord {
	f := &FeatureLevelRecord{
		FrameVersion: frameVersion,
		RecordType:   RecordType_FeatureLevel,
	}

	f.Version = util.ReadN(1, dataSource)[0]
	f.NameLength = util.ReadN(1, dataSource)[0]
	f.Name = util.ReadN(int(f.NameLength-1), dataSource)
	f.FeatureLevel = binary.BigEndian.Uint16(util.ReadN(2, dataSource))
	f.TaggedFieldsCount = util.ReadN(1, dataSource)[0]
	if f.TaggedFieldsCount > 0 {
		util.ReadN(int(f.TaggedFieldsCount), dataSource)
	}
	return f
}
