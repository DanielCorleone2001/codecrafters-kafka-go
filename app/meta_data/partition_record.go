package meta_data

import (
	"encoding/binary"
	"github.com/codecrafters-io/kafka-starter-go/app/util"
	"io"
)

var _ RecordValue = &PartitionRecord{}

type PartitionRecord struct {
	FrameVersion uint8

	RecordType RecordType

	Version uint8

	PartitionID uint32

	TopicUUID [16]byte

	LengthOfReplicaArray uint8

	ReplicaArray uint32

	LengthOfInSyncReplicaArray uint8

	InSyncReplicaArray uint32

	LengthOfRemovingReplicaArray uint8

	LengthOfAddingReplicaArray uint8

	LeaderID uint32

	LeaderEpoch uint32

	PartitionEpoch uint32

	LengthOfDirectoriesArray uint8

	DirectoriesArray [16]byte

	TaggedFieldsCount uint8
}

func (PartitionRecord) Type() RecordType {
	return RecordType_PartitionRecord
}

func decode2PartitionRecord(frameVersion uint8, dataSource io.Reader) *PartitionRecord {
	p := &PartitionRecord{
		FrameVersion: frameVersion,
		RecordType:   RecordType_PartitionRecord,
	}
	p.Version = util.ReadN(1, dataSource)[0]
	p.PartitionID = binary.BigEndian.Uint32(util.ReadN(4, dataSource))
	copy(p.TopicUUID[:], util.ReadN(16, dataSource))
	p.LengthOfReplicaArray = util.ReadN(1, dataSource)[0]
	p.ReplicaArray = binary.BigEndian.Uint32(util.ReadN(4, dataSource))
	p.LengthOfInSyncReplicaArray = util.ReadN(1, dataSource)[0]
	p.InSyncReplicaArray = binary.BigEndian.Uint32(util.ReadN(4, dataSource))
	p.LengthOfRemovingReplicaArray = util.ReadN(1, dataSource)[0]
	p.LengthOfAddingReplicaArray = util.ReadN(1, dataSource)[0]
	p.LeaderID = binary.BigEndian.Uint32(util.ReadN(4, dataSource))
	p.LeaderEpoch = binary.BigEndian.Uint32(util.ReadN(4, dataSource))
	p.PartitionEpoch = binary.BigEndian.Uint32(util.ReadN(4, dataSource))
	p.LengthOfDirectoriesArray = util.ReadN(1, dataSource)[0]
	copy(p.DirectoriesArray[:], util.ReadN(16, dataSource))
	p.TaggedFieldsCount = util.ReadN(1, dataSource)[0]
	if p.TaggedFieldsCount > 0 {
		util.ReadN(int(p.TaggedFieldsCount), dataSource)
	}
	return p
}
