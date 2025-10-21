package meta_data

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/codecrafters-io/kafka-starter-go/app/util"
	"io"
	"os"
)

const (
	BaseOffsetBytes           = 8
	BatchLengthBytes          = 4
	PartitionLeaderEpochBytes = 4
	MagicByteBytes            = 1
	CRCChecksumBytes          = 4
	AttributesBytes           = 2
	LastOffsetDeltaBytes      = 4
	BaseTimestampBytes        = 8
	MaxTimestampBytes         = 8
	ProducerIDBytes           = 8
	ProducerEpochBytes        = 2
	BaseSequenceBytes         = 4
	RecordsLengthBytes        = 4
)

type LogRecordBatch struct {
	BaseOffset uint64

	LogRecordBatchLength uint32

	PartitionLeaderEpoch uint32

	MagicByte byte

	CRCChecksum uint32 //CRC32-C algorithm

	Attributes uint16

	LastOffsetDelta uint32

	BaseTimestamp uint64

	MaxTimestamp uint64

	ProducerID uint64

	ProducerEpoch uint16

	BaseSequence uint32

	RecordsLength uint32

	RecordList []*LogRecord
}

type LogRecord struct {
	Length int8

	Attributes uint8

	TimestampDelta int8 // signed

	OffsetDelta int8 // signed

	KeyLength int8 // signed

	Key []byte

	ValueLength int8

	Value RecordValue

	HeadersArrayCount uint8
}

type LogFileParser struct {
	f          *os.File
	fileReader *bufio.Reader
}

func (p *LogFileParser) initDataSource(filePath string) {
	f, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	p.f = f
	p.fileReader = bufio.NewReader(f)
}

func (p *LogFileParser) readN(n int) []byte {
	b := make([]byte, n)
	read, err := p.fileReader.Read(b)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if read != n {
		fmt.Println("read want:", n, ",actual:", read)
		os.Exit(1)
	}
	return b
}

func (p *LogFileParser) hasMore() bool {
	if p.fileReader.Buffered() > 0 {
		return true
	}
	_, err := p.fileReader.Peek(1)
	if err == nil {
		return true
	}
	if errors.Is(err, io.EOF) {
		return false
	}
	return false
}

func (p *LogFileParser) Close() error {
	return p.f.Close()
}

func (p *LogFileParser) ParseAllBatchRecord(filePath string) []*LogRecordBatch {
	p.initDataSource(filePath)

	b := make([]*LogRecordBatch, 0)
	for p.hasMore() {
		b = append(b, p.parseSingleBatchRecord())
	}

	_ = p.Close()

	return b
}

func (p *LogFileParser) parseSingleBatchRecord() *LogRecordBatch {
	rb := &LogRecordBatch{
		BaseOffset:           binary.BigEndian.Uint64(p.readN(BaseOffsetBytes)),
		LogRecordBatchLength: binary.BigEndian.Uint32(p.readN(BatchLengthBytes)),
	}

	reader := bytes.NewBuffer(p.readN(int(rb.LogRecordBatchLength)))

	rb.PartitionLeaderEpoch = binary.BigEndian.Uint32(util.ReadLength(PartitionLeaderEpochBytes, reader))
	rb.MagicByte = util.ReadLength(MagicByteBytes, reader)[0]
	rb.CRCChecksum = binary.BigEndian.Uint32(util.ReadLength(CRCChecksumBytes, reader))
	rb.Attributes = binary.BigEndian.Uint16(util.ReadLength(AttributesBytes, reader))
	rb.LastOffsetDelta = binary.BigEndian.Uint32(util.ReadLength(LastOffsetDeltaBytes, reader))
	rb.BaseTimestamp = binary.BigEndian.Uint64(util.ReadLength(BaseTimestampBytes, reader))
	rb.MaxTimestamp = binary.BigEndian.Uint64(util.ReadLength(MaxTimestampBytes, reader))
	rb.ProducerID = binary.BigEndian.Uint64(util.ReadLength(ProducerIDBytes, reader))
	rb.ProducerEpoch = binary.BigEndian.Uint16(util.ReadLength(ProducerEpochBytes, reader))
	rb.BaseSequence = binary.BigEndian.Uint32(util.ReadLength(BaseSequenceBytes, reader))
	rb.RecordsLength = binary.BigEndian.Uint32(util.ReadLength(RecordsLengthBytes, reader))

	if rb.RecordsLength > 0 {
		rb.RecordList = make([]*LogRecord, 0, rb.RecordsLength)
		for i := uint32(0); i < rb.RecordsLength; i++ {
			rb.RecordList = append(rb.RecordList, p.parseSingleRecord(reader))
		}
	}

	return rb
}

const (
	RecordLengthBytes           = 1
	RecordAttributesBytes       = 1
	RecordTimestampDeltaBytes   = 1
	RecordOffsetDeltaBytes      = 1
	RecordKeyLengthBytes        = 1
	RecordValueLengthBytes      = 1
	RecordHeaderArrayCountBytes = 1
)

func (p *LogFileParser) parseSingleRecord(reader io.Reader) *LogRecord {
	r := &LogRecord{}

	r.Length = int8(util.ReadLength(RecordLengthBytes, reader)[0])
	r.Attributes = util.ReadLength(RecordAttributesBytes, reader)[0]
	r.TimestampDelta = int8(util.ReadLength(RecordTimestampDeltaBytes, reader)[0])
	r.OffsetDelta = int8(util.ReadLength(RecordOffsetDeltaBytes, reader)[0])
	r.KeyLength = int8(util.ReadLength(RecordKeyLengthBytes, reader)[0])
	if r.KeyLength > 0 {
		r.Key = util.ReadLength(int(r.KeyLength), reader)
	}
	r.ValueLength = int8(util.ReadLength(RecordValueLengthBytes, reader)[0])
	if r.ValueLength > 0 {
		data := util.ReadLength(int(r.KeyLength), reader)
		dataSource := bytes.NewBuffer(data)
		r.Value = Decode2RecordValue(dataSource)
	}
	r.HeadersArrayCount = util.ReadLength(RecordHeaderArrayCountBytes, reader)[0]
	return r
}
