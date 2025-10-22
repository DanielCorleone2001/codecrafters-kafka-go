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
	"runtime/debug"
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
	Length int32

	Attributes uint8

	TimestampDelta int64

	OffsetDelta int32

	KeyLength int32

	Key []byte

	ValueLength int32

	Value RecordValue

	HeadersArrayCount int32
}

type LogFileParser struct {
	f          *os.File
	fileReader *bufio.Reader
}

func (p *LogFileParser) initDataSource(filePath string) {
	f, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err)
		panic(err)
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
		panic(fmt.Sprintf("read not match,stack:%s", string(debug.Stack())))
	}
	return b
}

func (p *LogFileParser) hasMore() bool {
	if p.fileReader.Buffered() > 0 {
		return true
	}
	_, err := p.fileReader.Peek(BaseOffsetBytes + BatchLengthBytes)
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

	rb.PartitionLeaderEpoch = binary.BigEndian.Uint32(util.ReadN(PartitionLeaderEpochBytes, reader))
	rb.MagicByte = util.ReadN(MagicByteBytes, reader)[0]
	rb.CRCChecksum = binary.BigEndian.Uint32(util.ReadN(CRCChecksumBytes, reader))
	rb.Attributes = binary.BigEndian.Uint16(util.ReadN(AttributesBytes, reader))
	rb.LastOffsetDelta = binary.BigEndian.Uint32(util.ReadN(LastOffsetDeltaBytes, reader))
	rb.BaseTimestamp = binary.BigEndian.Uint64(util.ReadN(BaseTimestampBytes, reader))
	rb.MaxTimestamp = binary.BigEndian.Uint64(util.ReadN(MaxTimestampBytes, reader))
	rb.ProducerID = binary.BigEndian.Uint64(util.ReadN(ProducerIDBytes, reader))
	rb.ProducerEpoch = binary.BigEndian.Uint16(util.ReadN(ProducerEpochBytes, reader))
	rb.BaseSequence = binary.BigEndian.Uint32(util.ReadN(BaseSequenceBytes, reader))
	rb.RecordsLength = binary.BigEndian.Uint32(util.ReadN(RecordsLengthBytes, reader))

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

	r.Length = util.ReadVarint32(reader)

	if r.Length <= 0 {
		return r
	}
	rest := bytes.NewReader(util.ReadN(int(r.Length), reader))
	r.Attributes = util.ReadN(RecordAttributesBytes, rest)[0]
	r.TimestampDelta = util.ReadVarint64(rest)
	r.OffsetDelta = util.ReadVarint32(rest)
	r.KeyLength = util.ReadVarint32(rest)
	if r.KeyLength > 0 {
		r.Key = util.ReadN(int(r.KeyLength), rest)
	}
	r.ValueLength = util.ReadVarint32(rest)
	if r.ValueLength > 0 {
		data := util.ReadN(int(r.ValueLength), rest)
		dataSource := bytes.NewReader(data)
		r.Value = Decode2RecordValue(dataSource)
	}
	r.HeadersArrayCount = util.ReadVarint32(rest)
	return r
}
