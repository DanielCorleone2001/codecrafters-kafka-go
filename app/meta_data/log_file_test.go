package meta_data

import (
	"encoding/json"
	"testing"
)

func Test_Parse(t *testing.T) {
	path := "/Users/daniel/code/go/codecrafter/codecrafters-kafka-go/app/meta_data/a.bin"
	p := &LogFileParser{}
	l := p.ParseAllBatchRecord(path)
	f, _ := json.MarshalIndent(l, " ", "\t")
	t.Logf("%s", string(f))
}
