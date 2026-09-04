//go:build avro

package zerobus

import (
	"testing"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

func TestWithAvroSetsSchemaAndRecordType(t *testing.T) {
	c := defaultStreamConfig()
	WithAvro(`{"type":"record","name":"R","fields":[]}`)(&c)
	if c.recordType != zerobuspb.RecordType_AVRO {
		t.Fatalf("want AVRO record type, got %v", c.recordType)
	}
	if c.avroSchema == "" {
		t.Fatal("want avro schema to be set")
	}
	if c.descriptor != nil {
		t.Fatal("want nil descriptor for an avro stream")
	}
}

func TestValidateStreamArgsRejectsEmptyAvroSchema(t *testing.T) {
	c := defaultStreamConfig()
	WithAvro("")(&c)
	if err := validateStreamArgs("catalog.schema.table", c); err == nil {
		t.Fatal("want error for empty avro schema")
	}
}

func TestEncodeObjectRecord(t *testing.T) {
	s := &Stream{avroSchemaJSON: `{"type":"record","name":"R","fields":[` +
		`{"name":"id","type":"long"},{"name":"name","type":"string"}]}`}

	b, err := s.encodeObjectRecord(AvroRecord{"id": int64(1), "name": "Ada"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(b) == 0 {
		t.Fatal("want non-empty datum")
	}

	bad := &Stream{avroSchemaJSON: "{ not valid avro schema"}
	if _, err := bad.encodeObjectRecord(AvroRecord{"id": int64(1)}); err == nil {
		t.Fatal("want error for malformed schema")
	}
}
