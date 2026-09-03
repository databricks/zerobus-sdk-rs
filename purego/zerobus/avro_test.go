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
