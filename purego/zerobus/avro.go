//go:build avro

package zerobus

import "github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"

// WithAvro selects Avro record encoding (Beta); schemaJSON is the writer schema.
// Ingest pre-encoded datums via IngestRecordOffset. Requires the `avro` build tag.
func WithAvro(schemaJSON string) StreamOption {
	return func(c *streamConfig) {
		c.recordType = zerobuspb.RecordType_AVRO
		c.descriptor = nil
		c.avroSchema = schemaJSON
	}
}
