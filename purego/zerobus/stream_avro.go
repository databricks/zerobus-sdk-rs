//go:build avro

package zerobus

import (
	"context"
	"fmt"

	"github.com/hamba/avro/v2"
)

// IngestAvroRecordOffset encodes one AvroRecord against the stream's writer
// schema and queues it, returning its logical offset (-1 on error).
//
// Requires the `avro` build tag. For pre-encoded datums use IngestRecordOffset.
// For throughput, queue records in a loop and call Flush once.
func (s *Stream) IngestAvroRecordOffset(record AvroRecord) (int64, error) {
	return s.IngestAvroRecordOffsetContext(context.Background(), record)
}

// IngestAvroRecordOffsetContext is IngestAvroRecordOffset with caller context.
func (s *Stream) IngestAvroRecordOffsetContext(ctx context.Context, record AvroRecord) (int64, error) {
	b, err := s.encodeObjectRecord(record)
	if err != nil {
		return -1, wrapErr("IngestAvroRecordOffset", err)
	}
	off, err := s.core.Ingest(ctx, b)
	return off, wrapErr("IngestAvroRecordOffset", err)
}

// IngestAvroRecordsOffset encodes AvroRecords and queues them as one batch.
// Empty batch returns -1 with nil error; failures return -1.
func (s *Stream) IngestAvroRecordsOffset(records []AvroRecord) (int64, error) {
	return s.IngestAvroRecordsOffsetContext(context.Background(), records)
}

// IngestAvroRecordsOffsetContext is IngestAvroRecordsOffset with caller context.
func (s *Stream) IngestAvroRecordsOffsetContext(ctx context.Context, records []AvroRecord) (int64, error) {
	b := make([][]byte, len(records))
	for i, r := range records {
		bs, err := s.encodeObjectRecord(r)
		if err != nil {
			return -1, wrapErr("IngestAvroRecordsOffset", fmt.Errorf("record %d: %w", i, err))
		}
		b[i] = bs
	}
	off, err := s.core.IngestBatch(ctx, b)
	return off, wrapErr("IngestAvroRecordsOffset", err)
}

// encodeObjectRecord encodes an AvroRecord to a raw Avro datum against the
// stream's writer schema, parsed once and cached.
func (s *Stream) encodeObjectRecord(rec AvroRecord) ([]byte, error) {
	s.avroSchemaOnce.Do(func() {
		schema, err := avro.Parse(s.avroSchemaJSON)
		if err != nil {
			s.avroSchemaErr = fmt.Errorf("parse avro schema: %w", err)
			return
		}
		s.avroSchema = schema
	})
	if s.avroSchemaErr != nil {
		return nil, s.avroSchemaErr
	}
	b, err := avro.Marshal(s.avroSchema.(avro.Schema), map[string]any(rec))
	if err != nil {
		return nil, fmt.Errorf("avro encode: %w", err)
	}
	return b, nil
}
