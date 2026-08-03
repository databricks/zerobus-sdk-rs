package zerobus

import (
	"context"
	"fmt"

	"github.com/databricks/zerobus-sdk/purego/internal/dynamicproto"
)

// DynamicProtoStream wraps a proto stream and accepts JSON input payloads.
type DynamicProtoStream struct {
	*Stream
	converter *dynamicproto.Converter
}

// IngestJSONOffset converts one JSON payload and queues it for ingestion.
func (s *DynamicProtoStream) IngestJSONOffset(record []byte) (int64, error) {
	return s.IngestJSONOffsetContext(context.Background(), record)
}

// IngestJSONOffsetContext is IngestJSONOffset with caller context.
func (s *DynamicProtoStream) IngestJSONOffsetContext(ctx context.Context, record []byte) (int64, error) {
	encoded, err := s.encodeJSONRecord(record)
	if err != nil {
		return -1, &Error{Op: "IngestJSONOffset", cause: err, retryable: false}
	}
	return s.Stream.IngestRecordOffsetContext(ctx, encoded)
}

// IngestJSONStringOffset converts one JSON string payload and queues it.
func (s *DynamicProtoStream) IngestJSONStringOffset(record string) (int64, error) {
	return s.IngestJSONStringOffsetContext(context.Background(), record)
}

// IngestJSONStringOffsetContext is IngestJSONStringOffset with caller context.
func (s *DynamicProtoStream) IngestJSONStringOffsetContext(ctx context.Context, record string) (int64, error) {
	return s.IngestJSONOffsetContext(ctx, []byte(record))
}

// IngestJSONRecordsOffset converts JSON byte payloads and queues one batch.
func (s *DynamicProtoStream) IngestJSONRecordsOffset(records [][]byte) (int64, error) {
	return s.IngestJSONRecordsOffsetContext(context.Background(), records)
}

// IngestJSONRecordsOffsetContext is IngestJSONRecordsOffset with caller context.
func (s *DynamicProtoStream) IngestJSONRecordsOffsetContext(ctx context.Context, records [][]byte) (int64, error) {
	batch, err := s.encodeJSONBatch(records)
	if err != nil {
		return -1, &Error{Op: "IngestJSONRecordsOffset", cause: err, retryable: false}
	}
	return s.Stream.IngestRecordsOffsetContext(ctx, batch)
}

// IngestJSONStringsOffset converts JSON string payloads and queues one batch.
func (s *DynamicProtoStream) IngestJSONStringsOffset(records []string) (int64, error) {
	return s.IngestJSONStringsOffsetContext(context.Background(), records)
}

// IngestJSONStringsOffsetContext is IngestJSONStringsOffset with caller context.
func (s *DynamicProtoStream) IngestJSONStringsOffsetContext(ctx context.Context, records []string) (int64, error) {
	batch := make([][]byte, len(records))
	for i := range records {
		batch[i] = []byte(records[i])
	}
	return s.IngestJSONRecordsOffsetContext(ctx, batch)
}

func (s *DynamicProtoStream) encodeJSONRecord(record []byte) ([]byte, error) {
	if s == nil || s.converter == nil {
		return nil, fmt.Errorf("dynamic proto stream is not initialized")
	}
	return s.converter.EncodeJSONBytes(record)
}

func (s *DynamicProtoStream) encodeJSONBatch(records [][]byte) ([][]byte, error) {
	out := make([][]byte, len(records))
	for i := range records {
		encoded, err := s.encodeJSONRecord(records[i])
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		out[i] = encoded
	}
	return out, nil
}
