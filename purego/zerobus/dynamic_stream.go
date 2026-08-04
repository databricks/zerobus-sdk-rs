package zerobus

import (
	"context"
	"fmt"

	"github.com/databricks/zerobus-sdk/purego/internal/dynamicproto"
	"github.com/databricks/zerobus-sdk/purego/internal/stream"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// DynamicProtoStream wraps a proto stream and accepts JSON input payloads.
type DynamicProtoStream struct {
	*Stream
	converter               *dynamicproto.Converter
	conversionGate          chan struct{}
	maxBatchRecords         int
	maxBufferedPayloadBytes int64
}

// MessageDescriptor returns the protobuf descriptor fetched for this stream.
func (s *DynamicProtoStream) MessageDescriptor() protoreflect.MessageDescriptor {
	if s == nil || s.converter == nil {
		return nil
	}
	return s.converter.MessageDescriptor()
}

// IngestJSONOffset converts one JSON payload and queues it for ingestion.
func (s *DynamicProtoStream) IngestJSONOffset(record []byte) (int64, error) {
	return s.IngestJSONOffsetContext(context.Background(), record)
}

// IngestJSONOffsetContext is IngestJSONOffset with caller context.
func (s *DynamicProtoStream) IngestJSONOffsetContext(ctx context.Context, record []byte) (int64, error) {
	if err := s.validateJSONBatch(1, len(record)); err != nil {
		return -1, &Error{Op: "IngestJSONOffset", cause: err, retryable: false}
	}
	if err := s.acquireConversion(ctx); err != nil {
		return -1, &Error{Op: "IngestJSONOffset", cause: err, retryable: false}
	}
	defer s.releaseConversion()
	if err := ctx.Err(); err != nil {
		return -1, &Error{Op: "IngestJSONOffset", cause: err, retryable: false}
	}
	encoded, err := s.encodeJSONRecord(record)
	if err != nil {
		return -1, &Error{Op: "IngestJSONOffset", cause: err, retryable: false}
	}
	return s.Stream.IngestRecordOffsetContext(ctx, encoded)
}

// IngestJSONRecordsOffset converts JSON byte payloads and queues one batch.
func (s *DynamicProtoStream) IngestJSONRecordsOffset(records [][]byte) (int64, error) {
	return s.IngestJSONRecordsOffsetContext(context.Background(), records)
}

// IngestJSONRecordsOffsetContext is IngestJSONRecordsOffset with caller context.
func (s *DynamicProtoStream) IngestJSONRecordsOffsetContext(ctx context.Context, records [][]byte) (int64, error) {
	total, err := totalByteLength(records)
	if err != nil {
		return -1, &Error{Op: "IngestJSONRecordsOffset", cause: err, retryable: false}
	}
	if err := s.validateJSONBatch(len(records), total); err != nil {
		return -1, &Error{Op: "IngestJSONRecordsOffset", cause: err, retryable: false}
	}
	if len(records) == 0 {
		return -1, nil
	}
	if err := s.acquireConversion(ctx); err != nil {
		return -1, &Error{Op: "IngestJSONRecordsOffset", cause: err, retryable: false}
	}
	defer s.releaseConversion()
	batch, err := s.encodeJSONBatchContext(ctx, records)
	if err != nil {
		return -1, &Error{Op: "IngestJSONRecordsOffset", cause: err, retryable: false}
	}
	return s.Stream.IngestRecordsOffsetContext(ctx, batch)
}

func (s *DynamicProtoStream) encodeJSONRecord(record []byte) ([]byte, error) {
	if s == nil || s.converter == nil {
		return nil, fmt.Errorf("dynamic proto stream is not initialized")
	}
	return s.converter.EncodeJSONBytes(record)
}

func (s *DynamicProtoStream) encodeJSONBatchContext(ctx context.Context, records [][]byte) ([][]byte, error) {
	out := make([][]byte, len(records))
	for i := range records {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		encoded, err := s.encodeJSONRecord(records[i])
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		out[i] = encoded
	}
	return out, nil
}

func (s *DynamicProtoStream) validateJSONBatch(recordCount, rawBytes int) error {
	maxRecords := s.maxBatchRecords
	if maxRecords <= 0 {
		maxRecords = stream.DefaultMaxBatchRecords
	}
	if recordCount > maxRecords {
		return fmt.Errorf("%w: %d records exceeds MaxBatchRecords=%d",
			stream.ErrPayloadTooLarge, recordCount, maxRecords)
	}
	maxBufferedBytes := s.maxBufferedPayloadBytes
	if maxBufferedBytes <= 0 {
		maxBufferedBytes = stream.DefaultMaxBufferedPayloadBytes
	}
	if int64(rawBytes) > maxBufferedBytes {
		return fmt.Errorf("%w: raw batch exceeds MaxBufferedPayloadBytes=%d",
			stream.ErrPayloadTooLarge, maxBufferedBytes)
	}
	return nil
}

func (s *DynamicProtoStream) acquireConversion(ctx context.Context) error {
	// Serialize conversion to bound memory before stream backpressure applies.
	if s.conversionGate == nil {
		return ctx.Err()
	}
	select {
	case s.conversionGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *DynamicProtoStream) releaseConversion() {
	if s.conversionGate != nil {
		<-s.conversionGate
	}
}

func totalByteLength(records [][]byte) (int, error) {
	total := 0
	for _, record := range records {
		if len(record) > int(^uint(0)>>1)-total {
			return 0, fmt.Errorf("%w: raw batch size overflows int", stream.ErrPayloadTooLarge)
		}
		total += len(record)
	}
	return total, nil
}
