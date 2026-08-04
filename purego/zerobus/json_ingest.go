package zerobus

import (
	"context"
	"fmt"

	"github.com/databricks/zerobus-sdk/purego/internal/stream"
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// MessageDescriptor returns the protobuf descriptor configured for this stream.
// It returns nil for JSON streams or unsupported descriptor formats.
func (s *Stream) MessageDescriptor() protoreflect.MessageDescriptor {
	if s == nil || s.jsonConverter == nil {
		return nil
	}
	return s.jsonConverter.MessageDescriptor()
}

// IngestJSONOffset queues one JSON payload.
// Proto streams convert it to protobuf before ingestion.
func (s *Stream) IngestJSONOffset(record []byte) (int64, error) {
	return s.IngestJSONOffsetContext(context.Background(), record)
}

// IngestJSONOffsetContext is IngestJSONOffset with caller context.
func (s *Stream) IngestJSONOffsetContext(ctx context.Context, record []byte) (int64, error) {
	if s.recordType == zerobuspb.RecordType_JSON {
		offset, err := s.core.Ingest(ctx, record)
		return offset, wrapErr("IngestJSONOffset", err)
	}
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
	offset, err := s.core.Ingest(ctx, encoded)
	return offset, wrapErr("IngestJSONOffset", err)
}

// IngestJSONRecordsOffset queues JSON payloads as one batch.
// Proto streams convert them to protobuf before ingestion.
func (s *Stream) IngestJSONRecordsOffset(records [][]byte) (int64, error) {
	return s.IngestJSONRecordsOffsetContext(context.Background(), records)
}

// IngestJSONRecordsOffsetContext is IngestJSONRecordsOffset with caller context.
func (s *Stream) IngestJSONRecordsOffsetContext(ctx context.Context, records [][]byte) (int64, error) {
	if s.recordType == zerobuspb.RecordType_JSON {
		offset, err := s.core.IngestBatch(ctx, records)
		return offset, wrapErr("IngestJSONRecordsOffset", err)
	}
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
	offset, err := s.core.IngestBatch(ctx, batch)
	return offset, wrapErr("IngestJSONRecordsOffset", err)
}

func (s *Stream) encodeJSONRecord(record []byte) ([]byte, error) {
	if s == nil || s.recordType != zerobuspb.RecordType_PROTO {
		return nil, fmt.Errorf("JSON conversion requires a proto stream")
	}
	if s.jsonConverterErr != nil {
		return nil, fmt.Errorf("JSON conversion is unavailable: %w", s.jsonConverterErr)
	}
	if s.jsonConverter == nil {
		return nil, fmt.Errorf("JSON conversion is unavailable")
	}
	return s.jsonConverter.EncodeJSONBytes(record)
}

func (s *Stream) encodeJSONBatchContext(ctx context.Context, records [][]byte) ([][]byte, error) {
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

func (s *Stream) validateJSONBatch(recordCount, rawBytes int) error {
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

func (s *Stream) acquireConversion(ctx context.Context) error {
	if s.jsonConverterErr != nil {
		return fmt.Errorf("JSON conversion is unavailable: %w", s.jsonConverterErr)
	}
	if s.conversionGate == nil {
		return fmt.Errorf("JSON conversion is unavailable")
	}
	select {
	case s.conversionGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Stream) releaseConversion() {
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
