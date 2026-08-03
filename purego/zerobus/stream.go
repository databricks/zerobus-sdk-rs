package zerobus

import (
	"context"

	"github.com/databricks/zerobus-sdk/purego/internal/stream"
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// Stream is an open ingestion stream.
// Use ingest methods to queue records, then Flush/WaitForOffset to confirm.
type Stream struct {
	core *stream.CoreStream[*zerobuspb.EphemeralStreamRequest, *zerobuspb.EphemeralStreamResponse]
	// sdk is the SDK that created this stream. Close deregisters from it so a
	// long-lived SDK does not retain streams the caller has already closed.
	sdk *SDK
}

// IngestRecordOffset queues one record and returns its logical offset.
// It blocks only on backpressure and returns -1 on error.
//
// record is the raw record payload: serialized protobuf bytes for a proto
// stream, or UTF-8 JSON bytes for a JSON stream.
//
// For throughput, queue records in a loop and call Flush once.
func (s *Stream) IngestRecordOffset(record []byte) (int64, error) {
	return s.IngestRecordOffsetContext(context.Background(), record)
}

// IngestRecordOffsetContext is IngestRecordOffset with caller context.
func (s *Stream) IngestRecordOffsetContext(ctx context.Context, record []byte) (int64, error) {
	off, err := s.core.Ingest(ctx, record)
	return off, wrapErr("IngestRecordOffset", err)
}

// IngestRecordsOffset queues records as one batch and returns its logical offset.
// Empty batch returns -1 with nil error; failures return -1 with error.
//
// Prefer this in hot paths to reduce overhead.
func (s *Stream) IngestRecordsOffset(records [][]byte) (int64, error) {
	return s.IngestRecordsOffsetContext(context.Background(), records)
}

// IngestRecordsOffsetContext is IngestRecordsOffset with caller context.
func (s *Stream) IngestRecordsOffsetContext(ctx context.Context, records [][]byte) (int64, error) {
	off, err := s.core.IngestBatch(ctx, records)
	return off, wrapErr("IngestRecordsOffset", err)
}

// Flush waits until all queued records are acknowledged or the stream fails.
func (s *Stream) Flush() error {
	return wrapErr("Flush", s.core.Flush(context.Background()))
}

// FlushContext is Flush with caller context.
func (s *Stream) FlushContext(ctx context.Context) error {
	return wrapErr("Flush", s.core.Flush(ctx))
}

// WaitForOffset waits until offset is acknowledged or the stream fails.
// Prefer Flush for bulk durability checks.
func (s *Stream) WaitForOffset(offset int64) error {
	return wrapErr("WaitForOffset", s.core.WaitForOffset(context.Background(), offset))
}

// WaitForOffsetContext is WaitForOffset with caller context.
func (s *Stream) WaitForOffsetContext(ctx context.Context, offset int64) error {
	return wrapErr("WaitForOffset", s.core.WaitForOffset(ctx, offset))
}

// GetUnackedRecords returns queued records that were never acknowledged.
// Call only after stream close or terminal failure.
func (s *Stream) GetUnackedRecords() ([][]byte, error) {
	recs, err := s.core.GetUnacked()
	return recs, wrapErr("GetUnackedRecords", err)
}

// GetUnackedBatches returns unacknowledged records grouped by ingest call.
func (s *Stream) GetUnackedBatches() ([][][]byte, error) {
	batches, err := s.core.GetUnackedBatches()
	return batches, wrapErr("GetUnackedBatches", err)
}

// Close flushes queued records, tears down the stream, and releases resources.
// It is idempotent.
func (s *Stream) Close() error {
	err := s.core.Close()
	if s.sdk != nil {
		s.sdk.forget(s)
	}
	return wrapErr("Close", err)
}

// terminate tears down the stream without a final flush (used by SDK.Close).
func (s *Stream) terminate() error {
	return wrapErr("Close", s.core.Terminate())
}

// IsClosed reports whether the stream has been closed or has failed terminally.
func (s *Stream) IsClosed() bool {
	return s.core.IsClosed()
}

// ID returns the client-generated stream identifier.
func (s *Stream) ID() string {
	return s.core.ID()
}

// ServerID returns the server-assigned identifier for the latest connection.
func (s *Stream) ServerID() string {
	return s.core.ServerID()
}
