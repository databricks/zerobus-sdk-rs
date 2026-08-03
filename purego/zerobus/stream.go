package zerobus

import (
	"context"

	"github.com/databricks/zerobus-sdk/purego/internal/stream"
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// Stream is an open ingestion stream. Records are queued with IngestRecordOffset
// / IngestRecordsOffset and confirmed durable with Flush or WaitForOffset.
//
// A Stream is safe for concurrent use by multiple goroutines. Ingestion is
// asynchronous: the ingest methods return once the record is queued and never
// wait for a server round-trip. The caller must Close the stream when done.
type Stream struct {
	core *stream.CoreStream[*zerobuspb.EphemeralStreamRequest, *zerobuspb.EphemeralStreamResponse]
	// sdk is the SDK that created this stream. Close deregisters from it so a
	// long-lived SDK does not retain streams the caller has already closed.
	sdk *SDK
}

// IngestRecordOffset queues one record and returns the logical offset assigned
// to it. It returns as soon as the record is buffered; sending and
// acknowledgement happen in the background. If the buffer is full it blocks for
// backpressure until space frees up or the stream fails. On error it returns
// offset -1, matching the original Go SDK.
//
// record is the raw record payload: serialized protobuf bytes for a proto
// stream, or UTF-8 JSON bytes for a JSON stream.
//
// The returned offset is a handle to wait on later with WaitForOffset — not a
// signal to wait now. For throughput, queue records in a loop and call Flush
// once; waiting after every record collapses throughput to one record per
// round-trip.
func (s *Stream) IngestRecordOffset(record []byte) (int64, error) {
	return s.IngestRecordOffsetContext(context.Background(), record)
}

// IngestRecordOffsetContext is IngestRecordOffset with a caller-supplied
// context. Cancellation only interrupts waiting for buffer capacity; once the
// record is queued, its lifecycle is owned by the stream.
func (s *Stream) IngestRecordOffsetContext(ctx context.Context, record []byte) (int64, error) {
	off, err := s.core.Ingest(ctx, record)
	return off, wrapErr("IngestRecordOffset", err)
}

// IngestRecordsOffset queues records as one atomic batch and returns the single
// logical offset covering the whole batch. The server acknowledges the batch
// atomically. An empty batch is a no-op that returns -1 with no error. On error
// it returns offset -1, matching the original Go SDK.
//
// Prefer this over IngestRecordOffset in hot paths: one batch is one buffer
// entry and one ack, amortizing per-call overhead. Each element is a raw record
// payload (proto bytes or JSON bytes) as for IngestRecordOffset.
func (s *Stream) IngestRecordsOffset(records [][]byte) (int64, error) {
	return s.IngestRecordsOffsetContext(context.Background(), records)
}

// IngestRecordsOffsetContext is IngestRecordsOffset with a caller-supplied
// context. Cancellation only interrupts waiting for buffer capacity; once the
// batch is queued, its lifecycle is owned by the stream.
func (s *Stream) IngestRecordsOffsetContext(ctx context.Context, records [][]byte) (int64, error) {
	off, err := s.core.IngestBatch(ctx, records)
	return off, wrapErr("IngestRecordsOffset", err)
}

// Flush blocks until every record queued so far has been acknowledged by the
// server, returning nil once all are durable. This is the idiomatic way to
// confirm durability for high-throughput ingestion: queue records in a loop,
// then call Flush once.
//
// It returns an error if the stream fails or the flush timeout
// (WithFlushTimeout) expires. The wait is bounded by that timeout.
func (s *Stream) Flush() error {
	return wrapErr("Flush", s.core.Flush(context.Background()))
}

// FlushContext is Flush with a caller-supplied context that can cancel or
// shorten the wait. The effective deadline is the earlier of ctx's deadline and
// the configured flush timeout.
func (s *Stream) FlushContext(ctx context.Context) error {
	return wrapErr("Flush", s.core.Flush(ctx))
}

// WaitForOffset blocks until the server has acknowledged every record up to and
// including offset, or the stream fails. Because acks are ordered and the
// watermark is monotonic, waiting on the last offset of a group confirms all
// prior offsets too.
//
// Use it to confirm a specific record before continuing; prefer Flush for bulk
// durability. offset must be one returned by a successful ingest call. The wait
// is bounded by the configured flush timeout.
func (s *Stream) WaitForOffset(offset int64) error {
	return wrapErr("WaitForOffset", s.core.WaitForOffset(context.Background(), offset))
}

// WaitForOffsetContext is WaitForOffset with a caller-supplied context that can
// cancel or shorten the wait, bounded by the configured flush timeout.
func (s *Stream) WaitForOffsetContext(ctx context.Context, offset int64) error {
	return wrapErr("WaitForOffset", s.core.WaitForOffset(ctx, offset))
}

// GetUnackedRecords returns the records that were queued but never acknowledged,
// one entry per record, so a caller can persist or replay them after a failure.
//
// It must be called only after the stream has closed or failed terminally;
// calling it on an active stream returns an error. The result is a fresh copy on
// every call and never aliases internal buffers, so a diagnostic read is
// repeatable and non-destructive.
func (s *Stream) GetUnackedRecords() ([][]byte, error) {
	recs, err := s.core.GetUnacked()
	return recs, wrapErr("GetUnackedRecords", err)
}

// GetUnackedBatches is the batch-preserving form of GetUnackedRecords: it groups
// unacknowledged records as they were submitted, one entry per ingest call, in
// offset order. Prefer it when replaying, since each group can be resubmitted as
// one batch to reproduce the original durability boundaries.
func (s *Stream) GetUnackedBatches() ([][][]byte, error) {
	batches, err := s.core.GetUnackedBatches()
	return batches, wrapErr("GetUnackedBatches", err)
}

// Close flushes every record queued before the close boundary, then tears the
// stream down and releases its resources. It is idempotent, blocks until
// teardown completes, and returns the same durability result to every caller.
//
// If the flush cannot complete within the flush timeout, Close proceeds with
// teardown and any remaining records are abandoned — retrievable via
// GetUnackedRecords or reported through the ack callback's OnError.
func (s *Stream) Close() error {
	err := s.core.Close()
	if s.sdk != nil {
		s.sdk.forget(s)
	}
	return wrapErr("Close", err)
}

// terminate tears the stream down without the final flush Close performs. It is
// the SDK.Close path: the shared connection is going away, so waiting for
// acknowledgements would only stall shutdown. Close and terminate share one
// once-guard, so whichever runs first decides the result both report.
func (s *Stream) terminate() error {
	return wrapErr("Close", s.core.Terminate())
}

// IsClosed reports whether the stream has been closed or has failed terminally.
func (s *Stream) IsClosed() bool {
	return s.core.IsClosed()
}

// ID returns the stable client-generated stream identifier, minted once when the
// stream is created and unchanged across recovery reconnects. Use it to correlate
// log lines for a logical stream. See ServerID for the per-connection identifier.
func (s *Stream) ID() string {
	return s.core.ID()
}

// ServerID returns the identifier the server assigned to the most recently
// opened connection. Unlike ID it changes on every reconnect, and it is empty
// until the first connection is established.
func (s *Stream) ServerID() string {
	return s.core.ServerID()
}
