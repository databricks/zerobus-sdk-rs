package stream

import (
	"errors"
	"fmt"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// errClosed is returned by buffer operations when the buffer has been closed.
var errClosed = errors.New("stream: buffer closed")

// errWatermarkClosed is returned when a clean Close makes an unacknowledged
// WaitForOffset target permanently unreachable.
var errWatermarkClosed = errors.New("stream: closed before offset was acknowledged")

// errClosedBeforeReady is returned when teardown happens before any successful
// open completes.
var errClosedBeforeReady = errors.New("stream: closed before first open completed")

// ErrPayloadTooLarge marks an ingest call whose encoded request exceeds the
// configured per-message limit.
var ErrPayloadTooLarge = errors.New("stream: ingest payload too large")

// ErrStreamStillActive is returned by GetUnacked when called before the stream
// reaches a terminal/closed state.
var ErrStreamStillActive = errors.New("stream: cannot get unacked records from an active stream")

// ErrOffsetExhausted marks a stream that assigned every logical offset.
var ErrOffsetExhausted = errors.New("stream: logical offset space exhausted")

// errUnsupportedRecordType is returned when no encoder/ackModel exists for a
// record type (e.g. RECORD_TYPE_UNSPECIFIED).
func errUnsupportedRecordType(rt zerobuspb.RecordType) error {
	return fmt.Errorf("stream: unsupported record type %v", rt)
}

// pauseSignal requests a delayed reconnect without consuming a retry.
type pauseSignal struct {
	// duration is the server-requested pause window; zero if unspecified.
	duration time.Duration
	// resumeAt is the effective client-capped reconnect deadline.
	resumeAt time.Time
}

func (pauseSignal) Error() string { return "stream: server requested pause (close-stream signal)" }

// openFailure distinguishes failures before a transport stream is established
// from failures on a live or recovering stream.
type openFailure struct{ cause error }

func (e *openFailure) Error() string { return e.cause.Error() }
func (e *openFailure) Unwrap() error { return e.cause }

// openBudgetExceeded marks a timed-out Open as retryable. Because isRetryable
// consults the outermost self-classifying error before the status code, only wrap
// causes that carry neither a terminal status nor a non-retryable classification
// of their own (see deniesRetry); otherwise a permanent rejection racing the
// deadline would be retried for the full budget.
type openBudgetExceeded struct{ cause error }

func (e *openBudgetExceeded) Error() string {
	return "stream: open budget exceeded: " + e.cause.Error()
}

func (e *openBudgetExceeded) Unwrap() error { return e.cause }

func (*openBudgetExceeded) IsRetryable() bool { return true }
