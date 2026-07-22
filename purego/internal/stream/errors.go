package stream

import (
	"errors"
	"fmt"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// errClosed is returned by buffer operations when the buffer has been closed.
var errClosed = errors.New("stream: buffer closed")

// ErrPayloadTooLarge is returned by Ingest / IngestBatch when the aggregate
// caller-supplied payload exceeds the configured MaxPayloadBytes cap. It is
// deterministic input validation, not a transport error, so recovery does not
// count against the retry budget.
var ErrPayloadTooLarge = errors.New("stream: ingest payload exceeds MaxPayloadBytes")

// errUnsupportedRecordType is returned when no encoder/ackModel exists for a
// record type (e.g. RECORD_TYPE_UNSPECIFIED).
func errUnsupportedRecordType(rt zerobuspb.RecordType) error {
	return fmt.Errorf("stream: unsupported record type %v", rt)
}

// pauseSignal is the cause runOnce returns when the server sent a
// CloseStreamSignal: the server is about to close this stream and the client
// should pause (stop sending, keep buffering and draining acks) then reconnect,
// rather than treat it as a failure that counts against the recovery budget. It
// carries the server-requested pause duration.
type pauseSignal struct {
	// duration is the server-requested pause window; zero if unspecified.
	duration time.Duration
}

func (pauseSignal) Error() string { return "stream: server requested pause (close-stream signal)" }

// openBudgetExceeded marks an Open attempt that exceeded the internal
// RecoveryTimeout budget (as opposed to the caller cancelling the supervisor
// ctx). It self-classifies as retryable so RecoveryRetries governs a stalled
// dial — a single slow Open should not permanently kill the stream while
// budget remains. Caller-side cancellation continues to unwind cleanly
// because the outer ctx path is checked separately in the supervisor.
type openBudgetExceeded struct{ cause error }

func (e *openBudgetExceeded) Error() string {
	return "stream: open budget exceeded: " + e.cause.Error()
}
func (e *openBudgetExceeded) Unwrap() error { return e.cause }

// IsRetryable makes openBudgetExceeded a self-classifying retryable error via
// the retryableError structural interface honoured by isRetryable.
func (*openBudgetExceeded) IsRetryable() bool { return true }
