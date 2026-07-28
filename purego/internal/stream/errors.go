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

// ErrPayloadTooLarge marks an ingest call whose encoded request exceeds the
// configured per-message limit.
var ErrPayloadTooLarge = errors.New("stream: ingest payload too large")

// ErrStreamStillActive is returned by GetUnacked when called before the stream
// reaches a terminal/closed state.
var ErrStreamStillActive = errors.New("stream: cannot get unacked records from an active stream")

// errUnsupportedRecordType is returned when no encoder/ackModel exists for a
// record type (e.g. RECORD_TYPE_UNSPECIFIED).
func errUnsupportedRecordType(rt zerobuspb.RecordType) error {
	return fmt.Errorf("stream: unsupported record type %v", rt)
}

// pauseSignal requests a delayed reconnect without consuming a retry.
type pauseSignal struct {
	// duration is the server-requested pause window; zero if unspecified.
	duration time.Duration
}

func (pauseSignal) Error() string { return "stream: server requested pause (close-stream signal)" }

// openFailure marks an error already handled by the transport Open path.
type openFailure struct{ cause error }

func (e *openFailure) Error() string { return e.cause.Error() }

func (e *openFailure) Unwrap() error { return e.cause }

// openBudgetExceeded marks a timed-out Open as retryable.
type openBudgetExceeded struct{ cause error }

func (e *openBudgetExceeded) Error() string {
	return "stream: open budget exceeded: " + e.cause.Error()
}

func (e *openBudgetExceeded) Unwrap() error { return e.cause }

func (*openBudgetExceeded) IsRetryable() bool { return true }
