package stream

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// supervise is the supervisor goroutine. It runs the create→run→recover loop
// until the context is cancelled (Close called), a non-retryable error fires,
// or recovery retries are exhausted. It always closes done on exit.
func (cs *CoreStream) supervise(ctx context.Context) {
	defer close(cs.done)

	var err error
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			// Cancelled by Close — clean exit, no error.
			return
		}

		if attempt > 0 {
			// Log-friendly: caller can observe via IsClosed / GetUnacked.
			if !cs.cfg.RecoveryEnabled || attempt > cs.cfg.RecoveryRetries {
				err = fmt.Errorf("stream: recovery exhausted after %d attempt(s): %w",
					attempt, err)
				break
			}
			select {
			case <-time.After(cs.cfg.RecoveryBackoff):
			case <-ctx.Done():
				return
			}
			// Requeue any items the previous sender observed but didn't ack.
			cs.buf.requeue()
		}

		runErr := cs.runOnce(ctx)

		// ctx cancelled = Clean exit via Close; nil also means clean if the
		// caller already cancelled. Don't treat server-side EOF as clean.
		if ctx.Err() != nil {
			return
		}
		if runErr == nil {
			// Server closed the stream cleanly (EOF). Treat as a retryable
			// disconnect and recover, matching Rust's behaviour.
			runErr = fmt.Errorf("stream: server closed the stream")
		}

		err = runErr
		if !isRetryable(runErr) {
			break
		}
	}

	// Terminal failure path.
	cs.wm.fail(err)
	cs.setTerminalErr(err)

	if cs.callback != nil {
		// Drain the buffer and fire OnError for each unacked item. drain() also
		// closes the buffer and unblocks any enqueue callers, so we must call it
		// here rather than close() + a separate drain in GetUnacked. GetUnacked
		// documents that it is mutually exclusive with AckCallback: when a
		// callback is set, the supervisor owns the drain; when it is nil,
		// GetUnacked drains on demand.
		for _, it := range cs.buf.drain() {
			cs.callback.OnError(it.offset, err)
		}
	} else {
		// No callback: just close the buffer so enqueue callers unblock. The
		// items remain retrievable via GetUnacked, which calls drain() itself.
		cs.buf.close()
	}
}

// isRetryable reports whether err represents a transient failure that warrants
// a stream reconnect. Context cancellation and stream-open validation errors
// (wrong table name, bad record type) are not retryable; transport-level
// failures (connection reset, server going away) are.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// errClosed means the buffer was drained by Close; not retryable.
	if errors.Is(err, errClosed) {
		return false
	}
	// Errors produced by the transport layer's Open (validation failures such
	// as empty table name or unsupported record type) are non-retryable
	// because reconnecting would produce the same error.
	var ve *validationError
	if errors.As(err, &ve) {
		return false
	}
	// All other errors (network failures, server resets, auth rejections after
	// the stream was open) are considered retryable; the supervisor's retry cap
	// is the backstop.
	return true
}

// validationError marks errors that are configuration/validation failures
// rather than transient transport failures.
type validationError struct{ cause error }

func (e *validationError) Error() string { return e.cause.Error() }
func (e *validationError) Unwrap() error { return e.cause }

// wrapValidation wraps err so the supervisor recognises it as non-retryable.
func wrapValidation(err error) error {
	if err == nil {
		return nil
	}
	return &validationError{cause: err}
}
