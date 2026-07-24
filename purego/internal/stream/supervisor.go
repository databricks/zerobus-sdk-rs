package stream

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// supervise runs the connect, stream, and recovery lifecycle.
// Successful connections reset the retry budget.
func (cs *CoreStream[Req, Resp]) supervise(ctx context.Context) {
	defer close(cs.done)

	var err error
	failedAttempts := 0
	for {
		if ctx.Err() != nil {
			// Cancelled by Close — clean exit, no error.
			return
		}

		if failedAttempts > 0 {
			if !cs.cfg.Recovery.enabled() || failedAttempts > cs.cfg.RecoveryRetries {
				err = fmt.Errorf("stream: recovery exhausted after %d attempt(s): %w",
					failedAttempts, err)
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

		runErr, healthy := cs.runOnce(ctx)

		// ctx cancelled = clean exit via Close. Don't treat server-side EOF as clean.
		if ctx.Err() != nil {
			return
		}

		// Reset the retry budget after a successful Open.
		if healthy {
			failedAttempts = 0
		}

		// Pauses do not consume the retry budget.
		var ps pauseSignal
		if errors.As(runErr, &ps) {
			if !cs.waitPause(ctx, ps.duration) {
				return // Close cancelled ctx during the pause.
			}
			cs.buf.requeue()
			continue
		}

		if runErr == nil {
			// Server closed the stream cleanly (EOF). Treat as a retryable
			// disconnect and recover.
			runErr = fmt.Errorf("stream: server closed the stream")
		}

		err = runErr
		if !isRetryable(runErr) {
			break
		}
		failedAttempts++
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

// waitPause sleeps for the server-requested pause window before reconnecting,
// capped by StreamPausedMaxWait (min of the two; a non-positive cap means "no
// client cap"). Returns false if ctx was cancelled (Close) during the wait, so
// the caller stops instead of reconnecting. A non-positive effective wait
// reconnects immediately.
func (cs *CoreStream[Req, Resp]) waitPause(ctx context.Context, serverDuration time.Duration) bool {
	wait := serverDuration
	if cap := cs.cfg.StreamPausedMaxWait; cap > 0 && (wait <= 0 || cap < wait) {
		wait = cap
	}
	if wait <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// retryableError lets errors classify their retry behavior.
type retryableError interface {
	IsRetryable() bool
}

// isRetryable reports whether reconnecting may succeed.
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
	// Validation failures are deterministic.
	var ve *validationError
	if errors.As(err, &ve) {
		return false
	}
	// Honor self-classifying wrapped errors.
	var re retryableError
	if errors.As(err, &re) {
		return re.IsRetryable()
	}
	// Treat remaining failures as transient.
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
