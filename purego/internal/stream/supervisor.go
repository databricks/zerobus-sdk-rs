package stream

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// supervise is the supervisor goroutine. It runs the create→run→recover loop
// until the context is cancelled (Close called), a non-retryable error fires,
// or consecutive recovery retries are exhausted. It always closes done on
// exit.
//
// The retry budget is per episode: `failedAttempts` counts only *consecutive*
// failed reconnects and resets to zero whenever a stream connects and runs
// successfully. A long-lived stream that disconnects occasionally therefore
// is not doomed after RecoveryRetries lifetime disconnects — each disconnect
// starts a fresh episode with the full budget. This mirrors the Rust core,
// which builds a fresh attempt counter per recovery loop iteration.
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

		// ctx cancelled = clean exit via Close. Don't treat server-side EOF
		// as clean.
		if ctx.Err() != nil {
			return
		}

		// A stream that successfully opened and ran resets the per-episode
		// budget: the failure that ended it (if any) begins a fresh recovery
		// episode rather than continuing the prior reconnect streak.
		if healthy {
			failedAttempts = 0
		}

		// A server-requested pause (CloseStreamSignal) is not a failure: wait
		// the requested window, then reconnect without consuming the recovery
		// budget. Ingest keeps buffering meanwhile; unacked records are
		// requeued on the next attempt.
		//
		// If recovery is disabled, though, we honor that: don't reconnect
		// after a pause. Treat it as a terminal failure so callers see the
		// signal rather than silently ingesting into a dead stream.
		var ps pauseSignal
		if errors.As(runErr, &ps) {
			if !cs.cfg.Recovery.enabled() {
				err = fmt.Errorf("stream: server requested pause and recovery is disabled: %w", runErr)
				break
			}
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

	// Terminal failure path. Fail the watermark first so any waiters unblock
	// promptly with the terminal error; the callback dispatch and buffer
	// drain run afterwards.
	cs.wm.fail(err)
	cs.setTerminalErr(err)

	// Drain the buffer once. We always preserve the payloads for GetUnacked
	// AND, if a callback is registered, dispatch per-offset OnError events —
	// both signals converge on the same underlying records. This matches the
	// Rust SDK, which retains failed records via failed_records regardless of
	// callback registration.
	items := cs.buf.drain()
	cs.setRetainedFailed(items)
	for _, it := range items {
		cs.dispatcher.enqueueError(it.offset, err)
	}
	// Ensure the buffer is closed (drain marks it closed already; belt and
	// braces).
	cs.buf.close()
}

// waitPause sleeps for the residual pause window before reconnecting. The
// receiver already drained acks against effectivePauseWait(cfg, duration) and
// exited when either drain completed or that deadline expired, so this
// supplementary wait is normally zero-length; it exists to preserve prior
// semantics for callers that configured a cap larger than the drain window or
// used PauseWaitServer without the receiver seeing any acks. Returns false if
// ctx was cancelled (Close) during the wait.
func (cs *CoreStream[Req, Resp]) waitPause(ctx context.Context, serverDuration time.Duration) bool {
	// The receiver already applied the per-signal cap via
	// effectivePauseWait; no additional wait is required here. Kept for a
	// last-chance ctx check so Close during pause returns promptly.
	_ = serverDuration
	return ctx.Err() == nil
}

// retryableError is any error that self-classifies its retryability. Errors
// from the transport and auth layers (e.g. TokenError from an OAuth mint)
// implement it; the supervisor honors their verdict rather than blindly
// retrying every non-context error. Structural so we don't have to import
// auth or transport just to name their concrete types.
type retryableError interface {
	IsRetryable() bool
}

// isRetryable reports whether err represents a transient failure that warrants
// a stream reconnect. Context cancellation, stream-open validation errors
// (wrong table name, bad record type), and errors that self-report as
// non-retryable (e.g. a revoked-credentials OAuth mint failure) are not
// retryable; transport-level failures (connection reset, server going away) are.
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
	// Honor any layer's self-reported retryability verdict (e.g. OAuth
	// TokenError). Walk the unwrap chain so a wrapped mint failure is still seen.
	var re retryableError
	if errors.As(err, &re) {
		return re.IsRetryable()
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
