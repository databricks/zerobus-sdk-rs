package stream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/transport"
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
// starts a fresh episode with the full budget: a long-lived stream that
// disconnects occasionally is not doomed by consecutive-lifetime failures.
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
			// runOnce requeues on each successful Open, so we do not duplicate
			// that work here.
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
			// The receiver already applied effectivePauseWait(cfg, duration)
			// during pause drain; no additional wait is required here. A
			// concurrent Close would have short-circuited the loop above
			// via the ctx.Err() check, so we don't repeat that check.
			continue
		}

		if runErr == nil {
			// Server closed the stream cleanly (EOF). Treat as a retryable
			// disconnect and recover.
			runErr = fmt.Errorf("stream: server closed the stream")
		}

		// Mid-stream auth rejection means the cached credential the transport
		// attached at Open is no longer accepted (e.g. token rotated or
		// revoked). Drop it so the next Open re-mints via GetHeaders instead
		// of reconnecting with the same stale value.
		if transport.IsAuthRejection(runErr) && cs.params.HeadersProvider != nil {
			cs.params.HeadersProvider.Invalidate(ctx, cs.params.TableName)
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
	// AND, if a callback is registered, dispatch per-offset OnError events;
	// both signals converge on the same underlying records rather than
	// being mutually exclusive.
	items := cs.buf.drain()
	cs.setRetainedFailed(items)
	for _, it := range items {
		cs.dispatcher.enqueueError(it.offset, err)
	}
	// drain() marks the buffer closed; the extra close call is defensive.
	cs.buf.close()
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
	// Self-classifying retryable errors take precedence over the generic
	// context-error check: an internal per-attempt budget (e.g. an
	// openBudgetExceeded wrapping context.DeadlineExceeded) wants to remain
	// retryable so RecoveryRetries actually governs stalled dials.
	// Validation errors and errClosed are checked first though, because
	// they wrap non-retryable causes regardless of self-classification.
	var ve *validationError
	if errors.As(err, &ve) {
		return false
	}
	if errors.Is(err, errClosed) {
		return false
	}
	var re retryableError
	if errors.As(err, &re) {
		return re.IsRetryable()
	}
	// Bare ctx-cancel / deadline (without a self-classifying wrapper) is
	// non-retryable: those come from the caller/supervisor cancelling us,
	// not an internal budget.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// All other errors (network failures, server resets, auth rejections
	// after the stream was open) are considered retryable; the supervisor's
	// retry cap is the backstop.
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
