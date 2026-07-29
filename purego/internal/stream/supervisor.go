package stream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/transport"
)

// supervise runs the connect, stream, and recovery lifecycle.
// Successful connections reset the retry budget.
func (cs *CoreStream[Req, Resp]) supervise(ctx context.Context) {
	defer func() {
		if ctx.Err() != nil {
			// Wake waiters whose targets are now unreachable.
			cs.wm.closeForClean()
			if cs.dispatcher != nil {
				items := cs.buf.drain()
				cs.retainUnacked(items)
				if len(items) > 0 {
					cs.dispatcher.enqueueErrors(
						items[0].offset, items[len(items)-1].offset, errWatermarkClosed,
					)
				}
			}
		}
		// Publish completion before waiting for callbacks.
		close(cs.done)
		cs.dispatcher.shutdown(cs.cfg.CallbackTeardownTimeout)
	}()

	var err error
	failedAttempts := 0
	openedOnce := false
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
			// runOnce requeues after Open succeeds.
		}

		runErr, resetRecoveryBudget := cs.runOnce(ctx)

		// ctx cancelled = clean exit via Close. Don't treat server-side EOF as clean.
		if ctx.Err() != nil {
			return
		}

		// Reset the retry budget after a successful Open.
		if resetRecoveryBudget {
			failedAttempts = 0
		}

		// Pauses do not consume the retry budget.
		var ps pauseSignal
		if errors.As(runErr, &ps) {
			if !cs.cfg.Recovery.enabled() {
				err = fmt.Errorf(
					"stream: server requested pause and recovery is disabled: %w",
					runErr,
				)
				break
			}
			// The receiver already drained the pause window.
			continue
		}

		if runErr == nil {
			// Server closed the stream cleanly (EOF). Treat as a retryable
			// disconnect and recover.
			runErr = fmt.Errorf("stream: server closed the stream")
		}

		var openErr *openFailure
		isOpenFailure := errors.As(runErr, &openErr)
		initialAuthRefresh := !openedOnce &&
			failedAttempts == 0 &&
			isOpenFailure &&
			transport.IsAuthRejection(runErr) &&
			cs.cfg.Recovery.enabled() &&
			cs.cfg.RecoveryRetries > 0
		if !isOpenFailure {
			openedOnce = true
		}
		if !isOpenFailure &&
			transport.IsAuthRejection(runErr) &&
			cs.params.HeadersProvider != nil {
			cs.params.HeadersProvider.Invalidate(ctx, cs.params.TableName)
		}

		err = runErr
		if !isRetryable(runErr) && !initialAuthRefresh {
			break
		}
		failedAttempts++
	}

	// Terminal failure path.
	cs.wm.fail(err)
	cs.setTerminalErr(err)

	if cs.dispatcher != nil {
		// Retain records and queue one callback range.
		items := cs.buf.drain()
		cs.retainUnacked(items)
		if len(items) > 0 {
			cs.dispatcher.enqueueErrors(
				items[0].offset, items[len(items)-1].offset, err,
			)
		}
	} else {
		// No callback: just close the buffer so enqueue callers unblock. The
		// items remain retrievable via GetUnacked, which calls drain() itself.
		cs.buf.close()
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
	// Validation failures are deterministic.
	var ve *validationError
	if errors.As(err, &ve) {
		return false
	}
	if errors.Is(err, errClosed) {
		return false
	}
	// Honor self-classifying wrapped errors.
	var re retryableError
	if errors.As(err, &re) {
		return re.IsRetryable()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// Permanent gRPC status codes (bad request, auth, unimplemented, ...) can't
	// be fixed by reconnecting; retrying only burns the budget and resends data.
	if transport.IsTerminalStatus(err) {
		return false
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
