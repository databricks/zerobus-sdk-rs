package stream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/transport"
)

// supervise runs the connect, stream, and recovery lifecycle. firstOpenCtx
// bounds every attempt and backoff before the first successful open;
// lifecycleCtx owns the live stream and every later reconnect.
// Durable progress or a connection that survives RecoveryResetAfter resets the
// retry budget.
func (cs *CoreStream[Req, Resp]) supervise(lifecycleCtx, firstOpenCtx context.Context) {
	defer func() {
		if lifecycleCtx.Err() != nil {
			cs.signalReady(errClosedBeforeReady)
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
	initialAuthRefreshUsed := false
superviseLoop:
	for {
		if lifecycleCtx.Err() != nil {
			// Cancelled by Close — clean exit, no error.
			return
		}
		if !openedOnce && firstOpenCtx.Err() != nil {
			err = firstOpenCtx.Err()
			break
		}

		if failedAttempts > 0 {
			if !cs.cfg.Recovery.enabled() || failedAttempts > cs.cfg.RecoveryRetries {
				err = fmt.Errorf("stream: recovery exhausted after %d attempt(s): %w",
					failedAttempts, err)
				break
			}
			backoffCtx := lifecycleCtx
			if !openedOnce {
				backoffCtx = firstOpenCtx
			}
			timer := time.NewTimer(cs.cfg.RecoveryBackoff)
			select {
			case <-timer.C:
			case <-lifecycleCtx.Done():
				timer.Stop()
				return
			case <-backoffCtx.Done():
				timer.Stop()
				if lifecycleCtx.Err() != nil {
					return
				}
				err = backoffCtx.Err()
				break superviseLoop
			}
			// runOnce requeues after Open succeeds.
		}

		openingCtx := lifecycleCtx
		if !openedOnce {
			openingCtx = firstOpenCtx
		}
		runErr, resetRecoveryBudget := cs.runOnce(lifecycleCtx, openingCtx)

		// Lifecycle cancellation is a clean exit via Close. Don't treat
		// server-side EOF as clean.
		if lifecycleCtx.Err() != nil {
			// A definitive rejection still invalidates the credential that
			// caused it, even when Close raced the completed Open/Recv. The
			// provider contract requires Invalidate to be non-blocking.
			if transport.IsAuthRejection(runErr) &&
				cs.params.HeadersProvider != nil {
				cs.params.HeadersProvider.Invalidate(
					context.WithoutCancel(lifecycleCtx), cs.params.TableName,
				)
			}
			return
		}

		// Reset after durable progress or sufficient connection uptime.
		if resetRecoveryBudget {
			failedAttempts = 0
		}

		// Pauses do not consume the retry budget.
		var ps pauseSignal
		if errors.As(runErr, &ps) {
			// A pause can only come from an established stream. Authentication
			// failures on the following Open are therefore reconnect failures,
			// not candidates for the one initial credential refresh.
			openedOnce = true
			if !cs.cfg.Recovery.enabled() {
				err = fmt.Errorf(
					"stream: server requested pause and recovery is disabled: %w",
					runErr,
				)
				break
			}
			if !cs.pauseWait(lifecycleCtx, ps.resumeAt) {
				return
			}
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
			!initialAuthRefreshUsed &&
			isOpenFailure &&
			transport.IsAuthRejection(runErr) &&
			cs.params.HeadersProvider != nil &&
			cs.cfg.Recovery.enabled() &&
			failedAttempts < cs.cfg.RecoveryRetries
		if initialAuthRefresh {
			initialAuthRefreshUsed = true
		}
		if !isOpenFailure {
			openedOnce = true
		}
		if transport.IsAuthRejection(runErr) &&
			cs.params.HeadersProvider != nil {
			cs.params.HeadersProvider.Invalidate(lifecycleCtx, cs.params.TableName)
		}

		err = runErr
		if !isRetryable(runErr) && !initialAuthRefresh {
			break
		}
		failedAttempts++
	}

	// Terminal failure path.
	cs.signalReady(err)
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

// IsRetryable reports whether err represents a transient failure that a retry
// or a fresh stream could recover from, using the same classification the
// supervisor applies to reconnect decisions. It is exported for the public
// zerobus package to derive the retryability of the errors it surfaces.
func IsRetryable(err error) bool { return isRetryable(err) }

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

// deniesRetry reports whether err already classifies itself as permanently
// failed for a reason other than a deadline. A provider failure can be permanent
// without carrying a terminal gRPC status — an OAuth TokenError for HTTP 401 is
// codes.Unknown — and isRetryable consults the outermost classification, so
// wrapping such an error as a retryable timeout would turn a rejected credential
// into repeated token requests. An error that reports itself non-retryable only
// because a deadline cut it short does not qualify: that is the timeout
// openBudgetExceeded exists to retry.
func deniesRetry(err error) bool {
	var re retryableError
	if !errors.As(err, &re) || re.IsRetryable() {
		return false
	}
	return !errors.Is(err, context.DeadlineExceeded)
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
