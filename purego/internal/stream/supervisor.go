package stream

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// supervise manages connection and recovery.
func (cs *CoreStream[Req, Resp]) supervise(ctx context.Context) {
	defer close(cs.done)

	var err error
	failedAttempts := 0
	for {
		if ctx.Err() != nil {
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
			cs.buf.requeue()
		}

		runErr, healthy := cs.runOnce(ctx)

		if ctx.Err() != nil {
			return
		}

		if healthy {
			failedAttempts = 0
		}

		var ps pauseSignal
		if errors.As(runErr, &ps) {
			if !cs.waitPause(ctx, ps.duration) {
				return // Close cancelled ctx during the pause.
			}
			cs.buf.requeue()
			continue
		}

		if runErr == nil {
			// EOF is a retryable disconnect.
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
		// Report every abandoned item.
		for _, it := range cs.buf.drain() {
			cs.callback.OnError(it.offset, err)
		}
	} else {
		// Preserve items for GetUnacked.
		cs.buf.close()
	}
}

// waitPause applies the server pause and optional client cap.
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
	if errors.Is(err, errClosed) {
		return false
	}
	var ve *validationError
	if errors.As(err, &ve) {
		return false
	}
	var re retryableError
	if errors.As(err, &re) {
		return re.IsRetryable()
	}
	return true
}

// validationError marks deterministic failures.
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
