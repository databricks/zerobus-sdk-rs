package zerobus

import (
	"errors"

	"github.com/databricks/zerobus-sdk/purego/internal/stream"
)

// Error is the error type returned by SDK and Stream operations. It wraps the
// underlying cause and reports whether reconnecting or retrying the operation
// could succeed.
//
// Not every error returned by the SDK is an *Error; wrapped standard-library
// and context errors also flow through. Use errors.As to test for it, or the
// package-level Retryable helper, which handles both cases:
//
//	if _, err := stream.IngestRecordOffset(rec); err != nil {
//	    if zerobus.Retryable(err) {
//	        // transient — a retry or fresh stream may succeed
//	    } else {
//	        // permanent — fix configuration or input before retrying
//	    }
//	}
type Error struct {
	// Op is the SDK operation that failed (e.g. "CreateStream", "Flush").
	Op string
	// cause is the underlying error.
	cause error
	// retryable records whether the failure is transient.
	retryable bool
}

func (e *Error) Error() string {
	if e.Op == "" {
		return e.cause.Error()
	}
	return "zerobus: " + e.Op + ": " + e.cause.Error()
}

// Unwrap exposes the underlying cause for errors.Is / errors.As.
func (e *Error) Unwrap() error { return e.cause }

// Retryable reports whether the failure is transient and a retry (or a fresh
// stream) may succeed. A non-retryable error indicates a permanent condition —
// invalid configuration, bad input, or a rejected credential — that a retry
// cannot fix.
func (e *Error) Retryable() bool { return e.retryable }

// wrapErr wraps a core/transport error as an *Error tagged with the operation
// name, deriving retryability from the core's classification. A nil error
// wraps to nil so call sites can wrap unconditionally.
func wrapErr(op string, err error) error {
	if err == nil {
		return nil
	}
	// Preserve an already-classified *Error rather than double-wrapping; just
	// attach the operation if it lacks one.
	var e *Error
	if errors.As(err, &e) {
		if e.Op == "" {
			e.Op = op
		}
		return err
	}
	return &Error{Op: op, cause: err, retryable: stream.IsRetryable(err)}
}

// Retryable reports whether err (or any error it wraps) is transient. It is the
// package-level counterpart to (*Error).Retryable and also classifies raw
// core/transport errors that were not wrapped as an *Error.
func Retryable(err error) bool {
	if err == nil {
		return false
	}
	var e *Error
	if errors.As(err, &e) {
		return e.retryable
	}
	return stream.IsRetryable(err)
}
