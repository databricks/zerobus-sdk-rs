// Package authctx holds context-cancellation causes shared between the auth and
// transport layers. It is a dependency-free leaf so both can reference the same
// sentinel without importing each other.
package authctx

import "errors"

// ErrHeadersBudgetExceeded tags the transport's internal header-resolution
// budget as the cause of a context.WithTimeoutCause. A HeadersProvider matches
// it via context.Cause to tell the SDK's own budget firing (transient, so a
// cached token may be served) apart from a caller-owned cancel (must be honored).
var ErrHeadersBudgetExceeded = errors.New("transport: open header budget exceeded")
