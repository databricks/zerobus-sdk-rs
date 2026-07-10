// Package auth provides OAuth 2.0 authentication for the Zerobus pure-Go SDK.
//
// The central abstraction is [TokenProvider], an interface for obtaining a
// bearer token on demand. Call its Token method before opening a stream and
// pass the result as [transport.StreamParams].Token.
//
// The package ships two implementations:
//   - [OAuthTokenProvider] — Unity Catalog OAuth 2.0 client credentials flow,
//     with per-table token caching and proactive refresh. This is the default
//     for production use.
//   - [StaticTokenProvider] — wraps a fixed string; useful for tests or when
//     the caller manages token lifecycle outside the SDK.
package auth

import (
	"context"
	"fmt"
	"strings"
)

// TokenProvider returns a bearer token for authenticating a stream.
//
// Implementations must be safe for concurrent use. Token may be called on
// every stream open, so implementations should cache aggressively.
//
// The returned token must be a value acceptable as an HTTP/gRPC header value
// (ASCII, no control characters). It may carry a scheme prefix ("Bearer …",
// "Basic …") or be a bare token — [transport.StreamParams].Token normalises
// either form.
//
// Invalidate is called when the server rejects the last token returned by
// Token with an authentication error, signalling that any cached credential is
// stale and must be discarded so the next Token call re-mints. Implementations
// that hold no cache may make Invalidate a no-op.
type TokenProvider interface {
	Token(ctx context.Context) (string, error)
	Invalidate(ctx context.Context)
}

// StaticTokenProvider returns the same fixed token on every call. It is
// intended for tests and for callers that manage token lifecycle externally.
//
// Invalidate is a no-op: the token is static and cannot be refreshed.
type StaticTokenProvider struct {
	token string
}

// NewStaticTokenProvider returns a [StaticTokenProvider] that always returns
// the given token. A bare token is accepted; the transport layer will prepend
// "Bearer " if needed.
func NewStaticTokenProvider(token string) *StaticTokenProvider {
	return &StaticTokenProvider{token: strings.TrimSpace(token)}
}

// Token returns the static token.
func (p *StaticTokenProvider) Token(_ context.Context) (string, error) {
	if p.token == "" {
		return "", fmt.Errorf("auth: static token is empty")
	}
	return p.token, nil
}

// Invalidate is a no-op for a static token.
func (p *StaticTokenProvider) Invalidate(_ context.Context) {}
