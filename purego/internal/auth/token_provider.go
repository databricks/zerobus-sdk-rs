// Package auth provides OAuth 2.0 and custom-header authentication for the
// Zerobus pure-Go SDK.
//
// It exposes two layers:
//   - [TokenProvider], for obtaining bearer tokens.
//   - [HeadersProvider], matching other SDKs' custom-header auth model.
//
// The package ships two token providers:
//   - [OAuthTokenProvider] — Unity Catalog OAuth 2.0 client credentials flow
//     with per-table token caching and proactive refresh.
//   - [StaticTokenProvider] — wraps a fixed string for tests or externally
//     managed token lifecycles.
//
// And two headers providers:
//   - [OAuthHeadersProvider] — wraps an [OAuthTokenProvider] and emits the
//     required Zerobus metadata headers.
//   - [StaticHeadersProvider] — returns a fixed headers map.
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
// tableName is the fully qualified target table (catalog.schema.table) used
// for downscoped-token minting.
//
// Invalidate is called when the server rejects the last token returned by Token
// with an authentication error, signalling that any cached credential for that
// table is stale and must be discarded so the next Token call re-mints.
// Implementations that hold no cache may make Invalidate a no-op.
type TokenProvider interface {
	Token(ctx context.Context, tableName string) (string, error)
	Invalidate(ctx context.Context, tableName string)
}

// HeadersProvider provides gRPC metadata headers for stream authentication.
//
// GetHeaders returns headers for the given tableName. The transport layer will
// always enforce the authoritative table-name header from stream-open params.
// Implementations may include it for parity with other SDKs.
//
// Invalidate is called on server auth rejection so any cached credentials can
// be dropped before the next open attempt.
type HeadersProvider interface {
	GetHeaders(ctx context.Context, tableName string) (map[string]string, error)
	Invalidate(ctx context.Context, tableName string)
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
func (p *StaticTokenProvider) Token(_ context.Context, _ string) (string, error) {
	if p.token == "" {
		return "", fmt.Errorf("auth: static token is empty")
	}
	return p.token, nil
}

// Invalidate is a no-op for a static token.
func (p *StaticTokenProvider) Invalidate(_ context.Context, _ string) {}

// OAuthHeadersProvider bridges OAuth token minting into headers-provider shape.
type OAuthHeadersProvider struct {
	tokenProvider *OAuthTokenProvider
}

// NewOAuthHeadersProvider creates an OAuth-backed headers provider.
func NewOAuthHeadersProvider(
	clientID, clientSecret, zerobusEndpoint, ucEndpoint string,
	opts ...OAuthOption,
) (*OAuthHeadersProvider, error) {
	p, err := NewOAuthTokenProvider(clientID, clientSecret, zerobusEndpoint, ucEndpoint, opts...)
	if err != nil {
		return nil, err
	}
	return &OAuthHeadersProvider{tokenProvider: p}, nil
}

// GetHeaders returns Zerobus auth headers for tableName.
func (p *OAuthHeadersProvider) GetHeaders(ctx context.Context, tableName string) (map[string]string, error) {
	token, err := p.tokenProvider.Token(ctx, tableName)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"authorization":                    "Bearer " + token,
		"x-databricks-zerobus-table-name": tableName,
	}, nil
}

// Invalidate drops the cached token for tableName.
func (p *OAuthHeadersProvider) Invalidate(ctx context.Context, tableName string) {
	p.tokenProvider.Invalidate(ctx, tableName)
}

// StaticHeadersProvider returns a fixed header set.
type StaticHeadersProvider struct {
	headers map[string]string
}

// NewStaticHeadersProvider returns a provider that returns the same headers on
// every call.
func NewStaticHeadersProvider(headers map[string]string) *StaticHeadersProvider {
	cloned := make(map[string]string, len(headers))
	for k, v := range headers {
		cloned[k] = strings.TrimSpace(v)
	}
	return &StaticHeadersProvider{headers: cloned}
}

// GetHeaders returns a copy of the configured headers.
func (p *StaticHeadersProvider) GetHeaders(_ context.Context, _ string) (map[string]string, error) {
	if len(p.headers) == 0 {
		return nil, fmt.Errorf("auth: static headers are empty")
	}
	out := make(map[string]string, len(p.headers))
	for k, v := range p.headers {
		out[k] = v
	}
	return out, nil
}

// Invalidate is a no-op for static headers.
func (p *StaticHeadersProvider) Invalidate(_ context.Context, _ string) {}
