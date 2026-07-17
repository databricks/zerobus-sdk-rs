// Package auth provides authentication for the Zerobus pure-Go SDK.
//
// Authentication is expressed through [HeadersProvider], which supplies the gRPC
// metadata headers for a stream. Two implementations are provided:
// [OAuthHeadersProvider] mints Unity Catalog OAuth 2.0 tokens per table (backed
// by [OAuthTokenProvider]'s cache), and [StaticHeadersProvider] returns a fixed
// headers map for tests or externally managed credentials.
package auth

import (
	"context"
	"fmt"
	"maps"
	"strings"
)

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

// OAuthHeadersProvider adapts an [OAuthTokenProvider] to the [HeadersProvider]
// seam: it mints per-table Unity Catalog OAuth tokens on demand and formats them
// as gRPC auth metadata. The token cache and OAuth options are configured at
// construction via [NewOAuthHeadersProvider].
type OAuthHeadersProvider struct {
	tokenProvider *OAuthTokenProvider
}

// NewOAuthHeadersProvider creates an OAuth-backed headers provider. The
// arguments and options are those of [NewOAuthTokenProvider].
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

// GetHeaders mints (or serves a cached) token for tableName and returns it as a
// "Bearer" authorization header. It may block on the token mint, bounded by ctx.
// The table-name header is included for cross-SDK parity; transport open treats
// its own stream-open TableName as authoritative.
func (p *OAuthHeadersProvider) GetHeaders(ctx context.Context, tableName string) (map[string]string, error) {
	token, err := p.tokenProvider.Token(ctx, tableName)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"authorization":                   "Bearer " + token,
		"x-databricks-zerobus-table-name": tableName,
	}, nil
}

// Invalidate drops the cached token for tableName so the next GetHeaders
// re-mints. Transport open calls this on a server auth rejection.
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
	maps.Copy(out, p.headers)
	return out, nil
}

// Invalidate is a no-op for static headers.
func (p *StaticHeadersProvider) Invalidate(_ context.Context, _ string) {}
