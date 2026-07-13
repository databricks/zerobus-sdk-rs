// Package auth provides authentication for the Zerobus pure-Go SDK.
//
// Authentication is expressed through [HeadersProvider], which supplies the gRPC
// metadata headers for a stream. [StaticHeadersProvider] returns a fixed headers
// map, for tests or externally managed credentials; a per-table token cache in
// this package supports OAuth-based providers built on top of this interface.
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
		"authorization":                   "Bearer " + token,
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
	maps.Copy(out, p.headers)
	return out, nil
}

// Invalidate is a no-op for static headers.
func (p *StaticHeadersProvider) Invalidate(_ context.Context, _ string) {}
