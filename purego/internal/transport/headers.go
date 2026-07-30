package transport

import (
	"context"
	"fmt"
	"strings"
	"unicode"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// gRPC metadata keys attached to ingestion streams. They are protocol-agnostic,
// so metadata setup is shared.
const (
	mdTableName     = "x-databricks-zerobus-table-name"
	mdAuthorization = "authorization"
)

// HeadersProvider provides gRPC metadata headers for stream authentication.
//
// Structurally identical to auth.HeadersProvider; defined here so transport (the
// lowest layer) does not import auth, which would invert the dependency. An
// *auth.StaticHeadersProvider satisfies this interface directly.
type HeadersProvider interface {
	// GetHeaders returns the metadata headers to attach to the stream. Called
	// during Open before the RPC handshake starts. Keys are case-folded to
	// lower case and single-valued; the table-name key is reserved and any value
	// returned for it is ignored in favor of the stream-open TableName.
	//
	// ctx bounds GetHeaders: it is the caller's Open context, or, when that
	// context has no deadline, one bounded by defaultHeadersTimeout. A
	// GetHeaders that blocks on network I/O (e.g. a token mint) is therefore
	// cancelled once the open budget is exhausted rather than hanging forever.
	GetHeaders(ctx context.Context, tableName string) (map[string]string, error)

	// Invalidate drops any cached credentials so the next GetHeaders re-derives
	// them. Open calls this when the server rejects the supplied credentials
	// with Unauthenticated or PermissionDenied during stream creation.
	//
	// It must not block: Open calls it synchronously on the failure path, and
	// the ctx it receives may already be cancelled.
	Invalidate(ctx context.Context, tableName string)
}

// resolveHeaders fetches the provider's headers for the open attempt and
// validates that each key and value is safe to send as gRPC metadata, so a
// malformed header fails loudly here rather than opaquely inside gRPC. It
// returns nil headers when no provider is set.
//
// ctx bounds the GetHeaders call; see HeadersProvider.GetHeaders.
func (p StreamParams) resolveHeaders(ctx context.Context) (map[string]string, error) {
	if p.HeadersProvider == nil {
		return nil, nil
	}
	headers, err := p.HeadersProvider.GetHeaders(ctx, p.TableName)
	if err != nil {
		return nil, fmt.Errorf("transport: open %q: headers provider: %w", p.TableName, err)
	}
	// Snapshot provider output so downstream processing can't race or change if
	// a provider reuses and mutates an internal map.
	snapshot := make(map[string]string, len(headers))
	for k, v := range headers {
		snapshot[k] = v
	}
	seenNormalizedKeys := make(map[string]struct{}, len(snapshot))
	for k, v := range snapshot {
		// Reject at the wire boundary before deduping; gRPC would otherwise fail
		// opaquely, and validating first means a doubled invalid key reports as
		// invalid rather than misleadingly as a duplicate.
		if !isUsableAsHeaderKey(k) {
			return nil, fmt.Errorf("transport: open %q: header key %q is not a valid gRPC metadata key", p.TableName, strings.TrimSpace(k))
		}
		if !isUsableAsHeaderValue(v) {
			return nil, fmt.Errorf("transport: open %q: header %q contains invalid value characters", p.TableName, strings.TrimSpace(k))
		}
		normalized := normalizeHeaderKey(k)
		if _, exists := seenNormalizedKeys[normalized]; exists {
			return nil, fmt.Errorf("transport: open %q: duplicate header key %q after normalization", p.TableName, normalized)
		}
		seenNormalizedKeys[normalized] = struct{}{}
	}
	return snapshot, nil
}

// withStreamMetadataHeaders returns ctx carrying the table-name header plus the
// provider headers. The table-name and authorization keys are Zerobus-owned, so
// any inherited values are replaced (gRPC is first-value-wins, so a duplicate
// could mis-route or send a stale token); unrelated caller metadata is preserved.
func withStreamMetadataHeaders(ctx context.Context, tableName string, headers map[string]string) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if ok {
		md = md.Copy()
	} else {
		md = metadata.MD{}
	}
	var authValue string
	for key, value := range headers {
		key = normalizeHeaderKey(key)
		switch key {
		case "":
			// Defensive: resolveHeaders already rejects empty keys.
			continue
		case mdTableName:
			// table header is authoritative from stream-open params.
			continue
		case mdAuthorization:
			authValue = value
		default:
			md.Set(key, strings.TrimSpace(value))
		}
	}
	md.Set(mdTableName, tableName)
	// The authorization value is sent verbatim, as the provider formatted it;
	// an empty value drops the header so a stale inherited one can't leak.
	if v := strings.TrimSpace(authValue); v != "" {
		md.Set(mdAuthorization, v)
	} else {
		md.Delete(mdAuthorization)
	}
	return metadata.NewOutgoingContext(ctx, md)
}

// isUsableAsHeaderKey reports whether key is a valid gRPC metadata key: one or
// more characters drawn from [0-9 a-z - _ .]. normalizeHeaderKey folds case, so
// an upper-case letter is accepted here. An empty key is unusable.
func isUsableAsHeaderKey(key string) bool {
	key = normalizeHeaderKey(key)
	if key == "" {
		return false
	}
	for _, r := range key {
		if !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9') && r != '-' && r != '_' && r != '.' {
			return false
		}
	}
	return true
}

// normalizeHeaderKey trims surrounding whitespace and lower-cases key, the
// canonical form used for validation, dedup, and matching Zerobus-owned keys.
func normalizeHeaderKey(key string) string {
	return strings.ToLower(strings.TrimSpace(key))
}

// isUsableAsHeaderValue reports whether value is safe as a gRPC metadata header
// value: no control or non-ASCII characters, which gRPC rejects. An empty value
// is usable (it yields no header when used for authorization).
func isUsableAsHeaderValue(value string) bool {
	for _, r := range value {
		if r > unicode.MaxASCII || unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// IsAuthRejection reports whether err rejects authentication.
func IsAuthRejection(err error) bool {
	code := status.Code(err)
	return code == codes.Unauthenticated || code == codes.PermissionDenied
}
