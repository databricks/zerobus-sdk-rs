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

// authSchemes are matched case-insensitively against an authorization value's
// first word; a match is sent verbatim, anything else is prefixed with "Bearer ".
var authSchemes = []string{"bearer", "basic", "dpop"}

// HeadersProvider provides gRPC metadata headers for stream authentication.
type HeadersProvider interface {
	// GetHeaders returns the metadata headers to attach to the stream. Called
	// during Open before the RPC handshake starts. Keys are case-folded to
	// lower case and single-valued; the table-name key is reserved and any value
	// returned for it is ignored in favor of the stream-open TableName.
	//
	// ctx bounds GetHeaders: it is the caller's Open context, or, when that
	// context has no deadline, one bounded by defaultHandshakeTimeout. A
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
		normalized := normalizeHeaderKey(k)
		if normalized != "" {
			if _, exists := seenNormalizedKeys[normalized]; exists {
				return nil, fmt.Errorf("transport: open %q: duplicate header key %q after normalization", p.TableName, normalized)
			}
			seenNormalizedKeys[normalized] = struct{}{}
		}
		// Reject at the wire boundary; gRPC would otherwise fail opaquely.
		if !isUsableAsHeaderKey(k) {
			return nil, fmt.Errorf("transport: open %q: header key %q is not a valid gRPC metadata key", p.TableName, strings.TrimSpace(k))
		}
		if !isUsableAsHeaderValue(v) {
			return nil, fmt.Errorf("transport: open %q: header %q contains invalid value characters", p.TableName, strings.TrimSpace(k))
		}
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
	if v := authHeaderValue(authValue); v != "" {
		md.Set(mdAuthorization, v)
	} else {
		md.Delete(mdAuthorization)
	}
	return metadata.NewOutgoingContext(ctx, md)
}

// isUsableAsHeaderKey reports whether key is a valid gRPC metadata key: one or
// more characters drawn from [0-9 a-z - _ .]. Keys are lower-cased before use,
// so an upper-case letter is accepted here and folded by withStreamMetadataHeaders.
// An empty key is unusable (it carries no header).
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

// authHeaderValue normalizes a value into an authorization header value: a value
// already carrying a known scheme (Bearer/Basic/DPoP) is returned verbatim, a
// bare value is prefixed with "Bearer ", and an empty value yields "".
func authHeaderValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if scheme, _, found := strings.Cut(value, " "); found {
		for _, s := range authSchemes {
			if strings.EqualFold(scheme, s) {
				return value
			}
		}
	}
	return "Bearer " + value
}

// isAuthRejection reports whether err is a gRPC auth rejection
// (Unauthenticated or PermissionDenied), unwrapping wrapped errors.
func isAuthRejection(err error) bool {
	code := status.Code(err)
	return code == codes.Unauthenticated || code == codes.PermissionDenied
}
