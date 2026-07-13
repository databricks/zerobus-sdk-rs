package transport

import (
	"context"
	"strings"
	"unicode"

	"google.golang.org/grpc/metadata"
)

// gRPC metadata keys attached to ingestion streams. They are protocol-agnostic,
// so metadata setup is shared.
const (
	mdTableName     = "x-databricks-zerobus-table-name"
	mdAuthorization = "authorization"
)

// authSchemes are matched case-insensitively against a token's first word; a
// match is sent verbatim, anything else is prefixed with "Bearer ".
var authSchemes = []string{"bearer", "basic", "dpop"}

// withStreamMetadata returns ctx carrying exactly the table-name and
// authorization headers. These are Zerobus-owned keys, so any inherited values
// are replaced (gRPC is first-value-wins, so a duplicate could mis-route or send
// a stale token); unrelated caller metadata is preserved.
func withStreamMetadata(ctx context.Context, tableName, token string) context.Context {
	return withStreamMetadataHeaders(ctx, tableName, nil, token)
}

// withStreamMetadataHeaders applies stream metadata using either headers from a
// provider or a direct token string.
func withStreamMetadataHeaders(ctx context.Context, tableName string, headers map[string]string, token string) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if ok {
		md = md.Copy()
	} else {
		md = metadata.MD{}
	}
	var authValue string
	for key, value := range headers {
		key = strings.ToLower(strings.TrimSpace(key))
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
	if authValue == "" {
		authValue = token
	}
	if v := authHeaderValue(authValue); v != "" {
		md.Set(mdAuthorization, v)
	} else {
		md.Delete(mdAuthorization)
	}
	return metadata.NewOutgoingContext(ctx, md)
}

// isUsableAsHeader reports whether value is safe as a gRPC metadata header
// value: no control or non-ASCII characters, which gRPC rejects. An empty
// value is usable (it yields no header when used for authorization).
func isUsableAsHeader(token string) bool {
	for _, r := range token {
		if r > unicode.MaxASCII || unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// authHeaderValue normalizes a token into an authorization header value: a value
// already carrying a known scheme (Bearer/Basic/DPoP) is returned verbatim, a
// bare token is prefixed with "Bearer ", and an empty token yields "".
func authHeaderValue(token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	if scheme, _, found := strings.Cut(token, " "); found {
		for _, s := range authSchemes {
			if strings.EqualFold(scheme, s) {
				return token
			}
		}
	}
	return "Bearer " + token
}
