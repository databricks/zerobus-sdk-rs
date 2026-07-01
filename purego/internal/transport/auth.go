package transport

import (
	"context"
	"strings"

	"google.golang.org/grpc/metadata"
)

// mdAuthorization is the gRPC metadata key carrying the bearer token. It is
// shared by every stream protocol: the proto/JSON and Arrow paths both
// authenticate this way.
const mdAuthorization = "authorization"

// withAuth returns ctx with the authorization header set from token, or ctx
// unchanged when token is empty. Shared across stream protocols so they present
// credentials identically.
func withAuth(ctx context.Context, token string) context.Context {
	if v := authHeaderValue(token); v != "" {
		return metadata.AppendToOutgoingContext(ctx, mdAuthorization, v)
	}
	return ctx
}

// authHeaderValue normalizes a token into an authorization header value: a bare
// token is prefixed with "Bearer "; a value that already carries a scheme (for
// example "Bearer ..." or "Basic ...") is returned verbatim. An empty token
// yields "".
func authHeaderValue(token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	// If the caller already supplied a scheme (for example "Bearer ..."), keep it.
	if strings.Contains(token, " ") {
		return token
	}
	return "Bearer " + token
}
