package transport

import (
	"context"
	"strings"

	"google.golang.org/grpc/metadata"
)

// gRPC metadata keys attached to ingestion streams. They are protocol-agnostic,
// so metadata setup is shared.
const (
	mdTableName     = "x-databricks-zerobus-table-name"
	mdAuthorization = "authorization"
)

// streamContext builds the outgoing context an ingestion stream needs: table
// name and authorization headers, wrapped in a cancelable child whose cancel
// func tears the stream down.
func streamContext(ctx context.Context, tableName, token string) (context.Context, context.CancelFunc) {
	ctx = metadata.AppendToOutgoingContext(ctx, mdTableName, tableName)
	ctx = withAuth(ctx, token)
	return context.WithCancel(ctx)
}

// withAuth returns ctx with the authorization header set from token, or ctx
// unchanged when token is empty.
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
