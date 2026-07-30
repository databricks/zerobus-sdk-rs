// Package zerobus is a native, pure-Go client for streaming ingestion into
// Databricks Delta tables through the Zerobus service. It talks gRPC to the
// service directly — no cgo, no FFI, no C toolchain — so it builds and
// cross-compiles as an ordinary Go module.
//
// # Quick start
//
//	sdk, err := zerobus.New(
//	    "https://your-workspace.zerobus.region.cloud.databricks.com",
//	    "https://your-workspace.cloud.databricks.com",
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer sdk.Close()
//
//	stream, err := sdk.CreateStream(ctx, "catalog.schema.table",
//	    clientID, clientSecret, zerobus.WithJSON())
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer stream.Close()
//
// # Ingesting data
//
// Ingestion is asynchronous and pipelined: IngestRecordOffset returns as soon as
// the record is queued and the offset is assigned; sending and acknowledgement
// happen in the background. Queue records in a loop and confirm durability with
// a single Flush — never wait for an acknowledgement after every record, which
// collapses throughput to one record per round-trip.
//
//	for _, rec := range records {
//	    if _, err := stream.IngestRecordOffset(rec); err != nil {
//	        log.Fatal(err)
//	    }
//	}
//	if err := stream.Flush(); err != nil { // wait once for all pending acks
//	    log.Fatal(err)
//	}
//
// For continuous streams call Flush periodically (every N records) or register
// an ack callback with WithAckCallback. Reserve per-record WaitForOffset for
// genuinely low-volume cases where each record must be confirmed before
// continuing.
//
// # Authentication
//
// CreateStream uses the Unity Catalog OAuth 2.0 client-credentials flow. For
// custom authentication, implement HeadersProvider and use
// CreateStreamWithProvider.
package zerobus

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"google.golang.org/grpc"

	"github.com/databricks/zerobus-sdk/purego/internal/auth"
	"github.com/databricks/zerobus-sdk/purego/internal/stream"
	"github.com/databricks/zerobus-sdk/purego/internal/transport"
)

// version identifies this SDK in the gRPC user-agent header.
const version = "0.1.0"

// userAgentPrefix is the leading user-agent token sent on every request. An
// application name supplied via WithApplicationName is appended after a space.
const userAgentPrefix = "zerobus-sdk-go-purego/" + version

// HeadersProvider supplies the gRPC authentication metadata for a stream.
// Implement it to plug in custom authentication with CreateStreamWithProvider;
// the built-in OAuth path (CreateStream) provides one for you.
//
// GetHeaders returns the metadata headers for tableName; it may block on a token
// mint, bounded by ctx. Invalidate is called on a server auth rejection so
// cached credentials can be dropped before the next attempt — it must not block
// on network I/O.
type HeadersProvider = transport.HeadersProvider

// NewStaticHeadersProvider returns a HeadersProvider that returns the same fixed
// headers on every call. It is intended for tests or externally managed
// credentials; pair it with CreateStreamWithProvider.
func NewStaticHeadersProvider(headers map[string]string) HeadersProvider {
	return auth.NewStaticHeadersProvider(headers)
}

// SDK is the entry point for creating ingestion streams. It owns the shared gRPC
// connection to the Zerobus service and, for the OAuth path, a per-table token
// cache reused across every stream it creates. A single SDK is safe for
// concurrent use; callers must Close it when done.
type SDK struct {
	zerobusEndpoint string
	ucEndpoint      string
	conn            *transport.Conn
}

// New connects to the Zerobus service and returns an SDK.
//
// zerobusEndpoint is the Zerobus gRPC endpoint (e.g.
// "https://ws.zerobus.region.cloud.databricks.com"). ucEndpoint is the Unity
// Catalog workspace URL used to mint OAuth tokens (e.g.
// "https://ws.cloud.databricks.com").
//
// The connection is secured with TLS using the host's root CAs unless
// WithTLSConfig overrides it. Dialing is lazy: the TCP/TLS handshake happens
// when the first stream opens, so New does not fail on an unreachable service.
func New(zerobusEndpoint, ucEndpoint string, opts ...Option) (*SDK, error) {
	var cfg sdkConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	target, err := grpcTarget(zerobusEndpoint)
	if err != nil {
		return nil, &Error{Op: "New", cause: err, retryable: false}
	}

	ua := userAgentPrefix
	if name := strings.TrimSpace(cfg.applicationName); name != "" {
		ua = ua + " " + name
	}

	dialOpts := []transport.DialOption{
		transport.WithGRPCDialOptions(grpc.WithUserAgent(ua)),
	}
	if cfg.tlsConfig != nil {
		dialOpts = append(dialOpts, transport.WithTLSConfig(cfg.tlsConfig))
	}

	conn, err := transport.Dial(target, dialOpts...)
	if err != nil {
		return nil, &Error{Op: "New", cause: err, retryable: false}
	}
	return &SDK{
		zerobusEndpoint: strings.TrimSpace(zerobusEndpoint),
		ucEndpoint:      strings.TrimSpace(ucEndpoint),
		conn:            conn,
	}, nil
}

// Close releases the SDK's connection. Streams created from it are terminated;
// close individual streams first for orderly shutdown and durability results.
func (s *SDK) Close() error {
	return wrapErr("Close", s.conn.Close())
}

// CreateStream opens an ingestion stream for tableName authenticated with the
// Unity Catalog OAuth 2.0 client-credentials flow using clientID and
// clientSecret. The record encoding defaults to JSON; pass WithProto to ingest
// Protocol Buffer records, along with any tuning options.
//
// The stream opens asynchronously: CreateStream validates its arguments and
// returns a ready stream immediately, while the first connection (token mint
// plus handshake) proceeds in the background and recovers on failure. A
// connection error that cannot be recovered — bad credentials, an unknown
// table — therefore surfaces on the first Flush, WaitForOffset, or ack
// callback, not from CreateStream. The caller must Close the returned stream.
func (s *SDK) CreateStream(
	ctx context.Context,
	tableName, clientID, clientSecret string,
	opts ...StreamOption,
) (*Stream, error) {
	provider, err := auth.NewOAuthHeadersProvider(
		clientID, clientSecret, s.zerobusEndpoint, s.ucEndpoint,
	)
	if err != nil {
		return nil, &Error{Op: "CreateStream", cause: err, retryable: false}
	}
	return s.createStream(ctx, "CreateStream", tableName, provider, opts...)
}

// CreateStreamWithProvider opens an ingestion stream for tableName using a custom
// HeadersProvider for authentication. Use it when the OAuth client-credentials
// flow of CreateStream does not fit — for externally managed credentials, a
// custom token source, or tests. Options behave as in CreateStream.
func (s *SDK) CreateStreamWithProvider(
	ctx context.Context,
	tableName string,
	provider HeadersProvider,
	opts ...StreamOption,
) (*Stream, error) {
	if provider == nil {
		return nil, &Error{
			Op:        "CreateStreamWithProvider",
			cause:     fmt.Errorf("headers provider is required"),
			retryable: false,
		}
	}
	return s.createStream(ctx, "CreateStreamWithProvider", tableName, provider, opts...)
}

func (s *SDK) createStream(
	_ context.Context,
	op, tableName string,
	provider HeadersProvider,
	opts ...StreamOption,
) (*Stream, error) {
	sc := defaultStreamConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&sc)
		}
	}

	params := stream.StreamParams{
		TableName:       tableName,
		RecordType:      sc.recordType,
		DescriptorProto: sc.descriptor,
		HeadersProvider: provider,
	}
	// NewProtoJSONStream validates the record type / descriptor and starts the
	// supervisor, which opens the first transport stream in the background. Only
	// construction-time validation errors surface here; connection failures
	// surface on the first Flush/WaitForOffset (see CreateStream). The context is
	// accepted for a uniform, forward-compatible signature.
	core, err := stream.NewProtoJSONStream(s.conn, params, sc.cfg, sc.callback)
	if err != nil {
		return nil, wrapErr(op, err)
	}
	return &Stream{core: core}, nil
}

// grpcTarget converts a user-facing endpoint (an https URL, a bare host, or a
// gRPC resolver target) into a target grpc.NewClient accepts. An https/http URL
// is reduced to host[:port], defaulting to :443. Explicit resolver targets
// (containing "://", e.g. "dns:///", "passthrough:///") pass through unchanged
// so tests and advanced callers can inject their own.
func grpcTarget(endpoint string) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", fmt.Errorf("zerobusEndpoint is required")
	}
	lower := strings.ToLower(endpoint)
	if !strings.HasPrefix(lower, "https://") && !strings.HasPrefix(lower, "http://") {
		// A gRPC resolver target (scheme://... other than http/https) is passed
		// through; a bare host gets the default TLS port.
		if strings.Contains(endpoint, "://") {
			return endpoint, nil
		}
		if _, port, err := splitHostPort(endpoint); err == nil && port != "" {
			return endpoint, nil
		}
		return endpoint + ":443", nil
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("zerobusEndpoint is not a valid URL: %w", err)
	}
	host := u.Hostname()
	if host == "" {
		return "", fmt.Errorf("zerobusEndpoint has no host: %q", endpoint)
	}
	port := u.Port()
	if port == "" {
		port = "443"
	}
	return host + ":" + port, nil
}

// splitHostPort reports the host and port of a "host:port" string, returning an
// empty port when none is present. It tolerates a bare host without erroring so
// grpcTarget can decide whether to append the default port.
func splitHostPort(s string) (host, port string, err error) {
	i := strings.LastIndexByte(s, ':')
	if i < 0 {
		return s, "", nil
	}
	// A ':' inside brackets is part of an IPv6 literal, not a port separator.
	if strings.HasSuffix(s[:i], "]") || !strings.Contains(s, "]") {
		return s[:i], s[i+1:], nil
	}
	return s, "", nil
}
