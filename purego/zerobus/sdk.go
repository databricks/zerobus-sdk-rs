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
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"unicode/utf8"

	"google.golang.org/grpc"

	"github.com/databricks/zerobus-sdk/purego/internal/auth"
	"github.com/databricks/zerobus-sdk/purego/internal/stream"
	"github.com/databricks/zerobus-sdk/purego/internal/transport"
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
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
	// tokenCache backs every OAuth provider this SDK creates so a token minted
	// for a table is reused by later streams on the same table instead of
	// re-minting per stream.
	tokenCache *auth.SharedTokenCache

	// mu guards the open-stream set and the closed flag.
	mu     sync.Mutex
	closed bool
	// streams holds the streams this SDK created and has not yet torn down, so
	// Close can terminate them. Entries are removed by Stream.Close.
	streams map[*Stream]struct{}
}

// newSDK builds an SDK around an already-dialed connection. It is the single
// construction point shared by New and the test-only NewWithConn so both get
// the shared token cache and stream registry.
func newSDK(conn *transport.Conn, zerobusEndpoint, ucEndpoint string) *SDK {
	return &SDK{
		zerobusEndpoint: strings.TrimSpace(zerobusEndpoint),
		ucEndpoint:      strings.TrimSpace(ucEndpoint),
		conn:            conn,
		tokenCache:      auth.NewSharedTokenCache(),
		streams:         make(map[*Stream]struct{}),
	}
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

	name := strings.TrimSpace(cfg.applicationName)
	if err := validateApplicationName(name); err != nil {
		return nil, &Error{Op: "New", cause: err, retryable: false}
	}
	ua := userAgentPrefix
	if name != "" {
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
	return newSDK(conn, zerobusEndpoint, ucEndpoint), nil
}

// Close terminates every stream still open from this SDK, then releases the
// shared connection. It is idempotent, and further CreateStream calls fail.
//
// Termination is abrupt: records queued but not yet acknowledged are abandoned
// rather than flushed, and are reported through each stream's ack callback (or
// retrievable with GetUnackedRecords). Close individual streams first for an
// orderly shutdown with durability results.
func (s *SDK) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	open := make([]*Stream, 0, len(s.streams))
	for st := range s.streams {
		open = append(open, st)
	}
	s.streams = nil
	s.mu.Unlock()

	// Each teardown waits on its own supervisor goroutine, so terminate
	// concurrently rather than summing every stream's teardown on the shutdown
	// path.
	errs := make([]error, len(open)+1)
	var wg sync.WaitGroup
	for i, st := range open {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = st.terminate()
		}()
	}
	wg.Wait()
	// Close the connection only after the streams are down, so a supervisor
	// cannot reconnect onto a connection that is going away.
	errs[len(open)] = s.conn.Close()
	return wrapErr("Close", errors.Join(errs...))
}

// CreateStream opens an ingestion stream for tableName authenticated with the
// Unity Catalog OAuth 2.0 client-credentials flow using clientID and
// clientSecret. The record encoding defaults to Protocol Buffers; pass
// WithProto with the required descriptor, or WithJSON for JSON records.
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
		auth.WithSharedTokenCache(s.tokenCache),
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
	if err := validateStreamArgs(tableName, sc); err != nil {
		return nil, &Error{Op: op, cause: err, retryable: false}
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
	st := &Stream{core: core, sdk: s}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		// Terminate rather than leak the supervisor goroutine NewProtoJSONStream
		// already started.
		_ = core.Terminate()
		return nil, &Error{Op: op, cause: fmt.Errorf("SDK is closed"), retryable: false}
	}
	s.streams[st] = struct{}{}
	s.mu.Unlock()
	return st, nil
}

// validateStreamArgs rejects stream arguments that transport open would reject
// asynchronously, so a caller mistake fails at CreateStream instead of surfacing
// much later on the first Flush. It deliberately mirrors the transport's checks
// rather than adding stricter rules of its own.
func validateStreamArgs(tableName string, sc streamConfig) error {
	if strings.TrimSpace(tableName) == "" {
		return fmt.Errorf("table name is required")
	}
	if sc.recordType == zerobuspb.RecordType_PROTO && len(sc.descriptor) == 0 {
		return fmt.Errorf("WithProto requires a non-empty descriptor proto")
	}
	return nil
}

// validateApplicationName applies gRPC metadata's ASCII-value constraints
// before constructing the lazy connection, so malformed user-agent data fails
// at New rather than later during the first stream open.
func validateApplicationName(name string) error {
	if !utf8.ValidString(name) {
		return fmt.Errorf("application name must be valid UTF-8")
	}
	for i := 0; i < len(name); i++ {
		b := name[i]
		if b != '\t' && (b < 0x20 || b == 0x7f || b > 0x7e) {
			return fmt.Errorf("application name contains invalid user-agent characters")
		}
	}
	return nil
}

// forget drops st from the open-stream set once it has torn itself down, so a
// long-lived SDK does not retain closed streams.
func (s *SDK) forget(st *Stream) {
	s.mu.Lock()
	delete(s.streams, st)
	s.mu.Unlock()
}

// grpcTarget converts a user-facing endpoint (an HTTPS URL, a bare host, or a
// gRPC resolver target) into a target grpc.NewClient accepts. An HTTPS URL is
// reduced to host[:port], defaulting to :443. Explicit resolver targets
// (containing "://", e.g. "dns:///", "passthrough:///") pass through unchanged
// so tests and advanced callers can inject their own.
func grpcTarget(endpoint string) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", fmt.Errorf("zerobusEndpoint is required")
	}
	lower := strings.ToLower(endpoint)
	if strings.HasPrefix(lower, "http://") {
		return "", fmt.Errorf("zerobusEndpoint must use HTTPS")
	}
	if !strings.HasPrefix(lower, "https://") {
		// A gRPC resolver target (scheme://... other than HTTPS) is passed
		// through; a bare host gets the default TLS port.
		if strings.Contains(endpoint, "://") {
			return endpoint, nil
		}
		if ip := net.ParseIP(strings.Trim(endpoint, "[]")); ip != nil {
			return net.JoinHostPort(ip.String(), "443"), nil
		}
		if host, port, err := net.SplitHostPort(endpoint); err == nil {
			return net.JoinHostPort(host, port), nil
		}
		return net.JoinHostPort(endpoint, "443"), nil
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
	return net.JoinHostPort(host, port), nil
}
