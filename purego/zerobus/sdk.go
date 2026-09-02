// Package zerobus is a pure-Go client for Zerobus ingestion. Create an SDK with
// New and a stream with CreateStream.
package zerobus

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/databricks/zerobus-sdk/purego/internal/auth"
	"github.com/databricks/zerobus-sdk/purego/internal/dynamicproto"
	"github.com/databricks/zerobus-sdk/purego/internal/schema"
	"github.com/databricks/zerobus-sdk/purego/internal/stream"
	"github.com/databricks/zerobus-sdk/purego/internal/transport"
	"github.com/databricks/zerobus-sdk/purego/internal/ucschema"
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// version identifies this SDK in the gRPC user-agent header.
const version = "0.1.0"

// userAgentPrefix is sent on every request.
const userAgentPrefix = "zerobus-sdk-go-purego/" + version

// HeadersProvider supplies the gRPC authentication metadata for a stream.
// Implement it to plug in custom authentication with CreateStreamWithProvider;
// the built-in OAuth path (CreateStream) provides one for you.
//
// GetHeaders may block on token minting (bounded by ctx).
// Invalidate should drop cached credentials and return quickly.
type HeadersProvider = transport.HeadersProvider

// NewStaticHeadersProvider returns a HeadersProvider that returns the same fixed
// headers on every call. It is intended for tests or externally managed
// credentials; pair it with CreateStreamWithProvider.
func NewStaticHeadersProvider(headers map[string]string) HeadersProvider {
	return auth.NewStaticHeadersProvider(headers)
}

// SDK creates streams and owns shared connection state.
type SDK struct {
	zerobusEndpoint string
	ucEndpoint      string
	conn            *transport.Conn
	// Shared OAuth token cache across streams.
	tokenCache                *auth.SharedTokenCache
	httpClient                *http.Client
	dynamicSchemaFetchTimeout time.Duration

	// connection per stream
	connectionPerStream bool
	dialConn            func() (*transport.Conn, error)

	// mu guards the open-stream set and closed flag.
	mu     sync.Mutex
	closed bool
	// Open streams owned by this SDK.
	streams map[*Stream]struct{}
}

const defaultDynamicSchemaFetchTimeout = 10 * time.Second

// newSDK builds an SDK around an existing connection.
func newSDK(conn *transport.Conn, zerobusEndpoint, ucEndpoint string, cfg sdkConfig) *SDK {
	fetchTimeout := cfg.dynamicSchemaFetchTimeout
	if fetchTimeout <= 0 {
		fetchTimeout = defaultDynamicSchemaFetchTimeout
	}
	return &SDK{
		zerobusEndpoint:           strings.TrimSpace(zerobusEndpoint),
		ucEndpoint:                strings.TrimSpace(ucEndpoint),
		conn:                      conn,
		tokenCache:                auth.NewSharedTokenCache(),
		httpClient:                cfg.httpClient,
		dynamicSchemaFetchTimeout: fetchTimeout,
		connectionPerStream:       cfg.connectionPerStream,
		streams:                   make(map[*Stream]struct{}),
	}
}

// newSdkWithDial builds an SDK around a dial func for creating a new connection
func newSdkWithDial(dialConn func() (*transport.Conn, error), zerobusEndpoint, ucEndpoint string, cfg sdkConfig) *SDK {
	sdk := newSDK(nil, zerobusEndpoint, ucEndpoint, cfg)
	sdk.dialConn = dialConn
	return sdk
}

// New connects to the Zerobus service and returns an SDK.
//
// zerobusEndpoint is the Zerobus gRPC endpoint (e.g.
// "https://ws.zerobus.region.cloud.databricks.com"). ucEndpoint is the Unity
// Catalog workspace URL used to mint OAuth tokens (e.g.
// "https://ws.cloud.databricks.com").
//
// TLS uses host root CAs unless overridden by WithTLSConfig.
// Dialing is lazy and happens on first stream open.
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

	dialConn := func() (*transport.Conn, error) {
		return transport.Dial(target, dialOpts...)
	}

	// Don't create a shared connection if each stream has dedicated conn
	// pass a dialer so each stream can create its own connection
	if cfg.connectionPerStream {
		return newSdkWithDial(dialConn, zerobusEndpoint, ucEndpoint, cfg), nil
	}

	// shared connection using dialer
	conn, err := dialConn()
	if err != nil {
		return nil, &Error{Op: "New", cause: err, retryable: false}
	}
	return newSDK(conn, zerobusEndpoint, ucEndpoint, cfg), nil
}

// Close terminates open streams and releases the shared connection.
// It is idempotent; CreateStream fails after Close.
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

	if !s.connectionPerStream {
		// Close the connection after stream teardown.
		errs[len(open)] = s.conn.Close()
	}

	return wrapErr("Close", errors.Join(errs...))
}

// CreateStream opens an OAuth-authenticated ingestion stream for tableName.
// By default it returns quickly and first-open runs in the background.
// Use WithWaitForReady to block until first-open succeeds or fails.
func (s *SDK) CreateStream(
	ctx context.Context,
	tableName, clientID, clientSecret string,
	opts ...StreamOption,
) (*Stream, error) {
	authOpts := []auth.OAuthOption{auth.WithSharedTokenCache(s.tokenCache)}
	if s.httpClient != nil {
		authOpts = append(authOpts, auth.WithHTTPClient(s.httpClient))
	}
	provider, err := auth.NewOAuthHeadersProvider(
		clientID, clientSecret, s.zerobusEndpoint, s.ucEndpoint,
		authOpts...,
	)
	if err != nil {
		return nil, &Error{Op: "CreateStream", cause: err, retryable: false}
	}
	return s.createStream(ctx, "CreateStream", tableName, provider, opts...)
}

// CreateStreamWithProvider opens a stream with a custom HeadersProvider.
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

// FetchProtoDescriptorFromUC returns a protobuf descriptor built from a Unity
// Catalog table schema. Every call performs a fresh Unity Catalog request; the
// SDK does not cache the result.
func (s *SDK) FetchProtoDescriptorFromUC(
	ctx context.Context,
	tableName, clientID, clientSecret string,
) ([]byte, error) {
	if s.isClosed() {
		return nil, &Error{
			Op:        "FetchProtoDescriptorFromUC",
			cause:     fmt.Errorf("SDK is closed"),
			retryable: false,
		}
	}
	descBytes, err := s.fetchProtoDescriptor(
		ctx, tableName, clientID, clientSecret,
	)
	if err != nil {
		return nil, &Error{
			Op:        "FetchProtoDescriptorFromUC",
			cause:     err,
			retryable: dynamicSchemaErrorRetryable(err),
		}
	}
	return descBytes, nil
}

// ColumnsFromDescriptor returns the column names declared by a serialized
// protobuf descriptor, such as the bytes from FetchProtoDescriptorFromUC or
// WithProto. Only top-level fields are columns; the fields of a nested message
// belong to that column's value.
func ColumnsFromDescriptor(raw []byte) (map[string]struct{}, error) {
	columns, err := columnsFromDescriptor(raw)
	if err != nil {
		return nil, &Error{Op: "ColumnsFromDescriptor", cause: err, retryable: false}
	}
	return columns, nil
}

func (s *SDK) createStream(
	ctx context.Context,
	op, tableName string,
	provider HeadersProvider,
	opts ...StreamOption,
) (*Stream, error) {
	return s.createStreamConfigured(ctx, op, tableName, provider, streamConfigFromOptions(opts))
}

func streamConfigFromOptions(opts []StreamOption) streamConfig {
	sc := defaultStreamConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&sc)
		}
	}
	return sc
}

func (s *SDK) createStreamConfigured(
	ctx context.Context,
	op, tableName string,
	provider HeadersProvider,
	sc streamConfig,
) (*Stream, error) {
	if err := validateStreamArgs(tableName, sc); err != nil {
		return nil, &Error{Op: op, cause: err, retryable: false}
	}

	params := stream.StreamParams{
		TableName:       tableName,
		RecordType:      sc.recordType,
		DescriptorProto: sc.descriptor,
		HeadersProvider: provider,
	}
	openingCtx := context.WithoutCancel(ctx)
	if sc.waitReady {
		openingCtx = ctx
	}

	// create a new connection for each stream if enabled else shared
	conn := s.conn
	if s.connectionPerStream {
		streamConn, dialErr := s.dialConn()
		if dialErr != nil {
			return nil, wrapErr(op, dialErr)
		}
		conn = streamConn
	}

	core, err := stream.NewProtoJSONStream(openingCtx, conn, params, sc.cfg, sc.callback)
	if err != nil {
		if s.connectionPerStream {
			// close stream connection in case of error
			_ = conn.Close()
		}
		return nil, wrapErr(op, err)
	}
	st := &Stream{
		core:                    core,
		sdk:                     s,
		recordType:              sc.recordType,
		maxBatchRecords:         positiveOrDefault(sc.cfg.MaxBatchRecords, stream.DefaultMaxBatchRecords),
		maxBufferedPayloadBytes: positiveOrDefault64(sc.cfg.MaxBufferedPayloadBytes, stream.DefaultMaxBufferedPayloadBytes),
	}
	if sc.recordType == zerobuspb.RecordType_PROTO {
		st.jsonConverter, st.jsonConverterErr = dynamicproto.NewFromDescriptorProtoBytes(sc.descriptor)
		st.conversionGate = make(chan struct{}, 1)
	}

	if s.connectionPerStream {
		st.streamConn = conn
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		// Stop the stream created before Close won the race.
		_ = st.terminate() // this calls stream.Core.Terminate()
		return nil, &Error{Op: op, cause: fmt.Errorf("SDK is closed"), retryable: false}
	}
	s.streams[st] = struct{}{}
	s.mu.Unlock()

	if sc.waitReady {
		if err := st.core.WaitReady(ctx); err != nil {
			_ = st.terminate()
			s.forget(st)
			return nil, wrapErr(op, err)
		}
	}
	return st, nil
}

// validateStreamArgs fails fast on invalid stream arguments.
func validateStreamArgs(tableName string, sc streamConfig) error {
	if strings.TrimSpace(tableName) == "" {
		return fmt.Errorf("table name is required")
	}
	if sc.recordType == zerobuspb.RecordType_PROTO && len(sc.descriptor) == 0 {
		return fmt.Errorf("WithProto requires a non-empty descriptor proto")
	}
	return nil
}

func (s *SDK) fetchProtoDescriptor(
	ctx context.Context,
	tableName, clientID, clientSecret string,
) ([]byte, error) {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}
	if strings.TrimSpace(clientID) == "" {
		return nil, fmt.Errorf("clientID is required")
	}
	if strings.TrimSpace(clientSecret) == "" {
		return nil, fmt.Errorf("clientSecret is required")
	}

	fetcher, err := ucschema.New(ucschema.Config{
		WorkspaceEndpoint: s.ucEndpoint,
		ClientID:          clientID,
		ClientSecret:      clientSecret,
		HTTPClient:        s.httpClient,
		RequestTimeout:    s.dynamicSchemaFetchTimeout,
	})
	if err != nil {
		return nil, err
	}
	tableSchema, err := fetcher.FetchTableSchema(ctx, tableName)
	if err != nil {
		return nil, err
	}
	msgDescriptor, err := schema.DescriptorFromUCSchema(tableSchema)
	if err != nil {
		return nil, err
	}
	descBytes, err := proto.Marshal(msgDescriptor)
	if err != nil {
		return nil, fmt.Errorf("serialize descriptor: %w", err)
	}
	return descBytes, nil
}

func columnsFromDescriptor(raw []byte) (map[string]struct{}, error) {
	var descriptor descriptorpb.DescriptorProto
	if err := proto.Unmarshal(raw, &descriptor); err != nil {
		return nil, fmt.Errorf("parse descriptor: %w", err)
	}
	columns := make(map[string]struct{}, len(descriptor.GetField()))
	for _, field := range descriptor.GetField() {
		if name := field.GetName(); name != "" {
			columns[name] = struct{}{}
		}
	}
	if len(columns) == 0 {
		return nil, errors.New("table schema descriptor has no columns")
	}
	return columns, nil
}

func dynamicSchemaErrorRetryable(err error) bool {
	var classified interface{ IsRetryable() bool }
	return errors.As(err, &classified) && classified.IsRetryable()
}

func positiveOrDefault(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

func positiveOrDefault64(value, fallback int64) int64 {
	if value <= 0 {
		return fallback
	}
	return value
}

// validateApplicationName enforces gRPC metadata character constraints.
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

// forget removes a closed stream from the SDK registry.
func (s *SDK) forget(st *Stream) {
	s.mu.Lock()
	delete(s.streams, st)
	s.mu.Unlock()
}

func (s *SDK) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// grpcTarget converts endpoint input to a grpc.NewClient target.
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
