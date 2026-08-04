// Package zerobus is a pure-Go client for Zerobus ingestion.
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
// Ingestion is asynchronous. Queue records in a loop and call Flush once.
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
// Stream creation is asynchronous by default: CreateStream returns after local
// validation while first-open runs in the background. Pass WithWaitForReady to
// make CreateStream block until first-open succeeds or fails terminally.
//
// # Authentication
//
// CreateStream uses the Unity Catalog OAuth 2.0 client-credentials flow. For
// custom authentication, implement HeadersProvider and use
// CreateStreamWithProvider.
package zerobus

import (
	"context"
	"crypto/sha256"
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
	dynamicSchemaCache        map[string]cachedDescriptor

	// mu guards the open-stream set, descriptor cache, and closed flag.
	mu     sync.Mutex
	closed bool
	// Open streams owned by this SDK.
	streams map[*Stream]struct{}
}

type cachedDescriptor struct {
	descriptor []byte
	expiresAt  time.Time
	storedAt   time.Time
}

const (
	defaultDynamicSchemaFetchTimeout = 10 * time.Second
	defaultDynamicSchemaCacheTTL     = 5 * time.Minute
	maxDynamicSchemaCacheEntries     = 128
)

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
		dynamicSchemaCache:        make(map[string]cachedDescriptor),
		streams:                   make(map[*Stream]struct{}),
	}
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

	conn, err := transport.Dial(target, dialOpts...)
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
	s.dynamicSchemaCache = make(map[string]cachedDescriptor)
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
	// Close the connection after stream teardown.
	errs[len(open)] = s.conn.Close()
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

// FetchProtoDescriptor returns a protobuf descriptor built from a Unity Catalog
// table schema.
func (s *SDK) FetchProtoDescriptor(
	ctx context.Context,
	tableName, clientID, clientSecret string,
) ([]byte, error) {
	if s.isClosed() {
		return nil, &Error{
			Op:        "FetchProtoDescriptor",
			cause:     fmt.Errorf("SDK is closed"),
			retryable: false,
		}
	}
	descBytes, err := s.fetchProtoDescriptor(
		ctx, tableName, clientID, clientSecret,
	)
	if err != nil {
		return nil, &Error{
			Op:        "FetchProtoDescriptor",
			cause:     err,
			retryable: dynamicSchemaErrorRetryable(err),
		}
	}
	return descBytes, nil
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
	core, err := stream.NewProtoJSONStream(openingCtx, s.conn, params, sc.cfg, sc.callback)
	if err != nil {
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
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		// Stop the stream created before Close won the race.
		_ = core.Terminate()
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

	cacheKey := dynamicSchemaCacheKey(s.ucEndpoint, tableName, clientID, clientSecret)
	if desc, ok := s.getDynamicDescriptorFromCache(cacheKey); ok {
		return desc, nil
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
	s.storeDynamicDescriptor(cacheKey, descBytes)
	return descBytes, nil
}

func (s *SDK) getDynamicDescriptorFromCache(
	cacheKey string,
) ([]byte, bool) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false
	}
	item, ok := s.dynamicSchemaCache[cacheKey]
	if !ok {
		return nil, false
	}
	if now.After(item.expiresAt) {
		delete(s.dynamicSchemaCache, cacheKey)
		return nil, false
	}
	dup := append([]byte(nil), item.descriptor...)
	return dup, true
}

func (s *SDK) storeDynamicDescriptor(
	cacheKey string,
	desc []byte,
) {
	now := time.Now()
	item := cachedDescriptor{
		descriptor: append([]byte(nil), desc...),
		expiresAt:  now.Add(defaultDynamicSchemaCacheTTL),
		storedAt:   now,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	for key, cached := range s.dynamicSchemaCache {
		if now.After(cached.expiresAt) {
			delete(s.dynamicSchemaCache, key)
		}
	}
	delete(s.dynamicSchemaCache, cacheKey)
	if len(s.dynamicSchemaCache) >= maxDynamicSchemaCacheEntries {
		var oldestKey string
		var oldestTime time.Time
		for key, cached := range s.dynamicSchemaCache {
			if oldestKey == "" || cached.storedAt.Before(oldestTime) {
				oldestKey = key
				oldestTime = cached.storedAt
			}
		}
		if oldestKey != "" {
			delete(s.dynamicSchemaCache, oldestKey)
		}
	}
	s.dynamicSchemaCache[cacheKey] = item
}

func dynamicSchemaCacheKey(ucEndpoint, tableName, clientID, clientSecret string) string {
	credential := sha256.Sum256([]byte(clientID + "\x00" + clientSecret))
	return ucEndpoint + "\x00" + tableName + "\x00" + string(credential[:])
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
