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
//
// # Arrow Flight (Beta)
//
// Arrow Flight ingestion is in Beta. CreateArrowStream accepts an
// *arrow.Schema; CreateArrowStreamFromIPC accepts a schema-only Arrow IPC
// stream. The WithProvider variants use custom authentication.
//
// Queue typed RecordBatch values without per-batch waits. IngestBatch
// serializes synchronously, so release each caller-owned batch after it
// returns:
//
//	for _, batch := range batches {
//	    _, err := stream.IngestBatch(batch)
//	    batch.Release()
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	}
//	if err := stream.Flush(); err != nil {
//	    log.Fatal(err)
//	}
//
// Arrow streams default to 1,000 unacknowledged batches and split rows into
// FlightData messages of at most 2 MiB. Partial record-count acknowledgements
// are retained across recovery. GetUnackedBatches returns caller-owned
// RecordBatch references that must each be released; GetUnackedIPCBatches
// returns independent byte slices.
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
// credentials; pair it with CreateStreamWithProvider or
// CreateArrowStreamWithProvider.
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
	dynamicSchemaFetches      map[descriptorFetchKey]*descriptorFetch
	descriptorGeneration      uint64
	descriptorFetchWG         sync.WaitGroup

	// mu guards the open-stream set, descriptor cache/fetches, and closed flag.
	mu     sync.Mutex
	closed bool
	// Open streams owned by this SDK.
	streams map[sdkOwnedStream]struct{}
}

type sdkOwnedStream interface {
	terminate() error
}

type cachedDescriptor struct {
	descriptor []byte
	expiresAt  time.Time
	storedAt   time.Time
	generation uint64
}

type descriptorFetchKey struct {
	cacheKey string
	refresh  bool
}

type descriptorFetch struct {
	key        descriptorFetchKey
	done       chan struct{}
	descriptor []byte
	err        error
	waiters    int
	generation uint64
	ctx        context.Context
	cancel     context.CancelCauseFunc
	completed  bool
	superseded bool
}

const (
	defaultDynamicSchemaFetchTimeout = 10 * time.Second
	defaultDynamicSchemaCacheTTL     = 5 * time.Minute
	maxDynamicSchemaCacheEntries     = 128
)

var (
	errSDKClosed                = errors.New("SDK is closed")
	errDescriptorFetchAbandoned = errors.New("descriptor fetch abandoned")
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
		dynamicSchemaFetches:      make(map[descriptorFetchKey]*descriptorFetch),
		streams:                   make(map[sdkOwnedStream]struct{}),
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
// It is idempotent; stream constructors fail after Close.
func (s *SDK) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	open := make([]sdkOwnedStream, 0, len(s.streams))
	for st := range s.streams {
		open = append(open, st)
	}
	s.streams = nil
	s.dynamicSchemaCache = make(map[string]cachedDescriptor)
	for key, fetch := range s.dynamicSchemaFetches {
		delete(s.dynamicSchemaFetches, key)
		s.completeDescriptorFetchLocked(fetch, nil, errSDKClosed)
		if fetch.cancel != nil {
			fetch.cancel(errSDKClosed)
		}
	}
	s.mu.Unlock()
	s.descriptorFetchWG.Wait()

	errs := make([]error, len(open)+1)
	var wg sync.WaitGroup
	for i, st := range open {
		wg.Add(1)
		go func(index int, owned sdkOwnedStream) {
			defer wg.Done()
			errs[index] = owned.terminate()
		}(i, st)
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

// FetchProtoDescriptorFromUC returns a protobuf descriptor built from a Unity
// Catalog table schema. Successful descriptors are cached for five minutes,
// with at most 128 entries per SDK. Concurrent misses for the same table and
// credentials share one request. Use RefreshProtoDescriptorFromUC to bypass a
// cached descriptor after a schema change.
func (s *SDK) FetchProtoDescriptorFromUC(
	ctx context.Context,
	tableName, clientID, clientSecret string,
) ([]byte, error) {
	return s.fetchProtoDescriptorFromUC(
		ctx,
		"FetchProtoDescriptorFromUC",
		tableName,
		clientID,
		clientSecret,
		false,
	)
}

// RefreshProtoDescriptorFromUC fetches a fresh Unity Catalog table schema,
// replaces its cached descriptor, and returns it. Concurrent refreshes for the
// same table and credentials share one request. Cancelling a caller does not
// cancel a fetch that another caller still needs.
func (s *SDK) RefreshProtoDescriptorFromUC(
	ctx context.Context,
	tableName, clientID, clientSecret string,
) ([]byte, error) {
	return s.fetchProtoDescriptorFromUC(
		ctx,
		"RefreshProtoDescriptorFromUC",
		tableName,
		clientID,
		clientSecret,
		true,
	)
}

func (s *SDK) fetchProtoDescriptorFromUC(
	ctx context.Context,
	op, tableName, clientID, clientSecret string,
	refresh bool,
) ([]byte, error) {
	if s.isClosed() {
		return nil, &Error{
			Op:        op,
			cause:     errSDKClosed,
			retryable: false,
		}
	}
	descBytes, err := s.fetchProtoDescriptorCached(
		ctx, tableName, clientID, clientSecret, refresh,
	)
	if err != nil {
		return nil, &Error{
			Op:        op,
			cause:     err,
			retryable: dynamicSchemaErrorRetryable(err),
		}
	}
	return descBytes, nil
}

// FetchProtoDescriptor returns a protobuf descriptor built from a Unity Catalog
// table schema.
//
// Deprecated: use FetchProtoDescriptorFromUC.
func (s *SDK) FetchProtoDescriptor(
	ctx context.Context,
	tableName, clientID, clientSecret string,
) ([]byte, error) {
	return s.FetchProtoDescriptorFromUC(ctx, tableName, clientID, clientSecret)
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
		return nil, &Error{Op: op, cause: errSDKClosed, retryable: false}
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
	return s.fetchProtoDescriptorCached(ctx, tableName, clientID, clientSecret, false)
}

func (s *SDK) fetchProtoDescriptorCached(
	ctx context.Context,
	tableName, clientID, clientSecret string,
	refresh bool,
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
	if err := ctx.Err(); err != nil {
		return nil, context.Cause(ctx)
	}

	cacheKey := dynamicSchemaCacheKey(s.ucEndpoint, tableName, clientID, clientSecret)
	desc, cached, fetch, leader, err := s.cachedDescriptorOrJoinFetch(
		ctx, cacheKey, refresh,
	)
	if err != nil {
		return nil, err
	}
	if cached {
		return desc, nil
	}
	if leader {
		go s.runDescriptorFetch(
			fetch.ctx,
			tableName,
			clientID,
			clientSecret,
			fetch,
		)
	}
	return s.waitForDescriptorFetch(ctx, fetch)
}

func (s *SDK) loadProtoDescriptor(
	ctx context.Context,
	tableName, clientID, clientSecret string,
) ([]byte, error) {
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

func (s *SDK) cachedDescriptorOrJoinFetch(
	ctx context.Context,
	cacheKey string,
	refresh bool,
) ([]byte, bool, *descriptorFetch, bool, error) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false, nil, false, errSDKClosed
	}
	refreshKey := descriptorFetchKey{cacheKey: cacheKey, refresh: true}
	if refresh {
		if fetch, ok := s.dynamicSchemaFetches[refreshKey]; ok {
			fetch.waiters++
			return nil, false, fetch, false, nil
		}
	}
	if !refresh {
		if item, ok := s.dynamicSchemaCache[cacheKey]; ok {
			if now.After(item.expiresAt) {
				delete(s.dynamicSchemaCache, cacheKey)
			} else {
				dup := append([]byte(nil), item.descriptor...)
				return dup, true, nil, false, nil
			}
		}
		if fetch, ok := s.dynamicSchemaFetches[refreshKey]; ok {
			fetch.waiters++
			return nil, false, fetch, false, nil
		}
	}
	fetchKey := descriptorFetchKey{cacheKey: cacheKey, refresh: refresh}
	if !refresh {
		if fetch, ok := s.dynamicSchemaFetches[fetchKey]; ok && !fetch.superseded {
			fetch.waiters++
			return nil, false, fetch, false, nil
		}
	} else {
		// Starting a refresh establishes a freshness barrier even if it fails.
		ordinaryKey := descriptorFetchKey{cacheKey: cacheKey}
		if ordinary := s.dynamicSchemaFetches[ordinaryKey]; ordinary != nil {
			ordinary.superseded = true
		}
	}
	sharedCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	s.descriptorGeneration++
	fetch := &descriptorFetch{
		key:        fetchKey,
		done:       make(chan struct{}),
		waiters:    1,
		generation: s.descriptorGeneration,
		ctx:        sharedCtx,
		cancel:     cancel,
	}
	s.dynamicSchemaFetches[fetchKey] = fetch
	s.descriptorFetchWG.Add(1)
	return nil, false, fetch, true, nil
}

func (s *SDK) runDescriptorFetch(
	ctx context.Context,
	tableName, clientID, clientSecret string,
	fetch *descriptorFetch,
) {
	defer s.descriptorFetchWG.Done()
	desc, err := s.loadProtoDescriptor(ctx, tableName, clientID, clientSecret)

	s.mu.Lock()
	defer s.mu.Unlock()
	if fetch.completed {
		return
	}
	current := s.dynamicSchemaFetches[fetch.key] == fetch
	if err == nil && !s.closed && current {
		if !fetch.superseded {
			item, cached := s.dynamicSchemaCache[fetch.key.cacheKey]
			if !cached || fetch.generation >= item.generation {
				s.storeDynamicDescriptorLocked(
					fetch.key.cacheKey, desc, time.Now(), fetch.generation,
				)
			}
		}
	}
	if current {
		delete(s.dynamicSchemaFetches, fetch.key)
	}
	s.completeDescriptorFetchLocked(fetch, desc, err)
}

func (s *SDK) waitForDescriptorFetch(
	ctx context.Context,
	fetch *descriptorFetch,
) ([]byte, error) {
	var desc []byte
	var err error
	select {
	case <-fetch.done:
		desc = append([]byte(nil), fetch.descriptor...)
		err = fetch.err
	case <-ctx.Done():
		err = context.Cause(ctx)
	}
	s.leaveDescriptorFetch(fetch)
	return desc, err
}

func (s *SDK) leaveDescriptorFetch(fetch *descriptorFetch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fetch.waiters > 0 {
		fetch.waiters--
	}
	if fetch.waiters != 0 || fetch.completed {
		return
	}
	if current := s.dynamicSchemaFetches[fetch.key]; current == fetch {
		delete(s.dynamicSchemaFetches, fetch.key)
	}
	s.completeDescriptorFetchLocked(fetch, nil, errDescriptorFetchAbandoned)
	if fetch.cancel != nil {
		fetch.cancel(errDescriptorFetchAbandoned)
	}
}

func (s *SDK) completeDescriptorFetchLocked(
	fetch *descriptorFetch,
	desc []byte,
	err error,
) {
	if fetch.completed {
		return
	}
	fetch.descriptor = append([]byte(nil), desc...)
	fetch.err = err
	fetch.completed = true
	close(fetch.done)
}

func (s *SDK) storeDynamicDescriptor(
	cacheKey string,
	desc []byte,
) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.descriptorGeneration++
	s.storeDynamicDescriptorLocked(cacheKey, desc, now, s.descriptorGeneration)
}

func (s *SDK) storeDynamicDescriptorLocked(
	cacheKey string,
	desc []byte,
	now time.Time,
	generation uint64,
) {
	item := cachedDescriptor{
		descriptor: append([]byte(nil), desc...),
		expiresAt:  now.Add(defaultDynamicSchemaCacheTTL),
		storedAt:   now,
		generation: generation,
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
func (s *SDK) forget(st sdkOwnedStream) {
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
