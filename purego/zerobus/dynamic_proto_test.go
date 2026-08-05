package zerobus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/dynamicproto"
	"github.com/databricks/zerobus-sdk/purego/internal/stream"
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

type retryableSchemaTestError struct{}

func (retryableSchemaTestError) Error() string     { return "retryable" }
func (retryableSchemaTestError) IsRetryable() bool { return true }

func waitForDescriptorFetchWaiters(
	t *testing.T,
	sdk *SDK,
	cacheKey string,
	want int,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		sdk.mu.Lock()
		fetch := sdk.dynamicSchemaFetches[cacheKey]
		got := 0
		if fetch != nil {
			got = fetch.waiters
		}
		sdk.mu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("descriptor fetch waiters = %d, want %d", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSDKFetchProtoDescriptor_CacheHit(t *testing.T) {
	var tokenCalls atomic.Int32
	var schemaCalls atomic.Int32

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
			tokenCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "abc"})
		case strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/"):
			schemaCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":         "orders",
				"catalog_name": "main",
				"schema_name":  "sales",
				"columns": []map[string]any{
					{"name": "id", "type_name": "LONG", "position": 0},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	sdk := newSDK(nil, "https://workspace.zerobus.cloud.databricks.com", server.URL, sdkConfig{
		httpClient:                server.Client(),
		dynamicSchemaFetchTimeout: time.Second,
	})
	b1, err := sdk.fetchProtoDescriptor(
		context.Background(), "main.sales.orders", "id", "secret",
	)
	if err != nil {
		t.Fatalf("fetchProtoDescriptor() first call error = %v", err)
	}
	b2, err := sdk.fetchProtoDescriptor(
		context.Background(), "main.sales.orders", "id", "secret",
	)
	if err != nil {
		t.Fatalf("fetchProtoDescriptor() second call error = %v", err)
	}
	if string(b1) != string(b2) {
		t.Fatalf("cached descriptor mismatch")
	}
	if tokenCalls.Load() != 1 {
		t.Fatalf("token calls = %d, want 1", tokenCalls.Load())
	}
	if schemaCalls.Load() != 1 {
		t.Fatalf("schema calls = %d, want 1", schemaCalls.Load())
	}
}

func TestSDKRefreshProtoDescriptorFromUC_BypassesAndReplacesCache(t *testing.T) {
	var tokenCalls atomic.Int32
	var schemaCalls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
			tokenCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "abc"})
		case strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/"):
			call := schemaCalls.Add(1)
			columns := []map[string]any{
				{"name": "id", "type_name": "LONG", "position": 0},
			}
			if call > 1 {
				columns = append(columns, map[string]any{
					"name": "note", "type_name": "STRING", "position": 1,
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":         "orders",
				"catalog_name": "main",
				"schema_name":  "sales",
				"columns":      columns,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	sdk := newSDK(nil, "https://workspace.zerobus.cloud.databricks.com", server.URL, sdkConfig{
		httpClient: server.Client(),
	})
	ctx := context.Background()
	first, err := sdk.FetchProtoDescriptorFromUC(ctx, "main.sales.orders", "id", "secret")
	if err != nil {
		t.Fatalf("FetchProtoDescriptorFromUC() first call error = %v", err)
	}
	cached, err := sdk.FetchProtoDescriptorFromUC(ctx, "main.sales.orders", "id", "secret")
	if err != nil {
		t.Fatalf("FetchProtoDescriptorFromUC() cached call error = %v", err)
	}
	if string(first) != string(cached) {
		t.Fatal("cached descriptor differs from first fetch")
	}

	refreshed, err := sdk.RefreshProtoDescriptorFromUC(ctx, "main.sales.orders", "id", "secret")
	if err != nil {
		t.Fatalf("RefreshProtoDescriptorFromUC() error = %v", err)
	}
	if string(first) == string(refreshed) {
		t.Fatal("refresh returned the stale descriptor")
	}
	afterRefresh, err := sdk.FetchProtoDescriptorFromUC(ctx, "main.sales.orders", "id", "secret")
	if err != nil {
		t.Fatalf("FetchProtoDescriptorFromUC() after refresh error = %v", err)
	}
	if string(refreshed) != string(afterRefresh) {
		t.Fatal("refreshed descriptor was not cached")
	}
	if got := tokenCalls.Load(); got != 2 {
		t.Fatalf("token calls = %d, want 2", got)
	}
	if got := schemaCalls.Load(); got != 2 {
		t.Fatalf("schema calls = %d, want 2", got)
	}
}

func TestSDKFetchProtoDescriptor_CoalescesConcurrentMisses(t *testing.T) {
	var tokenCalls atomic.Int32
	var schemaCalls atomic.Int32
	schemaStarted := make(chan struct{}, 1)
	releaseSchema := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSchema) }) }

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
			tokenCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "abc"})
		case strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/"):
			schemaCalls.Add(1)
			select {
			case schemaStarted <- struct{}{}:
			default:
			}
			<-releaseSchema
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":         "orders",
				"catalog_name": "main",
				"schema_name":  "sales",
				"columns": []map[string]any{
					{"name": "id", "type_name": "LONG", "position": 0},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	defer release()

	sdk := newSDK(nil, "https://workspace.zerobus.cloud.databricks.com", server.URL, sdkConfig{
		httpClient: server.Client(),
	})
	const callers = 16
	results := make([][]byte, callers)
	errs := make([]error, callers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i], errs[i] = sdk.FetchProtoDescriptorFromUC(
				context.Background(), "main.sales.orders", "id", "secret",
			)
		}()
	}
	close(start)

	select {
	case <-schemaStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("schema request did not start")
	}
	cacheKey := dynamicSchemaCacheKey(server.URL, "main.sales.orders", "id", "secret")
	waitForDescriptorFetchWaiters(t, sdk, cacheKey, callers)
	release()
	wg.Wait()

	for i := range callers {
		if errs[i] != nil {
			t.Fatalf("caller %d error = %v", i, errs[i])
		}
		if len(results[i]) == 0 {
			t.Fatalf("caller %d returned an empty descriptor", i)
		}
		if string(results[i]) != string(results[0]) {
			t.Fatalf("caller %d returned a different descriptor", i)
		}
	}
	if got := tokenCalls.Load(); got != 1 {
		t.Fatalf("token calls = %d, want 1", got)
	}
	if got := schemaCalls.Load(); got != 1 {
		t.Fatalf("schema calls = %d, want 1", got)
	}
}

func TestSDKFetchProtoDescriptor_CallerCancellationDoesNotCancelSharedFetch(t *testing.T) {
	var tokenCalls atomic.Int32
	var schemaCalls atomic.Int32
	schemaStarted := make(chan struct{}, 1)
	releaseSchema := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSchema) }) }

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
			tokenCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "abc"})
		case strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/"):
			schemaCalls.Add(1)
			select {
			case schemaStarted <- struct{}{}:
			default:
			}
			<-releaseSchema
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":         "orders",
				"catalog_name": "main",
				"schema_name":  "sales",
				"columns": []map[string]any{
					{"name": "id", "type_name": "LONG", "position": 0},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	defer release()

	sdk := newSDK(nil, "https://workspace.zerobus.cloud.databricks.com", server.URL, sdkConfig{
		httpClient: server.Client(),
	})
	leaderResult := make(chan error, 1)
	go func() {
		_, err := sdk.FetchProtoDescriptorFromUC(
			context.Background(), "main.sales.orders", "id", "secret",
		)
		leaderResult <- err
	}()
	select {
	case <-schemaStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("schema request did not start")
	}

	cacheKey := dynamicSchemaCacheKey(server.URL, "main.sales.orders", "id", "secret")
	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	waiterResult := make(chan error, 1)
	go func() {
		_, err := sdk.FetchProtoDescriptorFromUC(
			waiterCtx, "main.sales.orders", "id", "secret",
		)
		waiterResult <- err
	}()
	waitForDescriptorFetchWaiters(t, sdk, cacheKey, 2)
	cancelWaiter()
	select {
	case err := <-waiterResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled waiter error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled waiter did not return")
	}
	waitForDescriptorFetchWaiters(t, sdk, cacheKey, 1)

	release()
	select {
	case err := <-leaderResult:
		if err != nil {
			t.Fatalf("shared fetch error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shared fetch did not complete")
	}
	if got := tokenCalls.Load(); got != 1 {
		t.Fatalf("token calls = %d, want 1", got)
	}
	if got := schemaCalls.Load(); got != 1 {
		t.Fatalf("schema calls = %d, want 1", got)
	}
}

func TestStreamEncodeJSONBatch(t *testing.T) {
	desc := &descriptorpb.DescriptorProto{
		Name: proto.String("Order"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{
				Name:   proto.String("id"),
				Number: proto.Int32(1),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_REQUIRED.Enum(),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
			},
		},
	}
	b, err := proto.Marshal(desc)
	if err != nil {
		t.Fatalf("marshal descriptor: %v", err)
	}
	c, err := dynamicproto.NewFromDescriptorProtoBytes(b)
	if err != nil {
		t.Fatalf("NewFromDescriptorProtoBytes() error = %v", err)
	}
	ds := &Stream{
		recordType:     zerobuspb.RecordType_PROTO,
		jsonConverter:  c,
		conversionGate: make(chan struct{}, 1),
	}
	out, err := ds.encodeJSONBatchContext(context.Background(), [][]byte{
		[]byte(`{"id":1}`),
		[]byte(`{"id":2}`),
	})
	if err != nil {
		t.Fatalf("encodeJSONBatchContext() error = %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("batch len = %d, want 2", len(out))
	}
}

func TestStreamMessageDescriptor(t *testing.T) {
	desc := &descriptorpb.DescriptorProto{
		Name: proto.String("Order"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{
				Name:   proto.String("id"),
				Number: proto.Int32(1),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
			},
		},
	}
	b, err := proto.Marshal(desc)
	if err != nil {
		t.Fatalf("marshal descriptor: %v", err)
	}
	converter, err := dynamicproto.NewFromDescriptorProtoBytes(b)
	if err != nil {
		t.Fatalf("NewFromDescriptorProtoBytes() error = %v", err)
	}

	stream := &Stream{jsonConverter: converter}
	got := stream.MessageDescriptor()
	if got == nil {
		t.Fatal("MessageDescriptor() = nil")
	}
	if got.Name() != "Order" {
		t.Fatalf("MessageDescriptor().Name() = %q, want Order", got.Name())
	}
	if got.Fields().ByName("id") == nil {
		t.Fatal("MessageDescriptor() missing id field")
	}
}

func TestStreamJSONConversionRejectsUnsupportedDescriptor(t *testing.T) {
	message := &descriptorpb.DescriptorProto{Name: proto.String("Order")}
	fileBytes, err := proto.Marshal(&descriptorpb.FileDescriptorProto{
		Name:        proto.String("orders.proto"),
		Syntax:      proto.String("proto2"),
		MessageType: []*descriptorpb.DescriptorProto{message},
	})
	if err != nil {
		t.Fatalf("marshal FileDescriptorProto: %v", err)
	}
	for name, descriptor := range map[string][]byte{
		"malformed":           []byte("bad descriptor"),
		"FileDescriptorProto": fileBytes,
	} {
		t.Run(name, func(t *testing.T) {
			_, converterErr := dynamicproto.NewFromDescriptorProtoBytes(descriptor)
			if converterErr == nil {
				t.Fatal("descriptor unexpectedly supports JSON conversion")
			}
			stream := &Stream{
				recordType:       zerobuspb.RecordType_PROTO,
				jsonConverterErr: converterErr,
				conversionGate:   make(chan struct{}, 1),
			}
			if _, err := stream.IngestJSONOffset([]byte(`{"id":1}`)); err == nil ||
				!strings.Contains(err.Error(), "JSON conversion is unavailable") {
				t.Fatalf("IngestJSONOffset() error = %v, want conversion error", err)
			}
		})
	}
}

func TestSDKFetchProtoDescriptor_CacheIsCredentialScoped(t *testing.T) {
	var tokenCalls atomic.Int32
	var schemaCalls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
			tokenCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "abc"})
		case strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/"):
			schemaCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":         "orders",
				"catalog_name": "main",
				"schema_name":  "sales",
				"columns": []map[string]any{
					{"name": "id", "type_name": "LONG", "position": 0},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	sdk := newSDK(nil, "https://workspace.zerobus.cloud.databricks.com", server.URL, sdkConfig{
		httpClient: server.Client(),
	})
	for _, credentials := range [][2]string{
		{"id-1", "secret-1"},
		{"id-2", "secret-1"},
		{"id-1", "secret-2"},
	} {
		if _, err := sdk.fetchProtoDescriptor(
			context.Background(), "main.sales.orders", credentials[0], credentials[1],
		); err != nil {
			t.Fatalf("fetchProtoDescriptor() error = %v", err)
		}
	}
	if got := tokenCalls.Load(); got != 3 {
		t.Fatalf("token calls = %d, want 3", got)
	}
	if got := schemaCalls.Load(); got != 3 {
		t.Fatalf("schema calls = %d, want 3", got)
	}
}

func TestSDKStoreDynamicDescriptor_PrunesExpiredEntries(t *testing.T) {
	sdk := newSDK(nil, "https://zerobus", "https://uc", sdkConfig{})
	sdk.dynamicSchemaCache["old"] = cachedDescriptor{
		descriptor: []byte("old"),
		expiresAt:  time.Now().Add(-time.Second),
	}
	sdk.storeDynamicDescriptor("new", []byte("new"))
	if len(sdk.dynamicSchemaCache) != 1 {
		t.Fatalf("cache entries = %d, want 1", len(sdk.dynamicSchemaCache))
	}
	if _, ok := sdk.dynamicSchemaCache["new"]; !ok {
		t.Fatal("new cache entry missing")
	}
}

func TestSDKStoreDynamicDescriptor_EnforcesEntryLimit(t *testing.T) {
	sdk := newSDK(nil, "https://zerobus", "https://uc", sdkConfig{})
	for i := range maxDynamicSchemaCacheEntries + 1 {
		sdk.storeDynamicDescriptor(fmt.Sprintf("entry-%03d", i), []byte("descriptor"))
	}
	if got := len(sdk.dynamicSchemaCache); got != maxDynamicSchemaCacheEntries {
		t.Fatalf("cache entries = %d, want %d", got, maxDynamicSchemaCacheEntries)
	}
	if _, ok := sdk.dynamicSchemaCache["entry-000"]; ok {
		t.Fatal("oldest descriptor was not evicted")
	}
	if _, ok := sdk.dynamicSchemaCache[fmt.Sprintf("entry-%03d", maxDynamicSchemaCacheEntries)]; !ok {
		t.Fatal("newest descriptor was not cached")
	}
}

func TestSDKStoreDynamicDescriptor_DoesNotPopulateClosedSDK(t *testing.T) {
	sdk := newSDK(nil, "https://zerobus", "https://uc", sdkConfig{})
	sdk.mu.Lock()
	sdk.closed = true
	sdk.mu.Unlock()
	sdk.storeDynamicDescriptor("closed", []byte("descriptor"))
	if len(sdk.dynamicSchemaCache) != 0 {
		t.Fatal("closed SDK cache was populated")
	}
}

func TestStreamRejectsJSONBatchBeforeConversion(t *testing.T) {
	ds := &Stream{
		recordType:              zerobuspb.RecordType_PROTO,
		conversionGate:          make(chan struct{}, 1),
		maxBatchRecords:         1,
		maxBufferedPayloadBytes: 64,
	}
	if _, err := ds.IngestJSONRecordsOffset([][]byte{[]byte(`{}`), []byte(`{}`)}); !errors.Is(err, stream.ErrPayloadTooLarge) {
		t.Fatalf("batch count error = %v, want ErrPayloadTooLarge", err)
	}
	if _, err := ds.IngestJSONOffset(make([]byte, 65)); !errors.Is(err, stream.ErrPayloadTooLarge) {
		t.Fatalf("buffered payload error = %v, want ErrPayloadTooLarge", err)
	}
}

func TestDynamicSchemaErrorRetryable(t *testing.T) {
	if dynamicSchemaErrorRetryable(errors.New("bad schema")) {
		t.Fatal("plain schema validation error classified retryable")
	}
	if !dynamicSchemaErrorRetryable(retryableSchemaTestError{}) {
		t.Fatal("self-classified fetch error classified non-retryable")
	}
}
