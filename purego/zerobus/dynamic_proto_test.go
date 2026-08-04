package zerobus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/dynamicproto"
	"github.com/databricks/zerobus-sdk/purego/internal/stream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

type retryableSchemaTestError struct{}

func (retryableSchemaTestError) Error() string     { return "retryable" }
func (retryableSchemaTestError) IsRetryable() bool { return true }

func TestSDKDynamicDescriptor_CacheHit(t *testing.T) {
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
		dynamicSchemaHTTPClient:   server.Client(),
		dynamicSchemaFetchTimeout: time.Second,
	})
	b1, c1, err := sdk.dynamicDescriptorAndConverter(
		context.Background(), "main.sales.orders", "id", "secret",
	)
	if err != nil {
		t.Fatalf("dynamicDescriptorAndConverter() first call error = %v", err)
	}
	b2, c2, err := sdk.dynamicDescriptorAndConverter(
		context.Background(), "main.sales.orders", "id", "secret",
	)
	if err != nil {
		t.Fatalf("dynamicDescriptorAndConverter() second call error = %v", err)
	}
	if string(b1) != string(b2) {
		t.Fatalf("cached descriptor mismatch")
	}
	if c1 != c2 {
		t.Fatal("cached converter was rebuilt")
	}
	if tokenCalls.Load() != 1 {
		t.Fatalf("token calls = %d, want 1", tokenCalls.Load())
	}
	if schemaCalls.Load() != 1 {
		t.Fatalf("schema calls = %d, want 1", schemaCalls.Load())
	}
}

func TestDynamicProtoStreamEncodeBatch(t *testing.T) {
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
	ds := &DynamicProtoStream{converter: c}
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

func TestDynamicProtoStreamMessageDescriptor(t *testing.T) {
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

	stream := &DynamicProtoStream{converter: converter}
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

func TestSDKDynamicDescriptor_CacheIsCredentialScoped(t *testing.T) {
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
		dynamicSchemaHTTPClient: server.Client(),
	})
	for _, credentials := range [][2]string{
		{"id-1", "secret-1"},
		{"id-2", "secret-1"},
		{"id-1", "secret-2"},
	} {
		if _, _, err := sdk.dynamicDescriptorAndConverter(
			context.Background(), "main.sales.orders", credentials[0], credentials[1],
		); err != nil {
			t.Fatalf("dynamicDescriptorAndConverter() error = %v", err)
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
	sdk.storeDynamicDescriptor("new", []byte("new"), nil)
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
		sdk.storeDynamicDescriptor(fmt.Sprintf("entry-%03d", i), []byte("descriptor"), nil)
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
	sdk.storeDynamicDescriptor("closed", []byte("descriptor"), nil)
	if len(sdk.dynamicSchemaCache) != 0 {
		t.Fatal("closed SDK cache was populated")
	}
}

func TestDynamicProtoStreamRejectsBatchBeforeConversion(t *testing.T) {
	ds := &DynamicProtoStream{
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
