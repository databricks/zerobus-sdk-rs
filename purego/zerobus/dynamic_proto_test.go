package zerobus

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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

// The SDK does not cache descriptors: each call is a fresh UC round-trip that
// returns an equivalent descriptor. Callers fetch once and reuse the bytes.
func TestSDKFetchProtoDescriptor_FetchesOnEveryCall(t *testing.T) {
	var schemaCalls atomic.Int32

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
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
		t.Fatal("descriptors from repeated fetches differ")
	}
	if got := schemaCalls.Load(); got != 2 {
		t.Fatalf("schema calls = %d, want 2 (no descriptor caching)", got)
	}
}

func TestSDKFetchProtoDescriptorFromUC_RejectsClosedSDK(t *testing.T) {
	sdk := newSDK(nil, "https://zerobus", "https://uc", sdkConfig{})
	sdk.mu.Lock()
	sdk.closed = true
	sdk.mu.Unlock()

	if _, err := sdk.FetchProtoDescriptorFromUC(
		context.Background(), "main.sales.orders", "id", "secret",
	); err == nil || !strings.Contains(err.Error(), "SDK is closed") {
		t.Fatalf("FetchProtoDescriptorFromUC() error = %v, want closed SDK error", err)
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
