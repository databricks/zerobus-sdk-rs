package zerobus

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/dynamicproto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

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
		dynamicSchemaCacheTTL:     time.Minute,
	})
	b1, err := sdk.dynamicDescriptor(context.Background(), "main.sales.orders", "id", "secret")
	if err != nil {
		t.Fatalf("dynamicDescriptor() first call error = %v", err)
	}
	b2, err := sdk.dynamicDescriptor(context.Background(), "main.sales.orders", "id", "secret")
	if err != nil {
		t.Fatalf("dynamicDescriptor() second call error = %v", err)
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
	out, err := ds.encodeJSONBatch([][]byte{
		[]byte(`{"id":1}`),
		[]byte(`{"id":2}`),
	})
	if err != nil {
		t.Fatalf("encodeJSONBatch() error = %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("batch len = %d, want 2", len(out))
	}
}
