package dynamicproto

import (
	"strings"
	"testing"

	"github.com/databricks/zerobus-sdk/purego/internal/schema"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func testDescriptorBytes(t *testing.T) []byte {
	t.Helper()
	desc := &descriptorpb.DescriptorProto{
		Name: proto.String("Order"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{
				Name:     proto.String("id"),
				Number:   proto.Int32(1),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_REQUIRED.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
				JsonName: proto.String("id"),
			},
			{
				Name:     proto.String("customer"),
				Number:   proto.Int32(2),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				JsonName: proto.String("customer"),
			},
		},
	}
	b, err := proto.Marshal(desc)
	if err != nil {
		t.Fatalf("marshal descriptor: %v", err)
	}
	return b
}

func TestConverter_EncodeJSONBytes(t *testing.T) {
	c, err := NewFromDescriptorProtoBytes(testDescriptorBytes(t))
	if err != nil {
		t.Fatalf("NewFromDescriptorProtoBytes() error = %v", err)
	}
	out, err := c.EncodeJSONBytes([]byte(`{"id": 123, "customer":"alice"}`))
	if err != nil {
		t.Fatalf("EncodeJSONBytes() error = %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected encoded bytes")
	}
	msg := dynamicpb.NewMessage(c.message)
	if err := proto.Unmarshal(out, msg); err != nil {
		t.Fatalf("decode encoded bytes: %v", err)
	}
	id := c.message.Fields().ByName("id")
	if got := msg.Get(id).Int(); got != 123 {
		t.Fatalf("id = %d, want 123", got)
	}
}

func TestConverter_MissingRequiredField(t *testing.T) {
	c, err := NewFromDescriptorProtoBytes(testDescriptorBytes(t))
	if err != nil {
		t.Fatalf("NewFromDescriptorProtoBytes() error = %v", err)
	}
	_, err = c.EncodeJSONBytes([]byte(`{"customer":"alice"}`))
	if err == nil {
		t.Fatalf("expected required-field error")
	}
	if !strings.Contains(err.Error(), "required") {
		t.Fatalf("error = %v, want missing required field", err)
	}
}

func TestConverter_MissingNestedRequiredFields(t *testing.T) {
	desc, err := schema.DescriptorFromUCColumns([]schema.UcColumn{
		{
			Name:     "addr",
			TypeName: "STRUCT",
			TypeJSON: `{"type":"struct","fields":[` +
				`{"name":"zip","type":"string","nullable":false},` +
				`{"name":"geo","type":{"type":"struct","fields":[` +
				`{"name":"lat","type":"double","nullable":false}` +
				`]},"nullable":true}` +
				`]}`,
			Nullable: true,
			Position: 0,
		},
		{
			Name:     "items",
			TypeName: "ARRAY",
			TypeJSON: `{"type":"array","elementType":{"type":"struct","fields":[` +
				`{"name":"id","type":"long","nullable":false},` +
				`{"name":"inner","type":{"type":"struct","fields":[` +
				`{"name":"id","type":"long","nullable":false}` +
				`]},"nullable":true}` +
				`]}}`,
			Nullable: true,
			Position: 1,
		},
		{
			Name:     "lookup",
			TypeName: "MAP",
			TypeJSON: `{"type":"map","keyType":"string","valueType":{"type":"struct","fields":[` +
				`{"name":"v","type":"string","nullable":false}` +
				`]}}`,
			Nullable: true,
			Position: 2,
		},
	}, "RequiredFields")
	if err != nil {
		t.Fatalf("DescriptorFromUCColumns() error = %v", err)
	}
	descriptorBytes, err := proto.Marshal(desc)
	if err != nil {
		t.Fatalf("marshal descriptor: %v", err)
	}
	converter, err := NewFromDescriptorProtoBytes(descriptorBytes)
	if err != nil {
		t.Fatalf("NewFromDescriptorProtoBytes() error = %v", err)
	}

	tests := []struct {
		name    string
		record  string
		missing string
	}{
		{name: "struct", record: `{"addr":{}}`, missing: "zip"},
		{name: "array element", record: `{"items":[{"id":"1"},{}]}`, missing: "id"},
		{name: "map value", record: `{"lookup":{"work":{}}}`, missing: "v"},
		{
			name:    "deep struct",
			record:  `{"addr":{"zip":"12345","geo":{}}}`,
			missing: "lat",
		},
		{
			name:    "deep array element",
			record:  `{"items":[{"id":"1","inner":{}}]}`,
			missing: "id",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := converter.EncodeJSONBytes([]byte(tc.record))
			if err == nil {
				t.Fatalf("missing nested required field %q was accepted", tc.missing)
			}
			if !strings.Contains(err.Error(), "required") ||
				!strings.Contains(err.Error(), tc.missing) {
				t.Fatalf("error = %v, want required field %q", err, tc.missing)
			}
		})
	}

	if _, err := converter.EncodeJSONBytes([]byte(
		`{"addr":{"zip":"12345","geo":{"lat":1.5}},` +
			`"items":[{"id":"1","inner":{"id":"2"}}],` +
			`"lookup":{"work":{"v":"ok"}}}`,
	)); err != nil {
		t.Fatalf("complete nested record rejected: %v", err)
	}
}

func TestConverter_UnknownFieldIgnored(t *testing.T) {
	c, err := NewFromDescriptorProtoBytes(testDescriptorBytes(t))
	if err != nil {
		t.Fatalf("NewFromDescriptorProtoBytes() error = %v", err)
	}
	if _, err := c.EncodeJSONBytes([]byte(`{"id":1, "unknown": true}`)); err != nil {
		t.Fatalf("EncodeJSONBytes() error = %v", err)
	}
}

func TestConverter_MalformedDescriptorReportsFallbackError(t *testing.T) {
	_, err := NewFromDescriptorProtoBytes([]byte{0x0a, 0x01})
	if err == nil {
		t.Fatal("expected malformed descriptor error")
	}
	if strings.Contains(err.Error(), "%!w(<nil>)") {
		t.Fatalf("error wrapped nil instead of fallback parse error: %v", err)
	}
}
