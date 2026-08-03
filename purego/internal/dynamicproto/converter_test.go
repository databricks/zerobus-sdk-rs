package dynamicproto

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
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

func TestConverter_UnknownField(t *testing.T) {
	c, err := NewFromDescriptorProtoBytes(testDescriptorBytes(t))
	if err != nil {
		t.Fatalf("NewFromDescriptorProtoBytes() error = %v", err)
	}
	_, err = c.EncodeJSONBytes([]byte(`{"id":1, "unknown": true}`))
	if err == nil {
		t.Fatalf("expected unknown field error")
	}
}
