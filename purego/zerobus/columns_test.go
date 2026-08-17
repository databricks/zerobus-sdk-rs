package zerobus_test

import (
	"errors"
	"maps"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/databricks/zerobus-sdk/purego/internal/schema"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

func TestColumnsFromDescriptorMatchesUCColumns(t *testing.T) {
	descriptor, err := schema.DescriptorFromUCColumns([]schema.UcColumn{
		{Name: "id", TypeName: "BIGINT", Position: 0},
		{Name: "payload", TypeName: "STRING", Position: 1, Nullable: true},
	}, "events")
	if err != nil {
		t.Fatalf("DescriptorFromUCColumns: %v", err)
	}
	raw, err := proto.Marshal(descriptor)
	if err != nil {
		t.Fatalf("marshal descriptor: %v", err)
	}

	got, err := zerobus.ColumnsFromDescriptor(raw)
	if err != nil {
		t.Fatalf("ColumnsFromDescriptor: %v", err)
	}
	want := map[string]struct{}{"id": {}, "payload": {}}
	if !maps.Equal(got, want) {
		t.Fatalf("columns = %v, want %v", got, want)
	}
}

func TestColumnsFromDescriptorSkipsNestedAndUnnamedFields(t *testing.T) {
	raw := mustMarshalDescriptor(t, &descriptorpb.DescriptorProto{
		Name: proto.String("events"),
		Field: []*descriptorpb.FieldDescriptorProto{
			descriptorField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64),
			{
				Name:     proto.String("address"),
				Number:   proto.Int32(2),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName: proto.String("address"),
			},
			{Number: proto.Int32(3), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
		},
		NestedType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("address"),
			Field: []*descriptorpb.FieldDescriptorProto{
				descriptorField("city", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING),
			},
		}},
	})

	got, err := zerobus.ColumnsFromDescriptor(raw)
	if err != nil {
		t.Fatalf("ColumnsFromDescriptor: %v", err)
	}
	want := map[string]struct{}{"id": {}, "address": {}}
	if !maps.Equal(got, want) {
		t.Fatalf("columns = %v, want %v", got, want)
	}
}

func TestColumnsFromDescriptorErrors(t *testing.T) {
	tests := []struct {
		name    string
		raw     []byte
		wantErr string
	}{
		{
			name:    "truncated bytes",
			raw:     []byte{0x0a, 0x05, 'e'},
			wantErr: "parse descriptor",
		},
		{
			name:    "empty bytes",
			raw:     nil,
			wantErr: "has no columns",
		},
		{
			name:    "descriptor without fields",
			raw:     mustMarshalDescriptor(t, &descriptorpb.DescriptorProto{Name: proto.String("events")}),
			wantErr: "has no columns",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := zerobus.ColumnsFromDescriptor(tc.raw)
			if err == nil {
				t.Fatalf("ColumnsFromDescriptor returned %v, want error", got)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.wantErr)
			}
			var sdkErr *zerobus.Error
			if !errors.As(err, &sdkErr) {
				t.Fatalf("error %v is not a *zerobus.Error", err)
			}
			if sdkErr.Op != "ColumnsFromDescriptor" {
				t.Fatalf("Op = %q, want %q", sdkErr.Op, "ColumnsFromDescriptor")
			}
			if zerobus.Retryable(err) {
				t.Fatal("a malformed descriptor must not be retryable")
			}
		})
	}
}

func descriptorField(
	name string,
	number int32,
	typ descriptorpb.FieldDescriptorProto_Type,
) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   typ.Enum(),
	}
}

func mustMarshalDescriptor(t *testing.T, descriptor *descriptorpb.DescriptorProto) []byte {
	t.Helper()
	raw, err := proto.Marshal(descriptor)
	if err != nil {
		t.Fatalf("marshal descriptor: %v", err)
	}
	return raw
}
