package schema

import (
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
)

func fieldByName(t *testing.T, desc *descriptorpb.DescriptorProto, name string) *descriptorpb.FieldDescriptorProto {
	t.Helper()
	for _, field := range desc.GetField() {
		if field.GetName() == name {
			return field
		}
	}
	t.Fatalf("field %q not found", name)
	return nil
}

func TestDescriptorFromUCColumns_PrimitivesAndOrder(t *testing.T) {
	cols := []UcColumn{
		{Name: "b", TypeName: "INT", Position: 1},
		{Name: "a", TypeName: "STRING", Position: 0},
	}
	desc, err := DescriptorFromUCColumns(cols, "OrderMessage")
	if err != nil {
		t.Fatalf("DescriptorFromUCColumns() error = %v", err)
	}
	if got := desc.GetField()[0].GetName(); got != "a" {
		t.Fatalf("field[0].name = %q, want %q", got, "a")
	}
	if got := desc.GetField()[0].GetType(); got != descriptorpb.FieldDescriptorProto_TYPE_STRING {
		t.Fatalf("field[0].type = %v, want TYPE_STRING", got)
	}
	if got := desc.GetField()[1].GetNumber(); got != 2 {
		t.Fatalf("field[1].number = %d, want 2", got)
	}
}

func TestDescriptorFromUCColumns_ComplexTypes(t *testing.T) {
	cols := []UcColumn{
		{
			Name:     "attrs",
			TypeName: "MAP",
			TypeJSON: `{"name":"attrs","type":{"type":"map","keyType":"string","valueType":"integer"}}`,
			Position: 0,
		},
		{
			Name:     "items",
			TypeName: "ARRAY",
			TypeJSON: `{"name":"items","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"id","type":"long","nullable":false}]}}}`,
			Position: 1,
		},
	}
	desc, err := DescriptorFromUCColumns(cols, "Complex")
	if err != nil {
		t.Fatalf("DescriptorFromUCColumns() error = %v", err)
	}
	if len(desc.GetNestedType()) == 0 {
		t.Fatalf("expected nested messages for complex fields")
	}
	if got := desc.GetField()[0].GetLabel(); got != descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		t.Fatalf("map field label = %v, want REPEATED", got)
	}
	if got := desc.GetField()[1].GetLabel(); got != descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		t.Fatalf("array field label = %v, want REPEATED", got)
	}
}

func TestDescriptorFromUCColumns_InvalidName(t *testing.T) {
	_, err := DescriptorFromUCColumns([]UcColumn{
		{Name: "bad-name", TypeName: "STRING", Position: 0},
	}, "M")
	if err == nil {
		t.Fatalf("expected error for invalid field name")
	}
}

func TestDescriptorFromUCColumns_NestedArrayRejected(t *testing.T) {
	_, err := DescriptorFromUCColumns([]UcColumn{
		{
			Name:     "nested",
			TypeName: "ARRAY",
			TypeJSON: `{"name":"nested","type":{"type":"array","elementType":{"type":"array","elementType":"long"}}}`,
			Position: 0,
		},
	}, "M")
	if err == nil {
		t.Fatalf("expected error for nested arrays")
	}
}

func TestUcColumnNullableJSONDefault(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{name: "absent", raw: `{"name":"id","type_name":"LONG","position":0}`, want: true},
		{name: "true", raw: `{"name":"id","type_name":"LONG","nullable":true,"position":0}`, want: true},
		{name: "false", raw: `{"name":"id","type_name":"LONG","nullable":false,"position":0}`, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var column UcColumn
			if err := json.Unmarshal([]byte(tc.raw), &column); err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}
			if column.Nullable != tc.want {
				t.Fatalf("Nullable = %v, want %v", column.Nullable, tc.want)
			}
		})
	}
}

func TestDescriptorFromUCColumns_AllPrimitiveAliases(t *testing.T) {
	tests := []struct {
		typeName string
		want     descriptorpb.FieldDescriptorProto_Type
	}{
		{typeName: "STRING", want: descriptorpb.FieldDescriptorProto_TYPE_STRING},
		{typeName: "VARIANT", want: descriptorpb.FieldDescriptorProto_TYPE_STRING},
		{typeName: "DECIMAL", want: descriptorpb.FieldDescriptorProto_TYPE_STRING},
		{typeName: "LONG", want: descriptorpb.FieldDescriptorProto_TYPE_INT64},
		{typeName: "BIGINT", want: descriptorpb.FieldDescriptorProto_TYPE_INT64},
		{typeName: "INT", want: descriptorpb.FieldDescriptorProto_TYPE_INT32},
		{typeName: "INTEGER", want: descriptorpb.FieldDescriptorProto_TYPE_INT32},
		{typeName: "SHORT", want: descriptorpb.FieldDescriptorProto_TYPE_INT32},
		{typeName: "SMALLINT", want: descriptorpb.FieldDescriptorProto_TYPE_INT32},
		{typeName: "BYTE", want: descriptorpb.FieldDescriptorProto_TYPE_INT32},
		{typeName: "TINYINT", want: descriptorpb.FieldDescriptorProto_TYPE_INT32},
		{typeName: "DOUBLE", want: descriptorpb.FieldDescriptorProto_TYPE_DOUBLE},
		{typeName: "FLOAT", want: descriptorpb.FieldDescriptorProto_TYPE_FLOAT},
		{typeName: "BOOLEAN", want: descriptorpb.FieldDescriptorProto_TYPE_BOOL},
		{typeName: "BOOL", want: descriptorpb.FieldDescriptorProto_TYPE_BOOL},
		{typeName: "BINARY", want: descriptorpb.FieldDescriptorProto_TYPE_BYTES},
		{typeName: "TIMESTAMP", want: descriptorpb.FieldDescriptorProto_TYPE_INT64},
		{typeName: "TIMESTAMP_NTZ", want: descriptorpb.FieldDescriptorProto_TYPE_INT64},
		{typeName: "DATE", want: descriptorpb.FieldDescriptorProto_TYPE_INT32},
	}
	for _, tc := range tests {
		t.Run(tc.typeName, func(t *testing.T) {
			desc, err := DescriptorFromUCColumns(
				[]UcColumn{{Name: "value", TypeName: tc.typeName, Nullable: true}},
				"Message",
			)
			if err != nil {
				t.Fatalf("DescriptorFromUCColumns() error = %v", err)
			}
			if got := desc.GetField()[0].GetType(); got != tc.want {
				t.Fatalf("type = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDescriptorFromUCColumns_PreservesPositionGaps(t *testing.T) {
	desc, err := DescriptorFromUCColumns([]UcColumn{
		{Name: "first", TypeName: "STRING", Nullable: true, Position: 0},
		{Name: "fifth", TypeName: "STRING", Nullable: true, Position: 4},
	}, "Message")
	if err != nil {
		t.Fatalf("DescriptorFromUCColumns() error = %v", err)
	}
	if got := fieldByName(t, desc, "fifth").GetNumber(); got != 5 {
		t.Fatalf("fifth field number = %d, want 5", got)
	}
}

func TestDescriptorFromUCColumns_MapAndNestedVariant(t *testing.T) {
	desc, err := DescriptorFromUCColumns([]UcColumn{
		{
			Name:     "payload",
			TypeName: "STRUCT",
			TypeJSON: `{"type":"struct","fields":[` +
				`{"name":"variant","type":"variant","nullable":true},` +
				`{"name":"attributes","type":{"type":"map","keyType":"string","valueType":"integer"},"nullable":true}` +
				`]}`,
			Nullable: true,
			Position: 0,
		},
	}, "Message")
	if err != nil {
		t.Fatalf("DescriptorFromUCColumns() error = %v", err)
	}
	payloadName := fieldByName(t, desc, "payload").GetTypeName()
	var payload *descriptorpb.DescriptorProto
	for _, nested := range desc.GetNestedType() {
		if nested.GetName() == payloadName {
			payload = nested
		}
	}
	if payload == nil {
		t.Fatalf("nested payload %q not found", payloadName)
	}
	if got := fieldByName(t, payload, "variant").GetType(); got != descriptorpb.FieldDescriptorProto_TYPE_STRING {
		t.Fatalf("variant type = %v, want string", got)
	}
	entryName := fieldByName(t, payload, "attributes").GetTypeName()
	var entry *descriptorpb.DescriptorProto
	for _, nested := range payload.GetNestedType() {
		if nested.GetName() == entryName {
			entry = nested
		}
	}
	if entry == nil || !entry.GetOptions().GetMapEntry() {
		t.Fatalf("map entry %q missing or not marked map_entry", entryName)
	}
	if _, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:        proto.String("schema_test.proto"),
		Syntax:      proto.String("proto2"),
		MessageType: []*descriptorpb.DescriptorProto{desc},
	}, nil); err != nil {
		t.Fatalf("generated descriptor is invalid: %v", err)
	}
}

func TestDescriptorFromUCColumns_RejectsMapKeyAndDeepNesting(t *testing.T) {
	_, err := DescriptorFromUCColumns([]UcColumn{{
		Name:     "bad",
		TypeName: "MAP",
		TypeJSON: `{"type":"map","keyType":"double","valueType":"integer"}`,
	}}, "Message")
	if err == nil {
		t.Fatal("expected unsupported map key error")
	}

	deep := `"integer"`
	for range maxNestingDepth + 2 {
		deep = `{"type":"array","elementType":` + deep + `}`
	}
	_, err = DescriptorFromUCColumns([]UcColumn{{
		Name:     "deep",
		TypeName: "ARRAY",
		TypeJSON: deep,
	}}, "Message")
	if err == nil || !strings.Contains(err.Error(), "maximum depth") {
		t.Fatalf("deep nesting error = %v, want maximum depth", err)
	}
}

func TestDescriptorFromUCSchema_DerivesName(t *testing.T) {
	desc, err := DescriptorFromUCSchema(&UcTableSchema{
		Name:       "events",
		SchemaName: "analytics",
		Columns: []UcColumn{
			{Name: "id", TypeName: "LONG", Nullable: false, Position: 0},
		},
	})
	if err != nil {
		t.Fatalf("DescriptorFromUCSchema() error = %v", err)
	}
	if got := desc.GetName(); got != "AnalyticsEvents" {
		t.Fatalf("message name = %q, want AnalyticsEvents", got)
	}
}
