package schema

import (
	"testing"

	"google.golang.org/protobuf/types/descriptorpb"
)

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
