package zerobus

import (
	"encoding/json"
	"fmt"
	"testing"

	"google.golang.org/protobuf/types/descriptorpb"
)

// col creates a simple UcColumn for testing.
func col(name, typeName string, nullable bool, position int32) UcColumn {
	return UcColumn{
		Name:     name,
		TypeName: typeName,
		TypeText: typeName,
		TypeJson: "",
		Nullable: nullable,
		Position: position,
	}
}

// complexCol creates a UcColumn with type_json for complex types.
func complexCol(name, typeName, typeJson string, position int32) UcColumn {
	return UcColumn{
		Name:     name,
		TypeName: typeName,
		TypeText: "",
		TypeJson: typeJson,
		Nullable: true,
		Position: position,
	}
}

// fieldByName finds a field in desc.Field by name, fatals if not found.
func fieldByName(t *testing.T, desc *descriptorpb.DescriptorProto, name string) *descriptorpb.FieldDescriptorProto {
	t.Helper()
	for _, f := range desc.GetField() {
		if f.GetName() == name {
			return f
		}
	}
	t.Fatalf("field '%s' not found in message '%s'", name, desc.GetName())
	return nil
}

func TestScalarsRoundTrip(t *testing.T) {
	cols := []UcColumn{
		col("id", "BIGINT", false, 0),
		col("name", "STRING", true, 1),
		col("score", "DOUBLE", true, 2),
		col("created_at", "TIMESTAMP", true, 3),
		col("d", "DATE", false, 4),
		col("data", "BINARY", false, 5),
	}
	desc, err := DescriptorFromUcColumns(cols, "m")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if desc.GetName() != "m" {
		t.Errorf("expected name 'm', got '%s'", desc.GetName())
	}

	id := fieldByName(t, desc, "id")
	if id.GetType() != descriptorpb.FieldDescriptorProto_TYPE_INT64 {
		t.Errorf("id: expected TYPE_INT64, got %v", id.GetType())
	}
	if id.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_REQUIRED {
		t.Errorf("id: expected LABEL_REQUIRED, got %v", id.GetLabel())
	}

	name := fieldByName(t, desc, "name")
	if name.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL {
		t.Errorf("name: expected LABEL_OPTIONAL, got %v", name.GetLabel())
	}

	score := fieldByName(t, desc, "score")
	if score.GetType() != descriptorpb.FieldDescriptorProto_TYPE_DOUBLE {
		t.Errorf("score: expected TYPE_DOUBLE, got %v", score.GetType())
	}

	createdAt := fieldByName(t, desc, "created_at")
	if createdAt.GetType() != descriptorpb.FieldDescriptorProto_TYPE_INT64 {
		t.Errorf("created_at: expected TYPE_INT64, got %v", createdAt.GetType())
	}

	d := fieldByName(t, desc, "d")
	if d.GetType() != descriptorpb.FieldDescriptorProto_TYPE_INT32 {
		t.Errorf("d: expected TYPE_INT32, got %v", d.GetType())
	}

	data := fieldByName(t, desc, "data")
	if data.GetType() != descriptorpb.FieldDescriptorProto_TYPE_BYTES {
		t.Errorf("data: expected TYPE_BYTES, got %v", data.GetType())
	}

	if id.GetNumber() != 1 {
		t.Errorf("id: expected number 1, got %d", id.GetNumber())
	}
	if data.GetNumber() != 6 {
		t.Errorf("data: expected number 6, got %d", data.GetNumber())
	}
}

func TestFieldNumbersMirrorUcPosition(t *testing.T) {
	cols := []UcColumn{
		col("a", "STRING", true, 0),
		col("b", "STRING", true, 4),
		col("c", "STRING", true, 8),
	}
	desc, err := DescriptorFromUcColumns(cols, "m")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fieldByName(t, desc, "a").GetNumber() != 1 {
		t.Errorf("a: expected number 1, got %d", fieldByName(t, desc, "a").GetNumber())
	}
	if fieldByName(t, desc, "b").GetNumber() != 5 {
		t.Errorf("b: expected number 5, got %d", fieldByName(t, desc, "b").GetNumber())
	}
	if fieldByName(t, desc, "c").GetNumber() != 9 {
		t.Errorf("c: expected number 9, got %d", fieldByName(t, desc, "c").GetNumber())
	}
}

func TestStructBecomesNestedMessage(t *testing.T) {
	typeJson := `{"type":"struct","fields":[{"name":"street","type":"string","nullable":true,"metadata":{}},{"name":"zip","type":"integer","nullable":false,"metadata":{}}]}`
	cols := []UcColumn{complexCol("address", "STRUCT", typeJson, 0)}
	desc, err := DescriptorFromUcColumns(cols, "m")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	f := fieldByName(t, desc, "address")
	if f.GetType() != descriptorpb.FieldDescriptorProto_TYPE_MESSAGE {
		t.Errorf("address: expected TYPE_MESSAGE, got %v", f.GetType())
	}
	if f.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL {
		t.Errorf("address: expected LABEL_OPTIONAL, got %v", f.GetLabel())
	}

	typeName := f.GetTypeName()
	if typeName == "" {
		t.Fatal("address: expected non-empty TypeName")
	}

	// Find the nested type.
	var nested *descriptorpb.DescriptorProto
	for _, nt := range desc.GetNestedType() {
		if nt.GetName() == typeName {
			nested = nt
			break
		}
	}
	if nested == nil {
		t.Fatalf("nested struct message '%s' not found", typeName)
	}

	street := fieldByName(t, nested, "street")
	if street.GetType() != descriptorpb.FieldDescriptorProto_TYPE_STRING {
		t.Errorf("street: expected TYPE_STRING, got %v", street.GetType())
	}
	zip := fieldByName(t, nested, "zip")
	if zip.GetType() != descriptorpb.FieldDescriptorProto_TYPE_INT32 {
		t.Errorf("zip: expected TYPE_INT32, got %v", zip.GetType())
	}
	if zip.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_REQUIRED {
		t.Errorf("zip: expected LABEL_REQUIRED, got %v", zip.GetLabel())
	}
}

func TestArrayOfPrimitiveIsRepeatedScalar(t *testing.T) {
	typeJson := `{"type":"array","elementType":"long","containsNull":true}`
	cols := []UcColumn{complexCol("tags", "ARRAY", typeJson, 0)}
	desc, err := DescriptorFromUcColumns(cols, "m")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	f := fieldByName(t, desc, "tags")
	if f.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		t.Errorf("tags: expected LABEL_REPEATED, got %v", f.GetLabel())
	}
	if f.GetType() != descriptorpb.FieldDescriptorProto_TYPE_INT64 {
		t.Errorf("tags: expected TYPE_INT64, got %v", f.GetType())
	}
	if f.TypeName != nil {
		t.Errorf("tags: expected nil TypeName, got '%s'", f.GetTypeName())
	}
}

func TestArrayOfStructEmitsNestedMessage(t *testing.T) {
	typeJson := `{"type":"array","elementType":{"type":"struct","fields":[{"name":"k","type":"string","nullable":true,"metadata":{}}]},"containsNull":true}`
	cols := []UcColumn{complexCol("items", "ARRAY", typeJson, 0)}
	desc, err := DescriptorFromUcColumns(cols, "m")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	f := fieldByName(t, desc, "items")
	if f.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		t.Errorf("items: expected LABEL_REPEATED, got %v", f.GetLabel())
	}
	if f.GetType() != descriptorpb.FieldDescriptorProto_TYPE_MESSAGE {
		t.Errorf("items: expected TYPE_MESSAGE, got %v", f.GetType())
	}

	typeName := f.GetTypeName()
	found := false
	for _, nt := range desc.GetNestedType() {
		if nt.GetName() == typeName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("nested type '%s' not found", typeName)
	}
}

func TestMapOfPrimitiveGeneratesEntryMessage(t *testing.T) {
	typeJson := `{"type":"map","keyType":"string","valueType":"integer","valueContainsNull":true}`
	cols := []UcColumn{complexCol("props", "MAP", typeJson, 0)}
	desc, err := DescriptorFromUcColumns(cols, "m")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	f := fieldByName(t, desc, "props")
	if f.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		t.Errorf("props: expected LABEL_REPEATED, got %v", f.GetLabel())
	}
	if f.GetType() != descriptorpb.FieldDescriptorProto_TYPE_MESSAGE {
		t.Errorf("props: expected TYPE_MESSAGE, got %v", f.GetType())
	}

	entryName := f.GetTypeName()
	var entry *descriptorpb.DescriptorProto
	for _, nt := range desc.GetNestedType() {
		if nt.GetName() == entryName {
			entry = nt
			break
		}
	}
	if entry == nil {
		t.Fatalf("map entry message '%s' not found", entryName)
	}

	if entry.GetOptions() == nil || !entry.GetOptions().GetMapEntry() {
		t.Error("expected map_entry=true on entry message")
	}

	key := fieldByName(t, entry, "key")
	if key.GetType() != descriptorpb.FieldDescriptorProto_TYPE_STRING {
		t.Errorf("key: expected TYPE_STRING, got %v", key.GetType())
	}
	value := fieldByName(t, entry, "value")
	if value.GetType() != descriptorpb.FieldDescriptorProto_TYPE_INT32 {
		t.Errorf("value: expected TYPE_INT32, got %v", value.GetType())
	}
}

func TestMapWithStructValueEmitsValueAndEntry(t *testing.T) {
	typeJson := `{"type":"map","keyType":"string","valueType":{"type":"struct","fields":[{"name":"v","type":"long","nullable":true,"metadata":{}}]},"valueContainsNull":true}`
	cols := []UcColumn{complexCol("lookup", "MAP", typeJson, 0)}
	desc, err := DescriptorFromUcColumns(cols, "m")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	f := fieldByName(t, desc, "lookup")
	entryName := f.GetTypeName()
	var entry *descriptorpb.DescriptorProto
	for _, nt := range desc.GetNestedType() {
		if nt.GetName() == entryName {
			entry = nt
			break
		}
	}
	if entry == nil {
		t.Fatalf("entry message '%s' not found", entryName)
	}
	if entry.GetOptions() == nil || !entry.GetOptions().GetMapEntry() {
		t.Error("expected map_entry=true on entry message")
	}

	valueField := fieldByName(t, entry, "value")
	valueTypeName := valueField.GetTypeName()
	if valueTypeName == "" {
		t.Fatal("expected non-empty TypeName for value field")
	}

	// The referenced value message also exists as a nested type.
	found := false
	for _, nt := range desc.GetNestedType() {
		if nt.GetName() == valueTypeName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("value message '%s' not found in nested types", valueTypeName)
	}
}

func TestRejectsUnsupportedMapKey(t *testing.T) {
	typeJson := `{"type":"map","keyType":"double","valueType":"integer","valueContainsNull":true}`
	cols := []UcColumn{complexCol("bad", "MAP", typeJson, 0)}
	_, err := DescriptorFromUcColumns(cols, "m")
	if err == nil {
		t.Fatal("expected error for unsupported map key type")
	}
	se, ok := err.(*SchemaError)
	if !ok {
		t.Fatalf("expected *SchemaError, got %T", err)
	}
	if se.Kind != "Invalid" {
		t.Errorf("expected Kind='Invalid', got '%s'", se.Kind)
	}
}

func TestRejectsExcessivelyDeepNesting(t *testing.T) {
	// Build a chain of maxNestingDepth+2 nested arrays.
	typeJson := `"integer"`
	for range maxNestingDepth + 2 {
		typeJson = fmt.Sprintf(`{"type":"array","elementType":%s,"containsNull":true}`, typeJson)
	}
	cols := []UcColumn{complexCol("deep", "ARRAY", typeJson, 0)}
	_, err := DescriptorFromUcColumns(cols, "m")
	if err == nil {
		t.Fatal("expected error for excessive nesting")
	}
	se, ok := err.(*SchemaError)
	if !ok {
		t.Fatalf("expected *SchemaError, got %T", err)
	}
	if se.Kind != "InvalidTypeJson" {
		t.Errorf("expected Kind='InvalidTypeJson', got '%s'", se.Kind)
	}
}

func TestRejectsNestedArrays(t *testing.T) {
	typeJson := `{"type":"array","elementType":{"type":"array","elementType":"integer","containsNull":true},"containsNull":true}`
	cols := []UcColumn{complexCol("nested", "ARRAY", typeJson, 0)}
	_, err := DescriptorFromUcColumns(cols, "m")
	if err == nil {
		t.Fatal("expected error for nested arrays")
	}
	se, ok := err.(*SchemaError)
	if !ok {
		t.Fatalf("expected *SchemaError, got %T", err)
	}
	if se.Kind != "Invalid" {
		t.Errorf("expected Kind='Invalid', got '%s'", se.Kind)
	}
}

func TestRejectsInvalidFieldName(t *testing.T) {
	cols := []UcColumn{col("1bad", "STRING", true, 0)}
	_, err := DescriptorFromUcColumns(cols, "m")
	if err == nil {
		t.Fatal("expected error for invalid field name")
	}
	se, ok := err.(*SchemaError)
	if !ok {
		t.Fatalf("expected *SchemaError, got %T", err)
	}
	if se.Kind != "InvalidFieldName" {
		t.Errorf("expected Kind='InvalidFieldName', got '%s'", se.Kind)
	}
}

func TestRejectsReservedProtoKeyword(t *testing.T) {
	cols := []UcColumn{col("message", "STRING", true, 0)}
	_, err := DescriptorFromUcColumns(cols, "m")
	if err == nil {
		t.Fatal("expected error for reserved keyword")
	}
	se, ok := err.(*SchemaError)
	if !ok {
		t.Fatalf("expected *SchemaError, got %T", err)
	}
	if se.Kind != "InvalidFieldName" {
		t.Errorf("expected Kind='InvalidFieldName', got '%s'", se.Kind)
	}
	if se.Field != "message" {
		t.Errorf("expected Field='message', got '%s'", se.Field)
	}
}

func TestComplexColumnRequiresTypeJson(t *testing.T) {
	cols := []UcColumn{col("x", "STRUCT", true, 0)}
	_, err := DescriptorFromUcColumns(cols, "m")
	if err == nil {
		t.Fatal("expected error for missing type_json")
	}
	se, ok := err.(*SchemaError)
	if !ok {
		t.Fatalf("expected *SchemaError, got %T", err)
	}
	if se.Kind != "MissingTypeJson" {
		t.Errorf("expected Kind='MissingTypeJson', got '%s'", se.Kind)
	}
}

func TestDescriptorFromUcSchemaDrivesName(t *testing.T) {
	schema := &UcTableSchema{
		Name:        "events",
		CatalogName: "main",
		SchemaName:  "analytics",
		Columns:     []UcColumn{col("id", "BIGINT", false, 0)},
	}
	desc, err := DescriptorFromUcSchema(schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if desc.GetName() != "AnalyticsEvents" {
		t.Errorf("expected name 'AnalyticsEvents', got '%s'", desc.GetName())
	}
}

func TestUniqueNameDisambiguatesCollisions(t *testing.T) {
	typeJson := `{"type":"struct","fields":[{"name":"foo","type":{"type":"struct","fields":[{"name":"a","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"Foo","type":{"type":"struct","fields":[{"name":"b","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]}`
	cols := []UcColumn{complexCol("parent", "STRUCT", typeJson, 0)}
	desc, err := DescriptorFromUcColumns(cols, "m")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Find the Parent nested message.
	var parent *descriptorpb.DescriptorProto
	for _, nt := range desc.GetNestedType() {
		if nt.GetName() == "Parent" {
			parent = nt
			break
		}
	}
	if parent == nil {
		t.Fatal("Parent message not found")
	}

	foo := fieldByName(t, parent, "foo")
	fooCap := fieldByName(t, parent, "Foo")
	if foo.GetTypeName() != "ParentFoo" {
		t.Errorf("foo: expected TypeName='ParentFoo', got '%s'", foo.GetTypeName())
	}
	if fooCap.GetTypeName() != "ParentFoo2" {
		t.Errorf("Foo: expected TypeName='ParentFoo2', got '%s'", fooCap.GetTypeName())
	}
}

func TestSanitizeMessageName(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{"foo-bar", "FooBar"},
		{"1abc", "M1abc"},
		{"analytics.events", "AnalyticsEvents"},
		{"café", "Caf"},
		{"中文", "M"},
	}
	for _, c := range cases {
		t.Run(c.input, func(t *testing.T) {
			got := sanitizeMessageName(c.input)
			if got != c.expected {
				t.Errorf("sanitizeMessageName(%q) = %q, want %q", c.input, got, c.expected)
			}
		})
	}
}

func TestUnsupportedTypeErrorHasColumnName(t *testing.T) {
	cols := []UcColumn{
		col("my_interval_col", "INTERVAL", false, 0),
	}
	_, err := DescriptorFromUcColumns(cols, "Test")
	if err == nil {
		t.Fatal("expected error for unsupported type INTERVAL")
	}
	schemaErr, ok := err.(*SchemaError)
	if !ok {
		t.Fatalf("expected *SchemaError, got %T", err)
	}
	if schemaErr.Kind != "UnsupportedType" {
		t.Fatalf("expected Kind UnsupportedType, got %s", schemaErr.Kind)
	}
	if schemaErr.Field != "my_interval_col" {
		t.Errorf("expected Field to be column name 'my_interval_col', got %q", schemaErr.Field)
	}
}

func TestUcColumnUnmarshalJSON(t *testing.T) {
	cases := []struct {
		name     string
		json     string
		nullable bool
	}{
		{"nullable absent defaults to true", `{"name":"x","type_name":"STRING","position":0}`, true},
		{"nullable explicit true", `{"name":"x","type_name":"STRING","nullable":true,"position":0}`, true},
		{"nullable explicit false", `{"name":"x","type_name":"STRING","nullable":false,"position":0}`, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got UcColumn
			if err := json.Unmarshal([]byte(c.json), &got); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Nullable != c.nullable {
				t.Errorf("Nullable = %v, want %v", got.Nullable, c.nullable)
			}
		})
	}
}

func TestAllTypeAliases(t *testing.T) {
	cases := []struct {
		typeName string
		want     descriptorpb.FieldDescriptorProto_Type
	}{
		{"STRING", descriptorpb.FieldDescriptorProto_TYPE_STRING},
		{"INT", descriptorpb.FieldDescriptorProto_TYPE_INT32},
		{"INTEGER", descriptorpb.FieldDescriptorProto_TYPE_INT32},
		{"LONG", descriptorpb.FieldDescriptorProto_TYPE_INT64},
		{"BIGINT", descriptorpb.FieldDescriptorProto_TYPE_INT64},
		{"SHORT", descriptorpb.FieldDescriptorProto_TYPE_INT32},
		{"SMALLINT", descriptorpb.FieldDescriptorProto_TYPE_INT32},
		{"BYTE", descriptorpb.FieldDescriptorProto_TYPE_INT32},
		{"TINYINT", descriptorpb.FieldDescriptorProto_TYPE_INT32},
		{"BOOLEAN", descriptorpb.FieldDescriptorProto_TYPE_BOOL},
		{"BOOL", descriptorpb.FieldDescriptorProto_TYPE_BOOL},
		{"DOUBLE", descriptorpb.FieldDescriptorProto_TYPE_DOUBLE},
		{"FLOAT", descriptorpb.FieldDescriptorProto_TYPE_FLOAT},
		{"TIMESTAMP", descriptorpb.FieldDescriptorProto_TYPE_INT64},
		{"TIMESTAMP_NTZ", descriptorpb.FieldDescriptorProto_TYPE_INT64},
		{"DATE", descriptorpb.FieldDescriptorProto_TYPE_INT32},
		{"BINARY", descriptorpb.FieldDescriptorProto_TYPE_BYTES},
		{"DECIMAL", descriptorpb.FieldDescriptorProto_TYPE_STRING},
		{"VARIANT", descriptorpb.FieldDescriptorProto_TYPE_STRING},
	}
	for _, c := range cases {
		t.Run(c.typeName, func(t *testing.T) {
			cols := []UcColumn{col("x", c.typeName, true, 0)}
			desc, err := DescriptorFromUcColumns(cols, "m")
			if err != nil {
				t.Fatalf("unexpected error for type %s: %v", c.typeName, err)
			}
			f := fieldByName(t, desc, "x")
			if f.GetType() != c.want {
				t.Errorf("type %s: expected %v, got %v", c.typeName, c.want, f.GetType())
			}
		})
	}
}

func TestTypeJsonWrapperFormat(t *testing.T) {
	// UC sometimes wraps type_json in {"name":"colname","type":<inner>}
	typeJson := `{"name":"address","type":{"type":"struct","fields":[{"name":"city","type":"string","nullable":true,"metadata":{}}]}}`
	cols := []UcColumn{complexCol("address", "STRUCT", typeJson, 0)}
	desc, err := DescriptorFromUcColumns(cols, "m")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	f := fieldByName(t, desc, "address")
	if f.GetType() != descriptorpb.FieldDescriptorProto_TYPE_MESSAGE {
		t.Errorf("expected TYPE_MESSAGE, got %v", f.GetType())
	}
}

func TestNegativePositionFiltered(t *testing.T) {
	cols := []UcColumn{
		col("visible", "STRING", true, 0),
		col("hidden", "STRING", true, -1),
	}
	desc, err := DescriptorFromUcColumns(cols, "m")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(desc.GetField()) != 1 {
		t.Errorf("expected 1 field, got %d", len(desc.GetField()))
	}
	if desc.GetField()[0].GetName() != "visible" {
		t.Errorf("expected field 'visible', got '%s'", desc.GetField()[0].GetName())
	}
}

func TestDecimalPrefixInTypeJson(t *testing.T) {
	typeJson := `{"type":"array","elementType":"decimal(10,2)","containsNull":true}`
	cols := []UcColumn{complexCol("amounts", "ARRAY", typeJson, 0)}
	desc, err := DescriptorFromUcColumns(cols, "m")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	f := fieldByName(t, desc, "amounts")
	if f.GetType() != descriptorpb.FieldDescriptorProto_TYPE_STRING {
		t.Errorf("expected TYPE_STRING for decimal, got %v", f.GetType())
	}
	if f.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		t.Errorf("expected LABEL_REPEATED, got %v", f.GetLabel())
	}
}

func TestMapWithArrayValueRejects(t *testing.T) {
	typeJson := `{"type":"map","keyType":"string","valueType":{"type":"array","elementType":"integer","containsNull":true},"valueContainsNull":true}`
	cols := []UcColumn{complexCol("bad", "MAP", typeJson, 0)}
	_, err := DescriptorFromUcColumns(cols, "m")
	if err == nil {
		t.Fatal("expected error for map with array value")
	}
	se, ok := err.(*SchemaError)
	if !ok {
		t.Fatalf("expected *SchemaError, got %T", err)
	}
	if se.Kind != "Invalid" {
		t.Errorf("expected Kind='Invalid', got '%s'", se.Kind)
	}
}

func TestEmptyFieldNameRejects(t *testing.T) {
	cols := []UcColumn{col("", "STRING", true, 0)}
	_, err := DescriptorFromUcColumns(cols, "m")
	if err == nil {
		t.Fatal("expected error for empty field name")
	}
	se, ok := err.(*SchemaError)
	if !ok {
		t.Fatalf("expected *SchemaError, got %T", err)
	}
	if se.Kind != "InvalidFieldName" {
		t.Errorf("expected Kind='InvalidFieldName', got '%s'", se.Kind)
	}
}
