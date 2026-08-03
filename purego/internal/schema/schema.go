// Package schema converts Unity Catalog table schemas into protobuf descriptors.
package schema

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/protobuf/types/descriptorpb"
)

// UcColumn mirrors the UC REST API column shape.
type UcColumn struct {
	Name     string `json:"name"`
	TypeName string `json:"type_name"`
	TypeText string `json:"type_text"`
	TypeJSON string `json:"type_json"`
	Nullable bool   `json:"nullable"`
	Position int32  `json:"position"`
}

// UcTableSchema mirrors the UC REST API table schema shape.
type UcTableSchema struct {
	Name        string     `json:"name"`
	CatalogName string     `json:"catalog_name"`
	SchemaName  string     `json:"schema_name"`
	Columns     []UcColumn `json:"columns"`
}

// SchemaError reports a schema-conversion failure.
type SchemaError struct {
	msg string
}

func (e *SchemaError) Error() string { return e.msg }

func schemaErrf(format string, args ...any) error {
	return &SchemaError{msg: fmt.Sprintf(format, args...)}
}

// DescriptorFromUCSchema builds a DescriptorProto from a full UC table schema.
func DescriptorFromUCSchema(table *UcTableSchema) (*descriptorpb.DescriptorProto, error) {
	if table == nil {
		return nil, schemaErrf("schema is nil")
	}
	msgName := sanitizeMessageName(table.SchemaName + "_" + table.Name)
	return DescriptorFromUCColumns(table.Columns, msgName)
}

// DescriptorFromUCColumns builds a DescriptorProto from UC columns.
func DescriptorFromUCColumns(columns []UcColumn, messageName string) (*descriptorpb.DescriptorProto, error) {
	if strings.TrimSpace(messageName) == "" {
		return nil, schemaErrf("message name is required")
	}
	sorted := make([]UcColumn, 0, len(columns))
	for _, c := range columns {
		if c.Position >= 0 {
			sorted = append(sorted, c)
		}
	}
	slices.SortFunc(sorted, func(a, b UcColumn) int {
		switch {
		case a.Position < b.Position:
			return -1
		case a.Position > b.Position:
			return 1
		default:
			return 0
		}
	})

	collector := &messageCollector{used: map[string]struct{}{}}
	fields := make([]*descriptorpb.FieldDescriptorProto, 0, len(sorted))
	for _, c := range sorted {
		if err := validateFieldName(c.Name); err != nil {
			return nil, err
		}
		fieldType, typeName, repeated, err := columnToProto(c, collector)
		if err != nil {
			return nil, err
		}
		fields = append(fields, fieldDescriptor(c.Name, c.Position+1, fieldType, typeName, c.Nullable, repeated))
	}

	return &descriptorpb.DescriptorProto{
		Name:       stringp(messageName),
		Field:      fields,
		NestedType: collector.nested,
	}, nil
}

func fieldDescriptor(
	name string,
	number int32,
	fieldType descriptorpb.FieldDescriptorProto_Type,
	typeName *string,
	nullable bool,
	repeated bool,
) *descriptorpb.FieldDescriptorProto {
	label := descriptorpb.FieldDescriptorProto_LABEL_REQUIRED
	if repeated {
		label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	} else if nullable {
		label = descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	}
	out := &descriptorpb.FieldDescriptorProto{
		Name:     stringp(name),
		Number:   int32p(number),
		Label:    label.Enum(),
		Type:     fieldType.Enum(),
		JsonName: stringp(name),
	}
	if typeName != nil {
		out.TypeName = typeName
	}
	return out
}

func columnToProto(c UcColumn, collector *messageCollector) (descriptorpb.FieldDescriptorProto_Type, *string, bool, error) {
	switch c.TypeName {
	case "STRUCT", "ARRAY", "MAP":
		if strings.TrimSpace(c.TypeJSON) == "" {
			return 0, nil, false, schemaErrf("missing type_json for complex column %q", c.Name)
		}
		complexType, err := parseTypeJSON(c.TypeJSON)
		if err != nil {
			return 0, nil, false, schemaErrf("invalid type_json for column %q: %v", c.Name, err)
		}
		repeated := complexType.kind == complexKindArray || complexType.kind == complexKindMap
		typ, typeName, err := mapComplexTypeToProtobuf(complexType, c.Name, collector)
		return typ, typeName, repeated, err
	default:
		prim, err := parsePrimitiveTopLevel(c.TypeName)
		if err != nil {
			return 0, nil, false, err
		}
		return primitiveProtoType(prim), nil, false, nil
	}
}

type primitiveType int

const (
	primUnknown primitiveType = iota
	primString
	primVariant
	primLong
	primInt
	primShort
	primByte
	primDouble
	primFloat
	primBool
	primBinary
	primTimestamp
	primTimestampNtz
	primDate
	primDecimal
)

func parsePrimitiveTopLevel(s string) (primitiveType, error) {
	switch s {
	case "STRING":
		return primString, nil
	case "VARIANT":
		return primVariant, nil
	case "LONG", "BIGINT":
		return primLong, nil
	case "INT", "INTEGER":
		return primInt, nil
	case "SHORT", "SMALLINT":
		return primShort, nil
	case "BYTE", "TINYINT":
		return primByte, nil
	case "DOUBLE":
		return primDouble, nil
	case "FLOAT":
		return primFloat, nil
	case "BOOLEAN", "BOOL":
		return primBool, nil
	case "BINARY":
		return primBinary, nil
	case "TIMESTAMP":
		return primTimestamp, nil
	case "TIMESTAMP_NTZ":
		return primTimestampNtz, nil
	case "DATE":
		return primDate, nil
	case "DECIMAL":
		return primDecimal, nil
	default:
		return primUnknown, schemaErrf("unsupported Databricks type %q", s)
	}
}

func parsePrimitiveNested(s string) (primitiveType, error) {
	switch {
	case s == "string":
		return primString, nil
	case s == "variant":
		return primVariant, nil
	case s == "long":
		return primLong, nil
	case s == "integer":
		return primInt, nil
	case s == "short":
		return primShort, nil
	case s == "byte":
		return primByte, nil
	case s == "double":
		return primDouble, nil
	case s == "float":
		return primFloat, nil
	case s == "boolean":
		return primBool, nil
	case s == "binary":
		return primBinary, nil
	case s == "timestamp":
		return primTimestamp, nil
	case s == "timestamp_ntz":
		return primTimestampNtz, nil
	case s == "date":
		return primDate, nil
	case strings.HasPrefix(s, "decimal"):
		return primDecimal, nil
	default:
		return primUnknown, schemaErrf("unsupported nested type %q", s)
	}
}

func primitiveProtoType(p primitiveType) descriptorpb.FieldDescriptorProto_Type {
	switch p {
	case primString, primVariant, primDecimal:
		return descriptorpb.FieldDescriptorProto_TYPE_STRING
	case primLong, primTimestamp, primTimestampNtz:
		return descriptorpb.FieldDescriptorProto_TYPE_INT64
	case primInt, primShort, primByte, primDate:
		return descriptorpb.FieldDescriptorProto_TYPE_INT32
	case primDouble:
		return descriptorpb.FieldDescriptorProto_TYPE_DOUBLE
	case primFloat:
		return descriptorpb.FieldDescriptorProto_TYPE_FLOAT
	case primBool:
		return descriptorpb.FieldDescriptorProto_TYPE_BOOL
	case primBinary:
		return descriptorpb.FieldDescriptorProto_TYPE_BYTES
	default:
		return descriptorpb.FieldDescriptorProto_TYPE_STRING
	}
}

func validMapKey(p primitiveType) bool {
	return p != primDouble && p != primFloat && p != primBinary
}

type complexKind int

const (
	complexKindPrimitive complexKind = iota
	complexKindStruct
	complexKindArray
	complexKindMap
)

type complexType struct {
	kind   complexKind
	prim   primitiveType
	fields []structField
	elem   *complexType
	key    *complexType
	value  *complexType
}

type structField struct {
	name     string
	nullable bool
	typ      *complexType
}

type typeRef struct {
	prim    *string
	complex *complexTypeJSON
}

func (r *typeRef) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		r.prim = &s
		return nil
	}
	var c complexTypeJSON
	if err := json.Unmarshal(data, &c); err != nil {
		return err
	}
	r.complex = &c
	return nil
}

type complexTypeJSON struct {
	Type        string            `json:"type"`
	Fields      []structFieldJSON `json:"fields"`
	ElementType *typeRef          `json:"elementType"`
	KeyType     *typeRef          `json:"keyType"`
	ValueType   *typeRef          `json:"valueType"`
}

type structFieldJSON struct {
	Name     string  `json:"name"`
	Type     typeRef `json:"type"`
	Nullable *bool   `json:"nullable"`
}

const maxNestingDepth = 100

func parseTypeJSON(raw string) (*complexType, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" {
		return nil, schemaErrf("empty type_json")
	}

	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return nil, err
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, schemaErrf("type_json must be object")
	}
	if _, hasName := m["name"]; hasName {
		if inner, ok := m["type"]; ok {
			v = inner
		}
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var top typeRef
	if err := json.Unmarshal(b, &top); err != nil {
		return nil, err
	}
	return typeRefToComplex(&top, 0)
}

func typeRefToComplex(ref *typeRef, depth int) (*complexType, error) {
	if depth > maxNestingDepth {
		return nil, schemaErrf("nesting level exceeds maximum depth of %d", maxNestingDepth)
	}
	if ref == nil {
		return nil, schemaErrf("nil type ref")
	}
	if ref.prim != nil {
		p, err := parsePrimitiveNested(*ref.prim)
		if err != nil {
			return nil, err
		}
		return &complexType{kind: complexKindPrimitive, prim: p}, nil
	}
	if ref.complex == nil {
		return nil, schemaErrf("invalid type ref")
	}
	switch ref.complex.Type {
	case "struct":
		out := &complexType{kind: complexKindStruct, fields: make([]structField, 0, len(ref.complex.Fields))}
		for _, f := range ref.complex.Fields {
			ft, err := typeRefToComplex(&f.Type, depth+1)
			if err != nil {
				return nil, err
			}
			nullable := true
			if f.Nullable != nil {
				nullable = *f.Nullable
			}
			out.fields = append(out.fields, structField{name: f.Name, nullable: nullable, typ: ft})
		}
		return out, nil
	case "array":
		elem, err := typeRefToComplex(ref.complex.ElementType, depth+1)
		if err != nil {
			return nil, err
		}
		return &complexType{kind: complexKindArray, elem: elem}, nil
	case "map":
		key, err := typeRefToComplex(ref.complex.KeyType, depth+1)
		if err != nil {
			return nil, err
		}
		value, err := typeRefToComplex(ref.complex.ValueType, depth+1)
		if err != nil {
			return nil, err
		}
		return &complexType{kind: complexKindMap, key: key, value: value}, nil
	default:
		return nil, schemaErrf("unsupported complex type %q", ref.complex.Type)
	}
}

type messageCollector struct {
	nested []*descriptorpb.DescriptorProto
	used   map[string]struct{}
}

func (c *messageCollector) uniqueName(base string) string {
	if _, ok := c.used[base]; !ok {
		c.used[base] = struct{}{}
		return base
	}
	for i := 2; ; i++ {
		candidate := fmt.Sprintf("%s%d", base, i)
		if _, ok := c.used[candidate]; !ok {
			c.used[candidate] = struct{}{}
			return candidate
		}
	}
}

func mapComplexTypeToProtobuf(ct *complexType, path string, collector *messageCollector) (descriptorpb.FieldDescriptorProto_Type, *string, error) {
	switch ct.kind {
	case complexKindPrimitive:
		return primitiveProtoType(ct.prim), nil, nil
	case complexKindStruct:
		name := collector.uniqueName(sanitizeMessageName(path))
		msg, err := generateStructMessage(name, ct.fields)
		if err != nil {
			return 0, nil, err
		}
		collector.nested = append(collector.nested, msg)
		return descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, &name, nil
	case complexKindArray:
		switch ct.elem.kind {
		case complexKindPrimitive:
			return primitiveProtoType(ct.elem.prim), nil, nil
		case complexKindStruct:
			return mapComplexTypeToProtobuf(ct.elem, sanitizeMessageName(path)+"_element", collector)
		case complexKindArray:
			return 0, nil, schemaErrf("nested arrays not supported for field %q", path)
		case complexKindMap:
			return 0, nil, schemaErrf("arrays of maps not supported for field %q", path)
		}
	case complexKindMap:
		if ct.key == nil || ct.key.kind != complexKindPrimitive || !validMapKey(ct.key.prim) {
			return 0, nil, schemaErrf("unsupported map key type for field %q (must be integral, bool, or string)", path)
		}
		base := sanitizeMessageName(path)
		var valueType descriptorpb.FieldDescriptorProto_Type
		var valueTypeName *string
		switch ct.value.kind {
		case complexKindPrimitive:
			valueType = primitiveProtoType(ct.value.prim)
		case complexKindStruct:
			valueName := collector.uniqueName(base + "Value")
			msg, err := generateStructMessage(valueName, ct.value.fields)
			if err != nil {
				return 0, nil, err
			}
			collector.nested = append(collector.nested, msg)
			valueType = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
			valueTypeName = &valueName
		default:
			return 0, nil, schemaErrf("maps with complex value types not supported for field %q", path)
		}
		entryName := collector.uniqueName(base + "Entry")
		entry := generateMapEntry(entryName, primitiveProtoType(ct.key.prim), valueType, valueTypeName)
		collector.nested = append(collector.nested, entry)
		return descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, &entryName, nil
	}
	return 0, nil, schemaErrf("unsupported complex shape for %q", path)
}

func generateStructMessage(messageName string, fields []structField) (*descriptorpb.DescriptorProto, error) {
	local := &messageCollector{used: map[string]struct{}{}}
	out := make([]*descriptorpb.FieldDescriptorProto, 0, len(fields))
	for i, f := range fields {
		if err := validateFieldName(f.name); err != nil {
			return nil, err
		}
		path := messageName + "_" + f.name
		typ, typeName, err := mapComplexTypeToProtobuf(f.typ, path, local)
		if err != nil {
			return nil, err
		}
		repeated := f.typ.kind == complexKindArray || f.typ.kind == complexKindMap
		out = append(out, fieldDescriptor(f.name, int32(i+1), typ, typeName, f.nullable, repeated))
	}
	return &descriptorpb.DescriptorProto{
		Name:       stringp(messageName),
		Field:      out,
		NestedType: local.nested,
	}, nil
}

func generateMapEntry(
	name string,
	keyType descriptorpb.FieldDescriptorProto_Type,
	valueType descriptorpb.FieldDescriptorProto_Type,
	valueTypeName *string,
) *descriptorpb.DescriptorProto {
	keyField := &descriptorpb.FieldDescriptorProto{
		Name:     stringp("key"),
		Number:   int32p(1),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		Type:     keyType.Enum(),
		JsonName: stringp("key"),
	}
	valueField := &descriptorpb.FieldDescriptorProto{
		Name:     stringp("value"),
		Number:   int32p(2),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		Type:     valueType.Enum(),
		TypeName: valueTypeName,
		JsonName: stringp("value"),
	}
	return &descriptorpb.DescriptorProto{
		Name:  stringp(name),
		Field: []*descriptorpb.FieldDescriptorProto{keyField, valueField},
		Options: &descriptorpb.MessageOptions{
			MapEntry: boolp(true),
		},
	}
}

func validateFieldName(name string) error {
	if name == "" {
		return schemaErrf("invalid field name %q: empty", name)
	}
	if name[0] >= '0' && name[0] <= '9' {
		return schemaErrf("invalid field name %q: cannot start with a digit", name)
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			continue
		}
		return schemaErrf("invalid field name %q: only alphanumeric and '_' are allowed", name)
	}
	return nil
}

func sanitizeMessageName(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	capNext := true
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			if capNext {
				if r >= 'a' && r <= 'z' {
					r = r - 'a' + 'A'
				}
				capNext = false
			}
			b.WriteRune(r)
			continue
		}
		capNext = true
	}
	out := b.String()
	if out == "" || !(out[0] >= 'A' && out[0] <= 'Z' || out[0] >= 'a' && out[0] <= 'z') {
		out = "M" + out
	}
	return out
}

func stringp(s string) *string { return &s }
func int32p(v int32) *int32    { return &v }
func boolp(v bool) *bool       { return &v }
