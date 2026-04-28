package zerobus

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// UcColumn is a single column from a Unity Catalog table.
// Field names mirror the Unity Catalog REST API response.
type UcColumn struct {
	Name     string `json:"name"`
	TypeName string `json:"type_name"`
	TypeText string `json:"type_text"`
	TypeJson string `json:"type_json"`
	Nullable bool   // defaults to true when absent from JSON; see UnmarshalJSON.
	Position int32  `json:"position"`
}

// UnmarshalJSON implements custom JSON unmarshalling so that Nullable defaults
// to true when the "nullable" key is absent, matching Unity Catalog API
// semantics where omitted nullable means "assume nullable".
func (c *UcColumn) UnmarshalJSON(data []byte) error {
	type alias struct {
		Name     string `json:"name"`
		TypeName string `json:"type_name"`
		TypeText string `json:"type_text"`
		TypeJson string `json:"type_json"`
		Nullable *bool  `json:"nullable"`
		Position int32  `json:"position"`
	}
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	c.Name = a.Name
	c.TypeName = a.TypeName
	c.TypeText = a.TypeText
	c.TypeJson = a.TypeJson
	c.Position = a.Position
	if a.Nullable == nil {
		c.Nullable = true
	} else {
		c.Nullable = *a.Nullable
	}
	return nil
}

// UcTableSchema is a Unity Catalog table schema, as returned by the REST API.
type UcTableSchema struct {
	Name        string     `json:"name"`
	CatalogName string     `json:"catalog_name"`
	SchemaName  string     `json:"schema_name"`
	Columns     []UcColumn `json:"columns"`
}

// SchemaError is returned when a UC schema cannot be converted to a protobuf descriptor.
type SchemaError struct {
	Kind    string // "InvalidFieldName", "UnsupportedType", "MissingTypeJson", "InvalidTypeJson", "Invalid"
	Field   string // field/column name, if applicable.
	Message string // human-readable detail.
}

func (e *SchemaError) Error() string { return e.Message }

// DescriptorFromUcColumns builds a *descriptorpb.DescriptorProto from UC columns.
// messageName becomes the top-level proto message name.
// Columns with TypeName STRUCT, ARRAY, or MAP require TypeJson to be populated.
func DescriptorFromUcColumns(columns []UcColumn, messageName string) (*descriptorpb.DescriptorProto, error) {
	// Copy, filter negative positions, sort by position.
	sorted := make([]UcColumn, 0, len(columns))
	for _, col := range columns {
		if col.Position >= 0 {
			sorted = append(sorted, col)
		}
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Position < sorted[j].Position
	})

	collector := newMessageCollector()
	fields := make([]*descriptorpb.FieldDescriptorProto, 0, len(sorted))

	for i := range sorted {
		col := &sorted[i]
		if err := validateFieldName(col.Name); err != nil {
			return nil, err
		}

		var (
			fieldType descriptorpb.FieldDescriptorProto_Type
			typeName  string
			repeated  bool
		)

		if isComplex(col.TypeName) {
			if col.TypeJson == "" {
				return nil, &SchemaError{
					Kind:    "MissingTypeJson",
					Field:   col.Name,
					Message: fmt.Sprintf("missing type_json for complex column '%s'", col.Name),
				}
			}
			ct, err := parseTypeJson(col.TypeJson)
			if err != nil {
				return nil, &SchemaError{
					Kind:    "InvalidTypeJson",
					Field:   col.Name,
					Message: fmt.Sprintf("failed to parse type_json for column '%s': %s", col.Name, err),
				}
			}
			switch ct.(type) {
			case arrayType, mapType:
				repeated = true
			}
			ft, tn, err := mapComplexTypeToProtobuf(ct, col.Name, collector)
			if err != nil {
				return nil, err
			}
			fieldType = ft
			typeName = tn
		} else {
			ft, err := mapSimpleDatabricksType(col.TypeName, col.Name)
			if err != nil {
				return nil, err
			}
			fieldType = ft
		}

		number := col.Position + 1
		fields = append(fields, buildFieldDescriptor(col.Name, number, fieldType, typeName, col.Nullable, repeated))
	}

	return &descriptorpb.DescriptorProto{
		Name:       strPtr(messageName),
		Field:      fields,
		NestedType: collector.nested,
	}, nil
}

// DescriptorFromUcSchema builds a *descriptorpb.DescriptorProto from a full UcTableSchema.
// The message name is derived as sanitizeMessageName(schemaName + "_" + tableName).
func DescriptorFromUcSchema(schema *UcTableSchema) (*descriptorpb.DescriptorProto, error) {
	messageName := sanitizeMessageName(schema.SchemaName + "_" + schema.Name)
	return DescriptorFromUcColumns(schema.Columns, messageName)
}

// MessageDescriptorFromUcColumns builds a protoreflect.MessageDescriptor and the
// serialized descriptor bytes from UC columns in a single call.
//
// The returned MessageDescriptor can be used with dynamicpb.NewMessage to construct
// proto messages at runtime without pre-generated code. The returned bytes are ready
// to pass directly to TableProperties.DescriptorProto.
//
// Returns an error if:
//   - Any column has an invalid field name
//   - Any column has an unsupported type
//   - A complex column (STRUCT, ARRAY, MAP) is missing TypeJson
//   - TypeJson cannot be parsed
func MessageDescriptorFromUcColumns(columns []UcColumn, messageName string) (protoreflect.MessageDescriptor, []byte, error) {
	return buildMessageDescriptor(func() (*descriptorpb.DescriptorProto, error) {
		return DescriptorFromUcColumns(columns, messageName)
	})
}

// MessageDescriptorFromUcSchema builds a protoreflect.MessageDescriptor and the
// serialized descriptor bytes from a full UcTableSchema in a single call.
//
// The returned MessageDescriptor can be used with dynamicpb.NewMessage to construct
// proto messages at runtime without pre-generated code. The returned bytes are ready
// to pass directly to TableProperties.DescriptorProto.
//
// Returns an error if the schema cannot be converted (see MessageDescriptorFromUcColumns).
func MessageDescriptorFromUcSchema(schema *UcTableSchema) (protoreflect.MessageDescriptor, []byte, error) {
	return buildMessageDescriptor(func() (*descriptorpb.DescriptorProto, error) {
		return DescriptorFromUcSchema(schema)
	})
}

// buildMessageDescriptor is the shared implementation for MessageDescriptorFrom* functions.
// It marshals the DescriptorProto to wire bytes, then builds a proto3-compatible
// FileDescriptor for reflection. LABEL_REQUIRED fields are relaxed to LABEL_OPTIONAL
// in the reflection copy only — the wire bytes retain the original labels.
func buildMessageDescriptor(build func() (*descriptorpb.DescriptorProto, error)) (protoreflect.MessageDescriptor, []byte, error) {
	msgDescProto, err := build()
	if err != nil {
		return nil, nil, err
	}

	// Marshal before any mutation so the server bytes are unaffected.
	wireBytes, err := proto.Marshal(msgDescProto)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal descriptor: %w", err)
	}

	// proto3 forbids LABEL_REQUIRED. Clone and relax required fields so
	// protodesc.NewFile accepts the descriptor for dynamic message building.
	relaxed := proto.Clone(msgDescProto).(*descriptorpb.DescriptorProto)
	for _, f := range relaxed.Field {
		if f.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REQUIRED {
			f.Label = descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
		}
	}

	fileDescProto := &descriptorpb.FileDescriptorProto{
		Name:        proto.String(msgDescProto.GetName() + ".proto"),
		Syntax:      proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{relaxed},
	}
	fileDesc, err := protodesc.NewFile(fileDescProto, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build file descriptor: %w", err)
	}

	return fileDesc.Messages().Get(0), wireBytes, nil
}

func isComplex(typeName string) bool {
	return typeName == "STRUCT" || typeName == "ARRAY" || typeName == "MAP"
}

func strPtr(s string) *string { return &s }

func int32Ptr(n int32) *int32 { return &n }

func boolPtr(b bool) *bool { return &b }

func buildFieldDescriptor(
	name string,
	number int32,
	fieldType descriptorpb.FieldDescriptorProto_Type,
	typeName string,
	nullable bool,
	isRepeated bool,
) *descriptorpb.FieldDescriptorProto {
	var label descriptorpb.FieldDescriptorProto_Label
	if isRepeated {
		label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	} else if nullable {
		label = descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	} else {
		label = descriptorpb.FieldDescriptorProto_LABEL_REQUIRED
	}

	fd := &descriptorpb.FieldDescriptorProto{
		Name:           strPtr(name),
		Number:         int32Ptr(number),
		Label:          label.Enum(),
		Type:           fieldType.Enum(),
		JsonName:       strPtr(name),
		Proto3Optional: boolPtr(nullable && !isRepeated),
	}
	if typeName != "" {
		fd.TypeName = strPtr(typeName)
	}
	return fd
}

func mapSimpleDatabricksType(typeName, columnName string) (descriptorpb.FieldDescriptorProto_Type, error) {
	switch typeName {
	case "STRING":
		return descriptorpb.FieldDescriptorProto_TYPE_STRING, nil
	case "INT", "INTEGER":
		return descriptorpb.FieldDescriptorProto_TYPE_INT32, nil
	case "LONG", "BIGINT":
		return descriptorpb.FieldDescriptorProto_TYPE_INT64, nil
	case "SHORT", "SMALLINT", "BYTE", "TINYINT":
		return descriptorpb.FieldDescriptorProto_TYPE_INT32, nil
	case "BOOLEAN", "BOOL":
		return descriptorpb.FieldDescriptorProto_TYPE_BOOL, nil
	case "DOUBLE":
		return descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, nil
	case "FLOAT":
		return descriptorpb.FieldDescriptorProto_TYPE_FLOAT, nil
	case "TIMESTAMP", "TIMESTAMP_NTZ":
		return descriptorpb.FieldDescriptorProto_TYPE_INT64, nil
	case "DATE":
		return descriptorpb.FieldDescriptorProto_TYPE_INT32, nil
	case "BINARY":
		return descriptorpb.FieldDescriptorProto_TYPE_BYTES, nil
	case "DECIMAL", "VARIANT":
		return descriptorpb.FieldDescriptorProto_TYPE_STRING, nil
	default:
		return 0, &SchemaError{
			Kind:    "UnsupportedType",
			Field:   columnName,
			Message: fmt.Sprintf("unsupported Databricks type '%s'", typeName),
		}
	}
}

// complexType is the internal representation of parsed type_json.
type complexType interface {
	isComplexType()
}

type primitiveType struct {
	proto descriptorpb.FieldDescriptorProto_Type
}

func (primitiveType) isComplexType() {}

type structField struct {
	name      string
	fieldType complexType
	nullable  bool
}

type structType struct {
	fields []structField
}

func (structType) isComplexType() {}

type arrayType struct {
	element complexType
}

func (arrayType) isComplexType() {}

type mapType struct {
	key   complexType
	value complexType
}

func (mapType) isComplexType() {}

const maxNestingDepth = 100

// parseTypeJson parses the JSON representation of a complex UC type.
func parseTypeJson(typeJson string) (complexType, error) {
	if typeJson == "" || typeJson == "{}" {
		return nil, fmt.Errorf("empty type_json")
	}
	var raw json.RawMessage
	if err := json.Unmarshal([]byte(typeJson), &raw); err != nil {
		return nil, err
	}

	// UC sometimes wraps the top-level value in {"name":"colname","type":<inner>}.
	var wrapper map[string]json.RawMessage
	if err := json.Unmarshal(raw, &wrapper); err == nil {
		if _, hasName := wrapper["name"]; hasName {
			if inner, hasType := wrapper["type"]; hasType {
				raw = inner
			}
		}
	}

	return parseTypeRef(raw, 0)
}

// parseTypeRef parses a type reference which can be a string (primitive) or an object (complex).
func parseTypeRef(raw json.RawMessage, depth int) (complexType, error) {
	if depth > maxNestingDepth {
		return nil, fmt.Errorf("nesting level exceeds maximum depth of %d", maxNestingDepth)
	}

	// Try as a string first (primitive type).
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return parsePrimitiveType(s)
	}

	// Must be a complex type object.
	var obj struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("invalid type reference: %s", err)
	}

	switch obj.Type {
	case "struct":
		return parseStructType(raw, depth)
	case "array":
		return parseArrayType(raw, depth)
	case "map":
		return parseMapType(raw, depth)
	default:
		return nil, fmt.Errorf("unknown complex type '%s'", obj.Type)
	}
}

func parseStructType(raw json.RawMessage, depth int) (complexType, error) {
	var s struct {
		Fields []struct {
			Name     string          `json:"name"`
			Type     json.RawMessage `json:"type"`
			Nullable *bool           `json:"nullable"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	fields := make([]structField, 0, len(s.Fields))
	for _, f := range s.Fields {
		ft, err := parseTypeRef(f.Type, depth+1)
		if err != nil {
			return nil, err
		}
		nullable := true
		if f.Nullable != nil {
			nullable = *f.Nullable
		}
		fields = append(fields, structField{
			name:      f.Name,
			fieldType: ft,
			nullable:  nullable,
		})
	}
	return structType{fields: fields}, nil
}

func parseArrayType(raw json.RawMessage, depth int) (complexType, error) {
	var a struct {
		ElementType json.RawMessage `json:"elementType"`
	}
	if err := json.Unmarshal(raw, &a); err != nil {
		return nil, err
	}
	elem, err := parseTypeRef(a.ElementType, depth+1)
	if err != nil {
		return nil, err
	}
	return arrayType{element: elem}, nil
}

func parseMapType(raw json.RawMessage, depth int) (complexType, error) {
	var m struct {
		KeyType   json.RawMessage `json:"keyType"`
		ValueType json.RawMessage `json:"valueType"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	key, err := parseTypeRef(m.KeyType, depth+1)
	if err != nil {
		return nil, err
	}
	value, err := parseTypeRef(m.ValueType, depth+1)
	if err != nil {
		return nil, err
	}
	return mapType{key: key, value: value}, nil
}

func parsePrimitiveType(s string) (complexType, error) {
	var pt descriptorpb.FieldDescriptorProto_Type
	switch s {
	case "string":
		pt = descriptorpb.FieldDescriptorProto_TYPE_STRING
	case "long":
		pt = descriptorpb.FieldDescriptorProto_TYPE_INT64
	case "integer":
		pt = descriptorpb.FieldDescriptorProto_TYPE_INT32
	case "short", "byte":
		pt = descriptorpb.FieldDescriptorProto_TYPE_INT32
	case "double":
		pt = descriptorpb.FieldDescriptorProto_TYPE_DOUBLE
	case "float":
		pt = descriptorpb.FieldDescriptorProto_TYPE_FLOAT
	case "boolean":
		pt = descriptorpb.FieldDescriptorProto_TYPE_BOOL
	case "binary":
		pt = descriptorpb.FieldDescriptorProto_TYPE_BYTES
	case "timestamp", "timestamp_ntz":
		pt = descriptorpb.FieldDescriptorProto_TYPE_INT64
	case "date":
		pt = descriptorpb.FieldDescriptorProto_TYPE_INT32
	default:
		if strings.HasPrefix(s, "decimal") {
			pt = descriptorpb.FieldDescriptorProto_TYPE_STRING
		} else {
			return nil, fmt.Errorf("unknown primitive type '%s'", s)
		}
	}
	return primitiveType{proto: pt}, nil
}

// messageCollector accumulates nested message definitions and deduplicates names.
type messageCollector struct {
	nested []*descriptorpb.DescriptorProto
	used   map[string]struct{}
}

func newMessageCollector() *messageCollector {
	return &messageCollector{
		used: make(map[string]struct{}),
	}
}

// uniqueName returns base if unused, otherwise base2, base3, etc.
func (mc *messageCollector) uniqueName(base string) string {
	if _, exists := mc.used[base]; !exists {
		mc.used[base] = struct{}{}
		return base
	}
	n := 2
	for {
		candidate := fmt.Sprintf("%s%d", base, n)
		if _, exists := mc.used[candidate]; !exists {
			mc.used[candidate] = struct{}{}
			return candidate
		}
		n++
	}
}

func (mc *messageCollector) push(msg *descriptorpb.DescriptorProto) {
	mc.nested = append(mc.nested, msg)
}

func mapComplexTypeToProtobuf(
	ct complexType,
	path string,
	collector *messageCollector,
) (descriptorpb.FieldDescriptorProto_Type, string, error) {
	switch t := ct.(type) {
	case primitiveType:
		return t.proto, "", nil

	case structType:
		name := collector.uniqueName(sanitizeMessageName(path))
		msg, err := generateStructMessage(name, t)
		if err != nil {
			return 0, "", err
		}
		collector.push(msg)
		return descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, name, nil

	case arrayType:
		switch elem := t.element.(type) {
		case primitiveType:
			return elem.proto, "", nil
		case structType:
			elementPath := sanitizeMessageName(path) + "Element"
			return mapComplexTypeToProtobuf(t.element, elementPath, collector)
		case arrayType:
			return 0, "", &SchemaError{
				Kind:    "Invalid",
				Message: fmt.Sprintf("nested arrays not supported for field '%s'", path),
			}
		case mapType:
			return 0, "", &SchemaError{
				Kind:    "Invalid",
				Message: fmt.Sprintf("arrays of maps not supported for field '%s'", path),
			}
		default:
			return 0, "", &SchemaError{
				Kind:    "Invalid",
				Message: fmt.Sprintf("unsupported array element type for field '%s'", path),
			}
		}

	case mapType:
		keyPrim, ok := t.key.(primitiveType)
		if !ok {
			return 0, "", &SchemaError{
				Kind:    "Invalid",
				Message: fmt.Sprintf("map keys must be primitive types (field '%s')", path),
			}
		}
		if !isValidMapKey(keyPrim) {
			return 0, "", &SchemaError{
				Kind:    "Invalid",
				Message: fmt.Sprintf("unsupported map key type for field '%s' (protobuf map keys must be integral, bool, or string)", path),
			}
		}

		base := sanitizeMessageName(path)

		// Handle value type.
		type mapValueInfo struct {
			proto    descriptorpb.FieldDescriptorProto_Type
			typeName string
		}
		var valInfo mapValueInfo

		switch v := t.value.(type) {
		case primitiveType:
			valInfo = mapValueInfo{proto: v.proto}
		case structType:
			valueName := collector.uniqueName(base + "Value")
			valueMsg, err := generateStructMessage(valueName, v)
			if err != nil {
				return 0, "", err
			}
			collector.push(valueMsg)
			valInfo = mapValueInfo{
				proto:    descriptorpb.FieldDescriptorProto_TYPE_MESSAGE,
				typeName: valueName,
			}
		default:
			return 0, "", &SchemaError{
				Kind:    "Invalid",
				Message: fmt.Sprintf("maps with complex value types not supported for field '%s'", path),
			}
		}

		entryName := collector.uniqueName(base + "Entry")
		entry := generateMapEntry(entryName, keyPrim.proto, valInfo.proto, valInfo.typeName)
		collector.push(entry)
		return descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, entryName, nil

	default:
		return 0, "", &SchemaError{
			Kind:    "Invalid",
			Message: fmt.Sprintf("unknown complex type for field '%s'", path),
		}
	}
}

func isValidMapKey(p primitiveType) bool {
	switch p.proto {
	case descriptorpb.FieldDescriptorProto_TYPE_DOUBLE,
		descriptorpb.FieldDescriptorProto_TYPE_FLOAT,
		descriptorpb.FieldDescriptorProto_TYPE_BYTES:
		return false
	default:
		return true
	}
}

func generateStructMessage(messageName string, st structType) (*descriptorpb.DescriptorProto, error) {
	local := newMessageCollector()
	fields := make([]*descriptorpb.FieldDescriptorProto, 0, len(st.fields))
	for i, f := range st.fields {
		if err := validateFieldName(f.name); err != nil {
			return nil, err
		}
		path := messageName + "_" + f.name
		ft, tn, err := mapComplexTypeToProtobuf(f.fieldType, path, local)
		if err != nil {
			return nil, err
		}
		var isRepeated bool
		switch f.fieldType.(type) {
		case arrayType, mapType:
			isRepeated = true
		}
		fields = append(fields, buildFieldDescriptor(f.name, int32(i+1), ft, tn, f.nullable, isRepeated))
	}
	return &descriptorpb.DescriptorProto{
		Name:       strPtr(messageName),
		Field:      fields,
		NestedType: local.nested,
	}, nil
}

func generateMapEntry(
	name string,
	keyType descriptorpb.FieldDescriptorProto_Type,
	valueType descriptorpb.FieldDescriptorProto_Type,
	valueTypeName string,
) *descriptorpb.DescriptorProto {
	keyField := &descriptorpb.FieldDescriptorProto{
		Name:           strPtr("key"),
		Number:         int32Ptr(1),
		Label:          descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		Type:           keyType.Enum(),
		JsonName:       strPtr("key"),
		Proto3Optional: boolPtr(false),
	}
	valueField := &descriptorpb.FieldDescriptorProto{
		Name:           strPtr("value"),
		Number:         int32Ptr(2),
		Label:          descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		Type:           valueType.Enum(),
		JsonName:       strPtr("value"),
		Proto3Optional: boolPtr(true),
	}
	if valueTypeName != "" {
		valueField.TypeName = strPtr(valueTypeName)
	}
	return &descriptorpb.DescriptorProto{
		Name:  strPtr(name),
		Field: []*descriptorpb.FieldDescriptorProto{keyField, valueField},
		Options: &descriptorpb.MessageOptions{
			MapEntry: boolPtr(true),
		},
	}
}

var reservedFieldNames = map[string]struct{}{
	"syntax": {}, "import": {}, "option": {}, "package": {}, "message": {},
	"enum": {}, "service": {}, "rpc": {}, "returns": {}, "reserved": {},
	"to": {}, "max": {}, "double": {}, "float": {}, "int32": {},
	"int64": {}, "uint32": {}, "uint64": {}, "sint32": {}, "sint64": {},
	"fixed32": {}, "fixed64": {}, "sfixed32": {}, "sfixed64": {}, "bool": {},
	"string": {}, "bytes": {},
}

func validateFieldName(name string) error {
	if name == "" {
		return &SchemaError{
			Kind:    "InvalidFieldName",
			Field:   name,
			Message: "invalid field name '': empty",
		}
	}
	if name[0] >= '0' && name[0] <= '9' {
		return &SchemaError{
			Kind:    "InvalidFieldName",
			Field:   name,
			Message: fmt.Sprintf("invalid field name '%s': cannot start with a digit", name),
		}
	}
	for _, c := range name {
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
			return &SchemaError{
				Kind:    "InvalidFieldName",
				Field:   name,
				Message: fmt.Sprintf("invalid field name '%s': only alphanumeric and '_' characters allowed", name),
			}
		}
	}
	if _, reserved := reservedFieldNames[name]; reserved {
		return &SchemaError{
			Kind:    "InvalidFieldName",
			Field:   name,
			Message: fmt.Sprintf("invalid field name '%s': reserved proto keyword", name),
		}
	}
	return nil
}

// sanitizeMessageName converts a UC identifier to a valid PascalCase protobuf message name.
func sanitizeMessageName(name string) string {
	var out strings.Builder
	out.Grow(len(name))
	capitalize := true
	for _, r := range name {
		if r <= unicode.MaxASCII && ((r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
			if capitalize {
				out.WriteRune(toASCIIUpper(r))
				capitalize = false
			} else {
				out.WriteRune(r)
			}
		} else {
			// Non-ASCII or non-alphanumeric: skip and capitalize next.
			capitalize = true
		}
	}
	s := out.String()
	if len(s) == 0 || !(s[0] >= 'A' && s[0] <= 'Z') && !(s[0] >= 'a' && s[0] <= 'z') {
		return "M" + s
	}
	return s
}

func toASCIIUpper(r rune) rune {
	if r >= 'a' && r <= 'z' {
		return r - ('a' - 'A')
	}
	return r
}
