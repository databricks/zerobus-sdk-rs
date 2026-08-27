// Package dynamicproto converts JSON payloads into protobuf bytes at runtime.
package dynamicproto

import (
	"bytes"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Converter encodes JSON payloads into protobuf bytes using a runtime descriptor.
type Converter struct {
	message                 protoreflect.MessageDescriptor
	validateNullCollections bool
}

// NewFromDescriptorProtoBytes creates a converter from a serialized DescriptorProto.
func NewFromDescriptorProtoBytes(descBytes []byte) (*Converter, error) {
	if len(descBytes) == 0 {
		return nil, fmt.Errorf("dynamicproto: descriptor bytes are empty")
	}

	var msg descriptorpb.DescriptorProto
	if err := proto.Unmarshal(descBytes, &msg); err != nil {
		return nil, fmt.Errorf("dynamicproto: parse descriptor bytes: %w", err)
	}
	file := descriptorpb.FileDescriptorProto{
		Name:        proto.String("zerobus_dynamic.proto"),
		Syntax:      proto.String("proto2"),
		Package:     proto.String("zerobus.dynamic"),
		MessageType: []*descriptorpb.DescriptorProto{&msg},
	}

	fd, err := protodesc.NewFile(&file, nil)
	if err != nil {
		return nil, fmt.Errorf("dynamicproto: build file descriptor: %w", err)
	}
	message := fd.Messages().Get(0)
	if message == nil {
		return nil, fmt.Errorf("dynamicproto: missing top-level message descriptor")
	}
	return &Converter{
		message:                 message,
		validateNullCollections: hasCollectionFields(message),
	}, nil
}

// MessageDescriptor returns the converter's runtime message descriptor.
func (c *Converter) MessageDescriptor() protoreflect.MessageDescriptor {
	if c == nil {
		return nil
	}
	return c.message
}

// EncodeJSONBytes converts one JSON payload to protobuf bytes.
func (c *Converter) EncodeJSONBytes(record []byte) ([]byte, error) {
	if c == nil || c.message == nil {
		return nil, fmt.Errorf("dynamicproto: converter is not initialized")
	}
	if len(bytes.TrimSpace(record)) == 0 {
		return nil, fmt.Errorf("dynamicproto: record is empty")
	}
	msg := dynamicpb.NewMessage(c.message)
	unmarshal := protojson.UnmarshalOptions{}
	if err := unmarshal.Unmarshal(record, msg); err != nil {
		return nil, fmt.Errorf("dynamicproto: parse JSON payload: %w", err)
	}
	// A JSON null token always contains these exact bytes. Matches inside strings
	// are harmless false positives that only trigger the validation pass.
	if c.validateNullCollections && bytes.Contains(record, []byte("null")) {
		if err := rejectNullCollections(record, c.message); err != nil {
			return nil, err
		}
	}
	out, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("dynamicproto: encode protobuf payload: %w", err)
	}
	return out, nil
}

func rejectNullCollections(record []byte, message protoreflect.MessageDescriptor) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(record, &object); err != nil {
		return fmt.Errorf("dynamicproto: inspect JSON collections: %w", err)
	}
	return rejectNullCollectionsInObject(object, message, "")
}

func rejectNullCollectionsInObject(
	object map[string]json.RawMessage,
	message protoreflect.MessageDescriptor,
	path string,
) error {
	for name, raw := range object {
		field := fieldByJSONName(message.Fields(), name)
		if field == nil {
			continue
		}
		fieldPath := name
		if path != "" {
			fieldPath = path + "." + name
		}
		value := bytes.TrimSpace(raw)
		if field.Cardinality() == protoreflect.Repeated {
			if bytes.Equal(value, []byte("null")) {
				return fmt.Errorf(
					"dynamicproto: collection field %q cannot be null",
					fieldPath,
				)
			}
			if field.IsMap() {
				if err := rejectNullMapValues(value, field, fieldPath); err != nil {
					return err
				}
				continue
			}
			if err := rejectNullListElements(value, field, fieldPath); err != nil {
				return err
			}
			continue
		}
		if field.Message() != nil && !bytes.Equal(value, []byte("null")) {
			var nested map[string]json.RawMessage
			if err := json.Unmarshal(value, &nested); err == nil {
				if err := rejectNullCollectionsInObject(nested, field.Message(), fieldPath); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func rejectNullListElements(
	value []byte,
	field protoreflect.FieldDescriptor,
	path string,
) error {
	var elements []json.RawMessage
	if err := json.Unmarshal(value, &elements); err != nil {
		return fmt.Errorf("dynamicproto: inspect array field %q: %w", path, err)
	}
	for i, element := range elements {
		element = bytes.TrimSpace(element)
		if bytes.Equal(element, []byte("null")) {
			return fmt.Errorf(
				"dynamicproto: array field %q contains null element at index %d",
				path,
				i,
			)
		}
		if field.Message() != nil {
			var nested map[string]json.RawMessage
			if err := json.Unmarshal(element, &nested); err == nil {
				if err := rejectNullCollectionsInObject(nested, field.Message(), path); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func rejectNullMapValues(
	value []byte,
	field protoreflect.FieldDescriptor,
	path string,
) error {
	var entries map[string]json.RawMessage
	if err := json.Unmarshal(value, &entries); err != nil {
		return fmt.Errorf("dynamicproto: inspect map field %q: %w", path, err)
	}
	mapValue := field.MapValue()
	for key, entry := range entries {
		entry = bytes.TrimSpace(entry)
		if bytes.Equal(entry, []byte("null")) {
			return fmt.Errorf(
				"dynamicproto: map field %q contains null value for key %q",
				path,
				key,
			)
		}
		if mapValue.Message() != nil {
			var nested map[string]json.RawMessage
			if err := json.Unmarshal(entry, &nested); err == nil {
				if err := rejectNullCollectionsInObject(nested, mapValue.Message(), path); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func fieldByJSONName(
	fields protoreflect.FieldDescriptors,
	name string,
) protoreflect.FieldDescriptor {
	if field := fields.ByJSONName(name); field != nil {
		return field
	}
	return fields.ByTextName(name)
}

func hasCollectionFields(message protoreflect.MessageDescriptor) bool {
	visited := make(map[protoreflect.FullName]struct{})
	var visit func(protoreflect.MessageDescriptor) bool
	visit = func(current protoreflect.MessageDescriptor) bool {
		if current == nil {
			return false
		}
		if _, ok := visited[current.FullName()]; ok {
			return false
		}
		visited[current.FullName()] = struct{}{}

		fields := current.Fields()
		for i := 0; i < fields.Len(); i++ {
			field := fields.Get(i)
			if field.Cardinality() == protoreflect.Repeated || visit(field.Message()) {
				return true
			}
		}
		return false
	}
	return visit(message)
}
