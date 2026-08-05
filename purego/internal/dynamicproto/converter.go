// Package dynamicproto converts JSON payloads into protobuf bytes at runtime.
package dynamicproto

import (
	"bytes"
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
	message protoreflect.MessageDescriptor
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
	return &Converter{message: message}, nil
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
	out, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("dynamicproto: encode protobuf payload: %w", err)
	}
	return out, nil
}
