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

// NewFromDescriptorProtoBytes creates a converter from serialized descriptor bytes.
// The payload may be either DescriptorProto or FileDescriptorProto.
func NewFromDescriptorProtoBytes(descBytes []byte) (*Converter, error) {
	if len(descBytes) == 0 {
		return nil, fmt.Errorf("dynamicproto: descriptor bytes are empty")
	}

	var file descriptorpb.FileDescriptorProto
	if err := proto.Unmarshal(descBytes, &file); err != nil || len(file.GetMessageType()) == 0 {
		var msg descriptorpb.DescriptorProto
		if err2 := proto.Unmarshal(descBytes, &msg); err2 != nil {
			return nil, fmt.Errorf("dynamicproto: parse descriptor bytes: %w", err2)
		}
		file = descriptorpb.FileDescriptorProto{
			Name:    proto.String("zerobus_dynamic.proto"),
			Syntax:  proto.String("proto2"),
			Package: proto.String("zerobus.dynamic"),
			MessageType: []*descriptorpb.DescriptorProto{
				&msg,
			},
		}
	}
	if len(file.GetMessageType()) == 0 {
		return nil, fmt.Errorf("dynamicproto: no message descriptors found")
	}

	fd, err := protodesc.NewFile(&file, nil)
	if err != nil {
		return nil, fmt.Errorf("dynamicproto: build file descriptor: %w", err)
	}
	msg := fd.Messages().Get(0)
	if msg == nil {
		return nil, fmt.Errorf("dynamicproto: missing top-level message descriptor")
	}
	return &Converter{message: msg}, nil
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
	unmarshal := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := unmarshal.Unmarshal(record, msg); err != nil {
		return nil, fmt.Errorf("dynamicproto: parse JSON payload: %w", err)
	}
	out, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("dynamicproto: encode protobuf payload: %w", err)
	}
	return out, nil
}
