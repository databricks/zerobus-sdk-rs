package config

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// MessageDescriptorBytes marshals the message descriptor for md into the
// serialized DescriptorProto the SDK's WithProto option expects. Pass the
// ProtoReflect().Descriptor() of a generated message (e.g.
// (&pb.AirQuality{}).ProtoReflect().Descriptor()).
func MessageDescriptorBytes(md protoreflect.MessageDescriptor) ([]byte, error) {
	return proto.Marshal(protodesc.ToDescriptorProto(md))
}
