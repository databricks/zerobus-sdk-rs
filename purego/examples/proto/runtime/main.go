// Runtime protobuf ingestion without .proto or generated Go files.
// Run from the examples directory:
//
//	go run ./proto/runtime
package main

import (
	"context"
	"fmt"
	"log"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/databricks/zerobus-sdk/purego/examples/config"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

func optionalField(
	name string,
	number int32,
	kind descriptorpb.FieldDescriptorProto_Type,
) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		Type:   kind.Enum(),
	}
}

func orderDescriptor() ([]byte, protoreflect.MessageDescriptor, error) {
	order := &descriptorpb.DescriptorProto{
		Name: proto.String("Order"),
		Field: []*descriptorpb.FieldDescriptorProto{
			optionalField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT32),
			optionalField("customer_name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING),
			optionalField("product_name", 3, descriptorpb.FieldDescriptorProto_TYPE_STRING),
			optionalField("quantity", 4, descriptorpb.FieldDescriptorProto_TYPE_INT32),
			optionalField("price", 5, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE),
			optionalField("status", 6, descriptorpb.FieldDescriptorProto_TYPE_STRING),
			optionalField("created_at", 7, descriptorpb.FieldDescriptorProto_TYPE_INT64),
			optionalField("updated_at", 8, descriptorpb.FieldDescriptorProto_TYPE_INT64),
		},
	}

	descriptorBytes, err := proto.Marshal(order)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal descriptor: %w", err)
	}
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:        proto.String("runtime_orders.proto"),
		Package:     proto.String("zerobus_examples"),
		Syntax:      proto.String("proto2"),
		MessageType: []*descriptorpb.DescriptorProto{order},
	}, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build descriptor: %w", err)
	}
	message := file.Messages().ByName("Order")
	if message == nil {
		return nil, nil, fmt.Errorf("build descriptor: Order message not found")
	}
	return descriptorBytes, message, nil
}

func makeOrder(
	md protoreflect.MessageDescriptor,
	id int32,
	customer, product string,
	quantity int32,
	price float64,
	status string,
	ts int64,
) ([]byte, error) {
	message := dynamicpb.NewMessage(md)
	fields := md.Fields()
	message.Set(fields.ByName("id"), protoreflect.ValueOfInt32(id))
	message.Set(fields.ByName("customer_name"), protoreflect.ValueOfString(customer))
	message.Set(fields.ByName("product_name"), protoreflect.ValueOfString(product))
	message.Set(fields.ByName("quantity"), protoreflect.ValueOfInt32(quantity))
	message.Set(fields.ByName("price"), protoreflect.ValueOfFloat64(price))
	message.Set(fields.ByName("status"), protoreflect.ValueOfString(status))
	message.Set(fields.ByName("created_at"), protoreflect.ValueOfInt64(ts))
	message.Set(fields.ByName("updated_at"), protoreflect.ValueOfInt64(ts))
	return proto.Marshal(message)
}

func main() {
	cfg := config.Load()
	descriptor, messageDescriptor, err := orderDescriptor()
	if err != nil {
		log.Fatal(err)
	}

	sdk, err := zerobus.New(
		cfg.ServerEndpoint,
		cfg.WorkspaceURL,
		zerobus.WithApplicationName("proto-runtime"),
	)
	if err != nil {
		log.Fatalf("create SDK: %v", err)
	}
	defer sdk.Close()

	stream, err := sdk.CreateStream(
		context.Background(),
		cfg.TableName,
		cfg.ClientID,
		cfg.ClientSecret,
		zerobus.WithProto(descriptor),
	)
	if err != nil {
		log.Fatalf("create stream: %v", err)
	}
	defer stream.Close()

	now := config.NowMicros()
	orders := []struct {
		id       int32
		customer string
		product  string
		quantity int32
		price    float64
		status   string
	}{
		{1, "Alice Smith", "Wireless Mouse", 2, 25.99, "pending"},
		{2, "Bob Johnson", "Mechanical Keyboard", 1, 89.99, "shipped"},
		{3, "Carol Williams", "USB-C Hub", 3, 45.00, "delivered"},
	}

	for i, order := range orders {
		record, err := makeOrder(
			messageDescriptor,
			order.id,
			order.customer,
			order.product,
			order.quantity,
			order.price,
			order.status,
			now,
		)
		if err != nil {
			log.Fatalf("marshal record %d: %v", i+1, err)
		}
		if _, err := stream.IngestRecordOffset(record); err != nil {
			log.Fatalf("ingest record %d: %v", i+1, err)
		}
	}
	if err := stream.Flush(); err != nil {
		log.Fatalf("flush: %v", err)
	}
	log.Printf("Ingested %d runtime protobuf records.", len(orders))
}
