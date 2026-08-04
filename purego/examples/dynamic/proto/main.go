// UC-backed dynamic protobuf ingestion example.
// Run from the examples directory:
//
//	go run ./dynamic/proto
package main

import (
	"context"
	"fmt"
	"log"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/databricks/zerobus-sdk/purego/examples/config"
	"github.com/databricks/zerobus-sdk/purego/examples/internal/exampleutil"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

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
	record, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("marshal order: %w", err)
	}
	return record, nil
}

func main() {
	cfg := config.Load()
	sdk, err := zerobus.New(
		cfg.ServerEndpoint,
		cfg.WorkspaceURL,
		zerobus.WithApplicationName("dynamic-proto"),
	)
	if err != nil {
		log.Fatalf("create SDK: %v", err)
	}
	defer sdk.Close()

	ctx := context.Background()
	descriptorBytes, err := sdk.FetchProtoDescriptor(
		ctx, cfg.TableName, cfg.ClientID, cfg.ClientSecret,
	)
	if err != nil {
		log.Fatalf("fetch descriptor: %v", err)
	}
	stream, err := sdk.CreateStream(
		ctx,
		cfg.TableName,
		cfg.ClientID,
		cfg.ClientSecret,
		zerobus.WithProto(descriptorBytes),
	)
	if err != nil {
		log.Fatalf("create stream: %v", err)
	}
	defer stream.Close()

	descriptor := stream.MessageDescriptor()
	if descriptor == nil {
		log.Fatal("stream has no message descriptor")
	}
	now := exampleutil.NowMicros()
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
			descriptor,
			order.id,
			order.customer,
			order.product,
			order.quantity,
			order.price,
			order.status,
			now,
		)
		if err != nil {
			log.Fatalf("build record %d: %v", i+1, err)
		}
		if _, err := stream.IngestRecordOffset(record); err != nil {
			log.Fatalf("ingest record %d: %v", i+1, err)
		}
	}
	if err := stream.Flush(); err != nil {
		log.Fatalf("flush: %v", err)
	}
	log.Printf("Ingested %d dynamic protobuf records.", len(orders))
}
