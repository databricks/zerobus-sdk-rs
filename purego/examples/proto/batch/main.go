// Batch protobuf ingestion example.
//
// Uses IngestRecordsOffset with a descriptor from generated bindings.
//
// Set these environment variables before running:
//
//	ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//	DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
//
//	go run ./proto/batch
//
// Target table:
//
//	orders(id INT, customer_name STRING, product_name STRING, quantity INT,
//	       price DOUBLE, status STRING, created_at TIMESTAMP, updated_at TIMESTAMP)
package main

import (
	"context"
	"log"

	"google.golang.org/protobuf/proto"

	"github.com/databricks/zerobus-sdk/purego/examples/config"
	"github.com/databricks/zerobus-sdk/purego/examples/internal/exampleutil"
	"github.com/databricks/zerobus-sdk/purego/examples/proto/pb"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

func makeOrder(id int, customer, product string, quantity int, price float64, status string, ts int64) *pb.Order {
	return &pb.Order{
		Id:           proto.Int32(int32(id)),
		CustomerName: proto.String(customer),
		ProductName:  proto.String(product),
		Quantity:     proto.Int32(int32(quantity)),
		Price:        proto.Float64(price),
		Status:       proto.String(status),
		CreatedAt:    proto.Int64(ts),
		UpdatedAt:    proto.Int64(ts),
	}
}

func main() {
	cfg := config.Load()

	descriptor, err := config.MessageDescriptorBytes((&pb.Order{}).ProtoReflect().Descriptor())
	if err != nil {
		log.Fatalf("marshal descriptor: %v", err)
	}

	sdk, err := zerobus.New(cfg.ServerEndpoint, cfg.WorkspaceURL,
		zerobus.WithApplicationName("proto-batch"))
	if err != nil {
		log.Fatalf("create SDK: %v", err)
	}
	defer sdk.Close()

	stream, err := sdk.CreateStream(context.Background(), cfg.TableName,
		cfg.ClientID, cfg.ClientSecret, zerobus.WithProto(descriptor))
	if err != nil {
		log.Fatalf("create stream: %v", err)
	}
	defer stream.Close()

	now := exampleutil.NowMicros()

	// Marshal records and queue one batch.
	orders := []*pb.Order{
		makeOrder(1, "Alice Smith", "Wireless Mouse", 2, 25.99, "pending", now),
		makeOrder(2, "Bob Johnson", "Mechanical Keyboard", 1, 89.99, "shipped", now),
		makeOrder(3, "Carol Williams", "USB-C Hub", 3, 45.00, "delivered", now),
	}
	batch := make([][]byte, 0, len(orders))
	for i, order := range orders {
		data, err := proto.Marshal(order)
		if err != nil {
			log.Fatalf("marshal record %d: %v", i+1, err)
		}
		batch = append(batch, data)
	}

	batchOffset, err := stream.IngestRecordsOffset(batch)
	if err != nil {
		log.Fatalf("ingest batch: %v", err)
	}
	log.Printf("Batch of %d records queued; batch offset ID: %d", len(batch), batchOffset)

	// Waiting on the batch offset confirms the batch.
	if batchOffset >= 0 {
		if err := stream.WaitForOffset(batchOffset); err != nil {
			log.Fatalf("wait for offset %d: %v", batchOffset, err)
		}
		log.Printf("Batch acknowledged at offset ID: %d", batchOffset)
	}

	if err := stream.Flush(); err != nil {
		log.Fatalf("flush: %v", err)
	}
	if err := stream.Close(); err != nil {
		log.Fatalf("close: %v", err)
	}
	log.Println("Stream closed successfully.")
}
