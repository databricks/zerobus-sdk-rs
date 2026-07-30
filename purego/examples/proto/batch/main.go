// Batch protobuf ingestion with the Zerobus pure-Go SDK.
//
// Opens a proto stream and ingests records with the BATCH API,
// IngestRecordsOffset, which queues a whole slice of records in one call and
// returns the single offset assigned to the batch. Prefer the batch API in hot
// paths: a batch is one buffer entry and one atomic ack.
//
// As in the single-record proto example, a static message descriptor (marshaled
// from the generated bindings in proto/pb) is supplied at stream creation.
//
// Configuration — every connection setting is read from the environment. Export
// these before running (see the examples README):
//
//	ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//	DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
//
//	go run ./proto/batch
//
// Target table (see the examples README and proto/orders.proto):
//
//	orders(id INT, customer_name STRING, product_name STRING, quantity INT,
//	       price DOUBLE, status STRING, created_at TIMESTAMP, updated_at TIMESTAMP)
package main

import (
	"context"
	"log"

	"google.golang.org/protobuf/proto"

	"github.com/databricks/zerobus-sdk/purego/examples/config"
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

	now := config.NowMicros()

	// Marshal each record, collect them into a batch, then hand the whole batch
	// over in a single call.
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

	// Waiting on the batch's single offset confirms every record in it.
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
