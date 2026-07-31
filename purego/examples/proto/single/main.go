// Single-record protobuf ingestion with the Zerobus pure-Go SDK.
//
// Opens a proto stream and ingests records ONE AT A TIME with
// IngestRecordOffset, then flushes ONCE at the end. IngestRecordOffset returns
// as soon as the record is queued; sending and acknowledgement happen in the
// background. Calling WaitForOffset/Flush after every record would collapse
// throughput. For high volume, prefer the batch API in proto/batch.
//
// A proto stream needs a message descriptor so the server can interpret the raw
// protobuf bytes. The pure-Go SDK uses a static schema: the descriptor is
// marshaled from the generated bindings in proto/pb (see proto/orders.proto).
//
// Configuration — every connection setting is read from the environment. Export
// these before running (see the examples README):
//
//	ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//	DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
//
//	go run ./proto/single
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

// makeOrder builds one Order message matching the table columns.
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

	// The serialized message descriptor the proto stream is opened with.
	descriptor, err := config.MessageDescriptorBytes((&pb.Order{}).ProtoReflect().Descriptor())
	if err != nil {
		log.Fatalf("marshal descriptor: %v", err)
	}

	sdk, err := zerobus.New(cfg.ServerEndpoint, cfg.WorkspaceURL,
		zerobus.WithApplicationName("proto-single"))
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

	// Ingest records one at a time: marshal each Order to protobuf bytes and
	// queue them — with NO per-record wait. The single wait point is the Flush.
	orders := []*pb.Order{
		makeOrder(1, "Alice Smith", "Wireless Mouse", 2, 25.99, "pending", now),
		makeOrder(2, "Bob Johnson", "Mechanical Keyboard", 1, 89.99, "shipped", now),
		makeOrder(3, "Carol Williams", "USB-C Hub", 3, 45.00, "delivered", now),
	}
	for i, order := range orders {
		data, err := proto.Marshal(order)
		if err != nil {
			log.Fatalf("marshal record %d: %v", i+1, err)
		}
		offset, err := stream.IngestRecordOffset(data)
		if err != nil {
			log.Fatalf("ingest record %d: %v", i+1, err)
		}
		log.Printf("Record %d queued with offset ID: %d", i+1, offset)
	}

	// Flush once at the end: block until every queued record is durably acked.
	if err := stream.Flush(); err != nil {
		log.Fatalf("flush: %v", err)
	}
	log.Println("All records acknowledged.")
	if err := stream.Close(); err != nil {
		log.Fatalf("close: %v", err)
	}
	log.Println("Stream closed successfully.")
}
