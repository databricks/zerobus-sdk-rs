// Typed Arrow RecordBatch ingestion example.
//
// Queues batches with IngestBatch, releases each batch after ingestion returns,
// and calls Flush once at the end.
//
// Set these environment variables before running:
//
//	ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//	DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
//
//	go run ./arrow/typed
//
// Target table:
//
//	orders(id INT, customer_name STRING, product_name STRING, quantity INT,
//	       price DOUBLE, status STRING, created_at TIMESTAMP, updated_at TIMESTAMP)
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/databricks/zerobus-sdk/purego/examples/config"
	"github.com/databricks/zerobus-sdk/purego/examples/internal/exampleutil"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

type order struct {
	id       int32
	customer string
	product  string
	quantity int32
	price    float64
	status   string
	at       arrow.Timestamp
}

func orderSchema() *arrow.Schema {
	utcMicros := &arrow.TimestampType{
		Unit:     arrow.Microsecond,
		TimeZone: "UTC",
	}
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "customer_name", Type: arrow.BinaryTypes.LargeString, Nullable: true},
		{Name: "product_name", Type: arrow.BinaryTypes.LargeString, Nullable: true},
		{Name: "quantity", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "status", Type: arrow.BinaryTypes.LargeString, Nullable: true},
		{Name: "created_at", Type: utcMicros, Nullable: true},
		{Name: "updated_at", Type: utcMicros, Nullable: true},
	}, nil)
}

func makeBatch(schema *arrow.Schema, orders []order) arrow.RecordBatch {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	for _, value := range orders {
		builder.Field(0).(*array.Int32Builder).Append(value.id)
		builder.Field(1).(*array.LargeStringBuilder).Append(value.customer)
		builder.Field(2).(*array.LargeStringBuilder).Append(value.product)
		builder.Field(3).(*array.Int32Builder).Append(value.quantity)
		builder.Field(4).(*array.Float64Builder).Append(value.price)
		builder.Field(5).(*array.LargeStringBuilder).Append(value.status)
		builder.Field(6).(*array.TimestampBuilder).Append(value.at)
		builder.Field(7).(*array.TimestampBuilder).Append(value.at)
	}
	return builder.NewRecordBatch()
}

func openStream(
	sdk *zerobus.SDK,
	cfg config.Settings,
	schema *arrow.Schema,
) (*zerobus.ArrowStream, error) {
	return sdk.CreateArrowStream(
		context.Background(),
		cfg.TableName,
		schema,
		cfg.ClientID,
		cfg.ClientSecret,
	)
}

func main() {
	cfg := config.Load()
	sdk, err := zerobus.New(
		cfg.ServerEndpoint,
		cfg.WorkspaceURL,
		zerobus.WithApplicationName("arrow-typed"),
	)
	if err != nil {
		log.Fatalf("create SDK: %v", err)
	}
	defer sdk.Close()

	schema := orderSchema()
	stream, err := openStream(sdk, cfg, schema)
	if err != nil {
		log.Fatalf("create Arrow stream: %v", err)
	}

	now := arrow.Timestamp(exampleutil.NowMicros())
	batches := [][]order{
		{
			{id: 1, customer: "Alice Smith", product: "Wireless Mouse", quantity: 2, price: 25.99, status: "pending", at: now},
			{id: 2, customer: "Bob Johnson", product: "Mechanical Keyboard", quantity: 1, price: 89.99, status: "shipped", at: now},
		},
		{
			{id: 3, customer: "Carol Williams", product: "USB-C Hub", quantity: 3, price: 45.00, status: "delivered", at: now},
		},
	}

	for index, values := range batches {
		batch := makeBatch(schema, values)
		offset, ingestErr := stream.IngestBatch(batch)
		batch.Release()
		if ingestErr != nil {
			log.Fatalf("ingest batch %d: %v", index+1, ingestErr)
		}
		log.Printf("Batch %d queued with offset ID: %d", index+1, offset)
	}

	if err := stream.Flush(); err != nil {
		log.Printf("stream failed: %v", err)
		_ = stream.Close()
		if replayErr := replayUnacked(sdk, cfg, schema, stream); replayErr != nil {
			log.Fatalf("recover Arrow batches: %v", replayErr)
		}
		return
	}
	if err := stream.Close(); err != nil {
		log.Fatalf("close: %v", err)
	}
	log.Println("All Arrow batches acknowledged.")
}

func replayUnacked(
	sdk *zerobus.SDK,
	cfg config.Settings,
	schema *arrow.Schema,
	failed *zerobus.ArrowStream,
) error {
	batches, err := failed.GetUnackedBatches()
	if err != nil {
		return fmt.Errorf("get unacknowledged batches: %w", err)
	}
	defer func() {
		for _, batch := range batches {
			batch.Release()
		}
	}()
	if len(batches) == 0 {
		return nil
	}

	retry, err := openStream(sdk, cfg, schema)
	if err != nil {
		return fmt.Errorf("reopen Arrow stream: %w", err)
	}
	defer retry.Close()

	for _, batch := range batches {
		if _, err := retry.IngestBatch(batch); err != nil {
			return fmt.Errorf("re-ingest Arrow batch: %w", err)
		}
	}
	if err := retry.Flush(); err != nil {
		return fmt.Errorf("re-flush Arrow batches: %w", err)
	}
	log.Printf("Recovered %d unacknowledged Arrow batches.", len(batches))
	return nil
}
