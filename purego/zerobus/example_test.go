package zerobus_test

import (
	"context"
	"log"

	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

// Ingest a batch of JSON records with the idiomatic loop-then-Flush pattern:
// queue every record without waiting, then confirm durability once at the end.
func ExampleStream_ingestThenFlush() {
	sdk, err := zerobus.New(
		"https://your-workspace.zerobus.region.cloud.databricks.com",
		"https://your-workspace.cloud.databricks.com",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sdk.Close()

	stream, err := sdk.CreateStream(context.Background(), "catalog.schema.table",
		clientID(), clientSecret(), zerobus.WithJSON())
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close()

	records := [][]byte{[]byte(`{"id":1}`), []byte(`{"id":2}`), []byte(`{"id":3}`)}
	for _, rec := range records {
		if _, err := stream.IngestRecordOffset(rec); err != nil { // queue only — do NOT wait here
			log.Fatal(err)
		}
	}
	if err := stream.Flush(); err != nil { // wait once for all pending acks
		log.Fatal(err)
	}
}

// For a continuous stream, register an ack callback instead of blocking on
// Flush, and flush periodically to bound in-flight memory.
func ExampleStream_ackCallback() {
	sdk, err := zerobus.New(
		"https://your-workspace.zerobus.region.cloud.databricks.com",
		"https://your-workspace.cloud.databricks.com",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sdk.Close()

	stream, err := sdk.CreateStream(context.Background(), "catalog.schema.table",
		clientID(), clientSecret(),
		zerobus.WithJSON(),
		zerobus.WithAckCallback(logAckCallback{}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close()

	for i := 0; ; i++ {
		if _, err := stream.IngestRecordOffset([]byte(`{"event":"tick"}`)); err != nil {
			log.Fatal(err)
		}
		if i%10_000 == 0 {
			if err := stream.Flush(); err != nil { // bound in-flight memory periodically
				log.Fatal(err)
			}
		}
	}
}

// Ingest Protocol Buffer records: pass the serialized message descriptor to
// WithProto and hand IngestRecordOffset the marshaled protobuf bytes.
func ExampleStream_proto() {
	sdk, err := zerobus.New(
		"https://your-workspace.zerobus.region.cloud.databricks.com",
		"https://your-workspace.cloud.databricks.com",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sdk.Close()

	stream, err := sdk.CreateStream(context.Background(), "catalog.schema.table",
		clientID(), clientSecret(), zerobus.WithProto(descriptorProto()))
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close()

	// protoBytes is a marshaled protobuf message matching the descriptor.
	if _, err := stream.IngestRecordOffset(protoBytes()); err != nil {
		log.Fatal(err)
	}
	if err := stream.Flush(); err != nil {
		log.Fatal(err)
	}
}

// logAckCallback logs each acknowledged offset and any per-record error.
type logAckCallback struct{}

func (logAckCallback) OnAck(offset int64) { log.Printf("acked offset %d", offset) }
func (logAckCallback) OnError(offset int64, err error) {
	log.Printf("offset %d failed: %v", offset, err)
}

// Stand-ins so the examples compile without external configuration.
func clientID() string        { return "" }
func clientSecret() string    { return "" }
func descriptorProto() []byte { return nil }
func protoBytes() []byte      { return nil }
