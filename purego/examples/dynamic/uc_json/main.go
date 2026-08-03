package main

import (
	"context"
	"log"

	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

func main() {
	ctx := context.Background()

	sdk, err := zerobus.New(
		"https://your-workspace.zerobus.region.cloud.databricks.com",
		"https://your-workspace.cloud.databricks.com",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sdk.Close()

	stream, err := sdk.CreateDynamicProtoStream(
		ctx,
		"catalog.schema.table",
		"<client-id>",
		"<client-secret>",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close()

	records := []string{
		`{"id": 1, "payload": "hello"}`,
		`{"id": 2, "payload": "world"}`,
	}
	for _, rec := range records {
		if _, err := stream.IngestJSONStringOffset(rec); err != nil {
			log.Fatal(err)
		}
	}
	if err := stream.Flush(); err != nil {
		log.Fatal(err)
	}
}
