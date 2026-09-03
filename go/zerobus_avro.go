//go:build avro

package zerobus

import (
	"runtime"
)

// AvroTableProperties contains information about the target table for Avro ingestion
type AvroTableProperties struct {
	// Fully qualified table name (catalog.schema.table)
	TableName string

	// Avro writer schema as a JSON string (required)
	SchemaJSON string
}

// CreateAvroStream creates a new Avro stream for ingesting pre-encoded Avro records.
// This is a Beta feature gated by the `avro` build tag. The Zerobus service Avro support is pending.
//
// Parameters:
//   - tableProps: Table properties including name and Avro schema JSON
//   - clientID: OAuth 2.0 client ID
//   - clientSecret: OAuth 2.0 client secret
//   - options: Stream configuration options (nil for defaults)
//
// Returns an error if:
//   - Invalid table name format
//   - Invalid Avro schema JSON
//   - Authentication fails
//   - Network connectivity issues
//
// Example:
//
//	schemaJSON := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`
//	stream, err := sdk.CreateAvroStream(
//	    AvroTableProperties{
//	        TableName: "catalog.schema.table",
//	        SchemaJSON: schemaJSON,
//	    },
//	    clientID,
//	    clientSecret,
//	    nil,
//	)
func (s *ZerobusSdk) CreateAvroStream(
	tableProps AvroTableProperties,
	clientID string,
	clientSecret string,
	options *StreamConfigurationOptions,
) (*ZerobusStream, error) {
	if s.ptr == nil {
		return nil, &ZerobusError{Message: "SDK has been freed", IsRetryable: false}
	}

	ptr, err := sdkCreateAvroStream(
		s.ptr,
		tableProps.TableName,
		tableProps.SchemaJSON,
		clientID,
		clientSecret,
		options,
	)
	if err != nil {
		return nil, err
	}

	stream := &ZerobusStream{ptr: ptr}

	// Set up finalizer for automatic cleanup
	runtime.SetFinalizer(stream, func(st *ZerobusStream) {
		st.Close()
	})

	return stream, nil
}

// CreateAvroStreamWithHeadersProvider creates a new Avro stream using a custom headers provider.
// This is a Beta feature gated by the `avro` build tag. The Zerobus service Avro support is pending.
//
// Parameters:
//   - tableProps: Table properties including name and Avro schema JSON
//   - headersProvider: Custom implementation of HeadersProvider interface
//   - options: Stream configuration options (nil for defaults)
//
// Returns an error if:
//   - Invalid table name format
//   - Invalid Avro schema JSON
//   - Headers provider returns an error
//   - Network connectivity issues
//
// Example:
//
//	provider := &CustomHeadersProvider{}
//	schemaJSON := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`
//	stream, err := sdk.CreateAvroStreamWithHeadersProvider(
//	    AvroTableProperties{
//	        TableName: "catalog.schema.table",
//	        SchemaJSON: schemaJSON,
//	    },
//	    provider,
//	    nil,
//	)
func (s *ZerobusSdk) CreateAvroStreamWithHeadersProvider(
	tableProps AvroTableProperties,
	headersProvider HeadersProvider,
	options *StreamConfigurationOptions,
) (*ZerobusStream, error) {
	if s.ptr == nil {
		return nil, &ZerobusError{Message: "SDK has been freed", IsRetryable: false}
	}

	ptr, err := sdkCreateAvroStreamWithHeadersProvider(
		s.ptr,
		tableProps.TableName,
		tableProps.SchemaJSON,
		headersProvider,
		options,
	)
	if err != nil {
		return nil, err
	}

	stream := &ZerobusStream{ptr: ptr}

	// Set up finalizer for automatic cleanup
	runtime.SetFinalizer(stream, func(st *ZerobusStream) {
		st.Close()
	})

	return stream, nil
}

// IngestAvroRecordOffset ingests a pre-encoded Avro datum and returns the offset.
// This is a Beta API gated by the `avro` build tag.
//
// Avro records must be pre-encoded as binary Avro datums before passing to this method.
// This method returns as soon as the record is queued; the SDK sends it and tracks its
// acknowledgment in the background.
//
// The idiomatic flow is to ingest in a loop and call Flush() to confirm durability.
//
// Parameters:
//   - data: Pre-encoded Avro datum as []byte
//
// Returns:
//   - int64: The offset of the ingested record
//   - error: Any error that occurred during ingestion
//
// Example:
//
//	// Assuming you have pre-encoded Avro data
//	avroData := encodeAvroRecord(record, schemaJSON)
//	offset, err := stream.IngestAvroRecordOffset(avroData)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (st *ZerobusStream) IngestAvroRecordOffset(data []byte) (int64, error) {
	if st.ptr == nil {
		return -1, &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}

	return streamIngestAvroRecord(st.ptr, data)
}

// IngestAvroRecordsOffset ingests a batch of pre-encoded Avro datums and returns one offset for the batch.
// This is a Beta API gated by the `avro` build tag.
//
// All records in the batch must be pre-encoded Avro datums.
// This method returns as soon as the batch is queued; the server round-trip happens in the background.
//
// Parameters:
//   - records: Slice of pre-encoded Avro datums, each as []byte
//
// Returns:
//   - int64: One offset that represents the entire batch
//   - error: Any error that occurred during ingestion
//
// If the batch is empty, returns -1 with no error.
//
// Example:
//
//	records := [][]byte{
//	    encodeAvroRecord(record1, schemaJSON),
//	    encodeAvroRecord(record2, schemaJSON),
//	    encodeAvroRecord(record3, schemaJSON),
//	}
//	offset, err := stream.IngestAvroRecordsOffset(records)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (st *ZerobusStream) IngestAvroRecordsOffset(records [][]byte) (int64, error) {
	if st.ptr == nil {
		return -1, &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}

	if len(records) == 0 {
		return -1, nil
	}

	return streamIngestAvroRecords(st.ptr, records)
}

// IngestAvroRecordNowait ingests a pre-encoded Avro datum without waiting (fire-and-forget).
// This is a Beta API gated by the `avro` build tag.
//
// The function returns immediately after spawning a background task to queue the record.
// Ingestion errors from the background task are silently ignored.
//
// The stream must remain open until all background tasks have completed.
//
// Parameters:
//   - data: Pre-encoded Avro datum as []byte
//
// Returns an error only for argument validation failures (e.g. nil stream, empty data).
//
// Example:
//
//	err := stream.IngestAvroRecordNowait(avroData)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (st *ZerobusStream) IngestAvroRecordNowait(data []byte) error {
	if st.ptr == nil {
		return &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}

	return streamIngestAvroRecordNowait(st.ptr, data)
}

// IngestAvroRecordsNowait ingests a batch of pre-encoded Avro datums without waiting (fire-and-forget).
// This is a Beta API gated by the `avro` build tag.
//
// Returns immediately after the records are handed off; ingestion errors from the background task
// are silently ignored.
//
// The stream must remain open until all background tasks have completed.
//
// Parameters:
//   - records: Slice of pre-encoded Avro datums, each as []byte
//
// Returns an error only for argument validation failures (nil stream, etc).
//
// Example:
//
//	err := stream.IngestAvroRecordsNowait([][]byte{data1, data2, data3})
//	if err != nil {
//	    log.Fatal(err)
//	}
func (st *ZerobusStream) IngestAvroRecordsNowait(records [][]byte) error {
	if st.ptr == nil {
		return &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}

	if len(records) == 0 {
		return nil
	}

	return streamIngestAvroRecordsNowait(st.ptr, records)
}
