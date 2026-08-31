// Package zerobus provides a high-performance Go client for streaming data ingestion
// into Databricks Delta tables using the Zerobus service.
//
// Zerobus is a high-throughput streaming service for direct data ingestion into
// Databricks Delta tables, optimized for real-time data pipelines and high-volume workloads.
//
// # Installation
//
// This package is a CGO wrapper around a Rust core. Tagged releases and
// checkouts that include lib/ archives do not need Rust. Consumers can install
// with:
//
//	go get github.com/databricks/zerobus-sdk/go@v1.4.0
//
// Prerequisites for consumers: Go 1.21+, CGO enabled, a C compiler.
// Rust and `go generate` are required only when rebuilding the FFI.
//
// # Quick Start
//
// Create an SDK instance and stream:
//
//	sdk, err := zerobus.NewZerobusSdkWithOptions(
//	    "https://your-shard.zerobus.databricks.com",
//	    "https://your-workspace.databricks.com",
//	    zerobus.WithApplicationName("my-app/1.0"),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer sdk.Free()
//
//	options := zerobus.DefaultStreamConfigurationOptions()
//	options.RecordType = zerobus.RecordTypeJson
//
//	stream, err := sdk.CreateStream(
//	    zerobus.TableProperties{TableName: "catalog.schema.table"},
//	    clientID,
//	    clientSecret,
//	    options,
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer stream.Close()
//
// # Ingesting Data
//
// Queue records in a loop, then wait once for all acknowledgments:
//
//	for _, record := range records {
//	    if _, err := stream.IngestRecordOffset(record); err != nil {
//	        log.Fatal(err)
//	    }
//	}
//	if err := stream.Flush(); err != nil {
//	    log.Fatal(err)
//	}
//
// The legacy API is still supported but deprecated. Awaiting each record is
// appropriate only for low-volume cases that require confirmation before
// continuing:
//
//	ack, err := stream.IngestRecord(`{"id": 1, "message": "Hello"}`)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	offset, err := ack.Await()
//
// Protocol Buffer records:
//
//	protoBytes, _ := proto.Marshal(myMessage)
//	offset, err := stream.IngestRecordOffset(protoBytes)
//
// # Authentication
//
// The SDK supports OAuth 2.0 authentication with Unity Catalog:
//
//	stream, err := sdk.CreateStream(
//	    tableProps,
//	    os.Getenv("DATABRICKS_CLIENT_ID"),
//	    os.Getenv("DATABRICKS_CLIENT_SECRET"),
//	    options,
//	)
//
// For custom authentication, implement the HeadersProvider interface:
//
//	type CustomAuth struct{}
//
//	func (a *CustomAuth) GetHeaders() (map[string]string, error) {
//	    return map[string]string{
//	        "authorization": "Bearer " + getToken(),
//	        "x-databricks-zerobus-table-name": "catalog.schema.table",
//	    }, nil
//	}
//
//	stream, err := sdk.CreateStreamWithHeadersProvider(tableProps, &CustomAuth{}, options)
//
// # Error Handling
//
// Errors are categorized as retryable or non-retryable:
//
//	_, err := stream.IngestRecordOffset(data)
//	if err != nil {
//	    if zbErr, ok := err.(*zerobus.ZerobusError); ok {
//	        if zbErr.Retryable() {
//	            // Transient error, SDK will auto-recover
//	        } else {
//	            // Fatal error, manual intervention needed
//	        }
//	    }
//	}
//
// # Performance
//
// Ingestion is asynchronous and pipelined. Queue records without waiting in
// the loop, then call Flush once. Prefer IngestRecordsOffset for hot paths to
// amortize cgo overhead:
//
//	for _, data := range records {
//	    if _, err := stream.IngestRecordOffset(data); err != nil {
//	        log.Fatal(err)
//	    }
//	}
//	if err := stream.Flush(); err != nil {
//	    log.Fatal(err)
//	}
//
// # Static Linking
//
// This SDK uses static linking of the Rust FFI layer, resulting in self-contained
// Go binaries with no runtime dependencies or library path configuration needed.
//
// For more information, visit: https://github.com/databricks/zerobus-sdk/go
package zerobus

import (
	"runtime"
	"strings"
	"unicode/utf8"
	"unsafe"
)

// ZerobusSdk is the main entry point for interacting with the Zerobus ingestion service.
// It manages the connection to the Zerobus endpoint and Unity Catalog.
type ZerobusSdk struct {
	ptr unsafe.Pointer
}

// ZerobusStream represents an active JSON or Protocol Buffer stream for ingesting records.
// Records can be ingested concurrently and will be acknowledged asynchronously.
type ZerobusStream struct {
	ptr unsafe.Pointer
}

type sdkOptions struct {
	applicationName     string
	connectionPerStream *bool
}

// SdkOption configures a ZerobusSdk created with NewZerobusSdkWithOptions.
type SdkOption func(*sdkOptions)

// WithApplicationName appends a caller-supplied identifier, such as
// "my-app/1.0", to the HTTP user-agent header. Leading and trailing whitespace
// is trimmed, and empty or whitespace-only names are ignored. The final value
// is "zerobus-sdk-go/<version> <name>".
//
// An invalid UTF-8 name, a name containing a NUL byte, or a name that is not a
// valid HTTP header value causes NewZerobusSdkWithOptions to return a
// non-retryable construction error.
func WithApplicationName(name string) SdkOption {
	name = strings.TrimSpace(name)
	return func(options *sdkOptions) {
		options.applicationName = name
	}
}

// WithConnectionPerStream controls whether every JSON/protobuf ingestion
// stream receives a dedicated gRPC connection. It is enabled by default. Pass
// false to multiplex all streams created by the SDK over one shared HTTP/2
// connection.
func WithConnectionPerStream(enabled bool) SdkOption {
	return func(options *sdkOptions) {
		options.connectionPerStream = &enabled
	}
}

// NewZerobusSdk creates a new SDK instance.
//
// Parameters:
//   - zerobusEndpoint: The gRPC endpoint for the Zerobus service (e.g., "https://zerobus.databricks.com")
//   - unityCatalogURL: The Unity Catalog URL for OAuth token acquisition (e.g., "https://workspace.databricks.com")
//
// Returns an error if:
//   - Invalid endpoint URLs
//   - Unable to extract workspace ID from Unity Catalog URL
func NewZerobusSdk(zerobusEndpoint, unityCatalogURL string) (*ZerobusSdk, error) {
	return newZerobusSdk(zerobusEndpoint, unityCatalogURL, sdkOptions{})
}

// NewZerobusSdkWithOptions creates an SDK instance with optional settings.
// Use WithApplicationName to add an application identifier to the user-agent
// header sent on every Zerobus request. Use WithConnectionPerStream(false) to
// opt into sharing one connection; dedicated connections are the default.
//
// Application names are trimmed before use, and blank values are ignored.
// Invalid UTF-8, NUL bytes, and values that are invalid in an HTTP header cause
// this function to return a non-retryable construction error.
//
// Existing callers that do not need options should continue to use
// NewZerobusSdk.
//
// Example:
//
//	sdk, err := zerobus.NewZerobusSdkWithOptions(
//	    "https://workspace.zerobus.databricks.com",
//	    "https://workspace.cloud.databricks.com",
//	    zerobus.WithApplicationName("my-app/1.0"),
//	)
func NewZerobusSdkWithOptions(
	zerobusEndpoint string,
	unityCatalogURL string,
	opts ...SdkOption,
) (*ZerobusSdk, error) {
	var resolved sdkOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&resolved)
		}
	}
	return newZerobusSdk(zerobusEndpoint, unityCatalogURL, resolved)
}

func newZerobusSdk(
	zerobusEndpoint string,
	unityCatalogURL string,
	opts sdkOptions,
) (*ZerobusSdk, error) {
	if strings.IndexByte(opts.applicationName, 0) >= 0 {
		return nil, &ZerobusError{
			Message:     "application name must not contain a NUL byte",
			IsRetryable: false,
		}
	}
	if !utf8.ValidString(opts.applicationName) {
		return nil, &ZerobusError{
			Message:     "application name must be valid UTF-8",
			IsRetryable: false,
		}
	}

	ptr, err := sdkNew(zerobusEndpoint, unityCatalogURL, opts)
	if err != nil {
		return nil, err
	}

	sdk := &ZerobusSdk{ptr: ptr}

	// Set up finalizer for automatic cleanup
	runtime.SetFinalizer(sdk, func(s *ZerobusSdk) {
		s.Free()
	})

	return sdk, nil
}

// Free explicitly releases resources associated with the SDK.
// The SDK cannot be used after calling Free().
// Note: This is automatically called by the garbage collector, but can be called explicitly for deterministic cleanup.
func (s *ZerobusSdk) Free() {
	if s.ptr != nil {
		sdkFree(s.ptr)
		s.ptr = nil
	}
}

// CreateStream creates a new JSON or Protocol Buffer stream for ingesting records into a Databricks table.
// This method uses OAuth 2.0 client credentials flow for authentication.
//
// Parameters:
//   - tableProps: Table properties including name and optional protobuf descriptor
//   - clientID: OAuth 2.0 client ID
//   - clientSecret: OAuth 2.0 client secret
//   - options: Stream configuration options (nil for defaults)
//
// Returns an error if:
//   - Invalid table name format
//   - Authentication fails
//   - Insufficient permissions
//   - Network connectivity issues
//
// Example:
//
//	stream, err := sdk.CreateStream(
//	    TableProperties{
//	        TableName: "catalog.schema.table",
//	        DescriptorProto: descriptorBytes,
//	    },
//	    clientID,
//	    clientSecret,
//	    nil, // use default options
//	)
func (s *ZerobusSdk) CreateStream(
	tableProps TableProperties,
	clientID string,
	clientSecret string,
	options *StreamConfigurationOptions,
) (*ZerobusStream, error) {
	if s.ptr == nil {
		return nil, &ZerobusError{Message: "SDK has been freed", IsRetryable: false}
	}

	ptr, err := sdkCreateStream(
		s.ptr,
		tableProps.TableName,
		tableProps.DescriptorProto,
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

// HeadersProvider is an interface for providing custom authentication headers.
// Implement this interface to provide custom authentication logic.
//
// Example:
//
//	type CustomHeadersProvider struct{}
//
//	func (c *CustomHeadersProvider) GetHeaders() (map[string]string, error) {
//	    return map[string]string{
//	        "authorization": "Bearer custom-token",
//	        "x-databricks-zerobus-table-name": "catalog.schema.table",
//	    }, nil
//	}
//
// The SDK owns the provider for the stream's lifetime and releases it only
// after any in-flight GetHeaders call (including one during connection
// recovery) has returned — so a slow GetHeaders racing stream teardown is
// never invoked on a released provider. GetHeaders may be called from an
// internal SDK worker thread, so implementations must be safe to use from a
// goroutine other than the one that created the stream.
type HeadersProvider interface {
	// GetHeaders returns the headers to be used for authentication.
	// This method will be called by the SDK when authentication is needed.
	GetHeaders() (map[string]string, error)
}

// CreateStreamWithHeadersProvider creates a new JSON or Protocol Buffer stream using a custom headers provider.
// This is useful for testing or when you need custom authentication logic.
//
// Parameters:
//   - tableProps: Table properties including name and optional protobuf descriptor
//   - headersProvider: Custom implementation of HeadersProvider interface
//   - options: Stream configuration options (nil for defaults)
//
// Returns an error if:
//   - Invalid table name format
//   - Headers provider returns an error
//   - Network connectivity issues
//
// Example:
//
//	provider := &CustomHeadersProvider{}
//	stream, err := sdk.CreateStreamWithHeadersProvider(
//	    TableProperties{TableName: "catalog.schema.table"},
//	    provider,
//	    nil, // use default options
//	)
func (s *ZerobusSdk) CreateStreamWithHeadersProvider(
	tableProps TableProperties,
	headersProvider HeadersProvider,
	options *StreamConfigurationOptions,
) (*ZerobusStream, error) {
	if s.ptr == nil {
		return nil, &ZerobusError{Message: "SDK has been freed", IsRetryable: false}
	}

	ptr, err := sdkCreateStreamWithHeadersProvider(
		s.ptr,
		tableProps.TableName,
		tableProps.DescriptorProto,
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

// IngestRecord ingests a record into the stream and returns an acknowledgment.
// This method blocks until the record is queued and the offset is available.
//
// Deprecated: This API is maintained for backwards compatibility.
// Use IngestRecordOffset() for a simpler API that returns the offset directly.
//
// The payload parameter accepts either:
//   - []byte for Protocol Buffer encoded records
//   - string for JSON encoded records
//
// Returns:
//   - *RecordAck: An acknowledgment containing the offset (available immediately)
//   - error: Any error that occurred during ingestion
//
// The record type is automatically detected based on the payload type.
//
// Examples:
//
//	// Old API (still works but deprecated)
//	ack, err := stream.IngestRecord(`{"field": "value1"}`)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	offset, err := ack.Await()
//
//	// Preferred: Use IngestRecordOffset instead
//	offset, err := stream.IngestRecordOffset(`{"field": "value1"}`)
func (st *ZerobusStream) IngestRecord(payload interface{}) (*RecordAck, error) {
	if st.ptr == nil {
		return nil, &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}

	var offset int64
	var err error

	switch v := payload.(type) {
	case []byte:
		offset, err = streamIngestProtoRecord(st.ptr, v)
	case string:
		offset, err = streamIngestJSONRecord(st.ptr, v)
	default:
		return nil, &ZerobusError{
			Message:     "Invalid payload type: must be []byte or string",
			IsRetryable: false,
		}
	}

	if err != nil {
		return nil, err
	}

	return &RecordAck{
		streamPtr: st.ptr,
		offset:    offset,
		err:       nil,
	}, nil
}

// IngestRecordOffset ingests a record into the stream and returns the offset directly.
// This is the preferred API for ingesting records.
// This method returns as soon as the record is queued; the SDK sends it and
// tracks its acknowledgment in the background.
//
// The idiomatic flow is to ingest in a loop and call Flush() to confirm
// durability. Use WaitForOffset() with the returned offset when you need to
// confirm a specific record before continuing (acks are ordered, so the last
// offset confirms the whole group); prefer Flush() for bulk durability. Avoid
// calling WaitForOffset() after every record in a tight loop, since that limits
// throughput to one record per round-trip.
//
// The payload parameter accepts either:
//   - []byte for Protocol Buffer encoded records
//   - string for JSON encoded records
//
// Returns:
//   - int64: The offset of the ingested record
//   - error: Any error that occurred during ingestion
//
// The record type is automatically detected based on the payload type.
//
// Examples:
//
//	// High throughput: ingest in a loop without waiting, then flush once.
//	for _, r := range records {
//	    if _, err := stream.IngestRecordOffset(r); err != nil {
//	        log.Fatal(err)
//	    }
//	}
//	if err := stream.Flush(); err != nil {
//	    log.Fatal(err)
//	}
func (st *ZerobusStream) IngestRecordOffset(payload interface{}) (int64, error) {
	if st.ptr == nil {
		return -1, &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}

	var offset int64
	var err error

	switch v := payload.(type) {
	case []byte:
		offset, err = streamIngestProtoRecord(st.ptr, v)
	case string:
		offset, err = streamIngestJSONRecord(st.ptr, v)
	default:
		return -1, &ZerobusError{
			Message:     "Invalid payload type: must be []byte or string",
			IsRetryable: false,
		}
	}

	if err != nil {
		return -1, err
	}

	return offset, nil
}

// IngestRecordNowait ingests a record into the stream without waiting for it to be queued (fire-and-forget).
// The function returns immediately after spawning a background task to queue the record.
// Ingestion errors from the background task are silently ignored.
//
// The payload parameter accepts either:
//   - []byte for Protocol Buffer encoded records
//   - string for JSON encoded records
//
// Returns an error only for argument validation failures (e.g. nil stream, invalid payload type).
//
// Note: The stream must remain open until all background tasks have completed.
//
// Example:
//
//	err := stream.IngestRecordNowait(`{"field": "value"}`)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	if err := stream.Flush(); err != nil {
//	    log.Fatal(err)
//	}
func (st *ZerobusStream) IngestRecordNowait(payload interface{}) error {
	if st.ptr == nil {
		return &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}

	switch v := payload.(type) {
	case []byte:
		return streamIngestProtoRecordNowait(st.ptr, v)
	case string:
		return streamIngestJSONRecordNowait(st.ptr, v)
	default:
		return &ZerobusError{
			Message:     "Invalid payload type: must be []byte or string",
			IsRetryable: false,
		}
	}
}

// IngestRecordsNowait ingests a batch of records without waiting for them to be queued (fire-and-forget).
// Returns immediately after the records are handed off; ingestion errors from the background task are silently ignored.
//
// The records parameter accepts a slice where each element is either:
//   - []byte for Protocol Buffer encoded records
//   - string for JSON encoded records
//
// All records in the batch must be of the same type.
// Returns an error only for argument validation failures (nil stream, mixed types, invalid payload type).
//
// Note: The stream must remain open until all background tasks have completed.
//
// Example:
//
//	err := stream.IngestRecordsNowait([]interface{}{
//	    `{"field": "value1"}`,
//	    `{"field": "value2"}`,
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	if err := stream.Flush(); err != nil {
//	    log.Fatal(err)
//	}
func (st *ZerobusStream) IngestRecordsNowait(records []interface{}) error {
	if st.ptr == nil {
		return &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}
	if len(records) == 0 {
		return nil
	}

	switch records[0].(type) {
	case []byte:
		byteRecords := make([][]byte, len(records))
		for i, r := range records {
			b, ok := r.([]byte)
			if !ok {
				return &ZerobusError{Message: "All records in batch must be of the same type ([]byte)", IsRetryable: false}
			}
			byteRecords[i] = b
		}
		return streamIngestProtoRecordsNowait(st.ptr, byteRecords)
	case string:
		stringRecords := make([]string, len(records))
		for i, r := range records {
			s, ok := r.(string)
			if !ok {
				return &ZerobusError{Message: "All records in batch must be of the same type (string)", IsRetryable: false}
			}
			stringRecords[i] = s
		}
		return streamIngestJSONRecordsNowait(st.ptr, stringRecords)
	default:
		return &ZerobusError{Message: "Invalid payload type: must be []byte or string", IsRetryable: false}
	}
}

// IngestRecordsOffset ingests a batch of records into the stream and returns one offset for the entire batch.
// This is an optimized API for ingesting multiple records at once.
// This method returns as soon as the batch is queued; the server round-trip
// happens in the background.
//
// Prefer this batch API over single-record calls in hot paths. The idiomatic
// flow is to ingest your batches in a loop and call Flush() to confirm
// durability. Use WaitForOffset() with a returned offset when you need to
// confirm a specific batch before continuing (acks are ordered, so the last
// offset confirms the whole group); prefer Flush() for bulk durability. Avoid
// calling WaitForOffset() after every batch in a tight loop, since that limits
// throughput to one batch per round-trip.
//
// The records parameter accepts a slice where each element is either:
//   - []byte for Protocol Buffer encoded records
//   - string for JSON encoded records
//
// All records in the batch must be of the same type (all protobuf or all JSON).
//
// Returns:
//   - int64: One offset that represents the entire batch
//   - error: Any error that occurred during ingestion
//
// If the batch is empty, returns -1 with no error.
//
// Example:
//
//	// Ingest a batch of JSON records
//	records := []interface{}{
//	    `{"field": "value1"}`,
//	    `{"field": "value2"}`,
//	    `{"field": "value3"}`,
//	}
//	batchOffset, err := stream.IngestRecordsOffset(records)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	if err := stream.Flush(); err != nil {
//	    log.Fatal(err)
//	}
//	log.Printf("Batch ingested with offset: %d", batchOffset)
func (st *ZerobusStream) IngestRecordsOffset(records []interface{}) (int64, error) {
	if st.ptr == nil {
		return -1, &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}

	if len(records) == 0 {
		return -1, nil
	}

	// Determine the type from the first record
	switch records[0].(type) {
	case []byte:
		// Convert to [][]byte
		byteRecords := make([][]byte, len(records))
		for i, r := range records {
			b, ok := r.([]byte)
			if !ok {
				return -1, &ZerobusError{
					Message:     "All records in batch must be of the same type ([]byte)",
					IsRetryable: false,
				}
			}
			byteRecords[i] = b
		}
		return streamIngestProtoRecords(st.ptr, byteRecords)

	case string:
		// Convert to []string
		stringRecords := make([]string, len(records))
		for i, r := range records {
			s, ok := r.(string)
			if !ok {
				return -1, &ZerobusError{
					Message:     "All records in batch must be of the same type (string)",
					IsRetryable: false,
				}
			}
			stringRecords[i] = s
		}
		return streamIngestJSONRecords(st.ptr, stringRecords)

	default:
		return -1, &ZerobusError{
			Message:     "Invalid payload type: must be []byte or string",
			IsRetryable: false,
		}
	}
}

// WaitForOffset blocks until the server acknowledges the record at the specified offset.
// This allows explicit control over when to wait for acknowledgments.
//
// Use this with offsets returned from IngestRecordOffset() to confirm a specific
// record before continuing, without waiting for all pending records (unlike Flush).
// Acks are ordered, so waiting on the last offset of a group confirms all prior
// offsets too.
//
// Use this when you need to confirm a specific record; prefer Flush() for bulk
// durability (ingest in a loop, then Flush() once). Avoid calling WaitForOffset()
// after every record in a tight loop, since that limits throughput to one record
// per round-trip.
//
// Example:
//
//	// Confirm a group of records with a single wait on the last offset.
//	var last int64
//	for _, r := range records {
//	    last, _ = stream.IngestRecordOffset(r)
//	}
//	if err := stream.WaitForOffset(last); err != nil { // confirms all prior offsets too
//	    log.Printf("Record at offset %d failed: %v", last, err)
//	}
func (st *ZerobusStream) WaitForOffset(offset int64) error {
	if st.ptr == nil {
		return &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}

	return streamWaitForOffset(st.ptr, offset)
}

// GetUnackedRecords retrieves all records that have not yet been acknowledged by the server.
//
// IMPORTANT: Call this on a failed stream before Close(). Close() nils the
// handle and frees native resources, so a later GetUnackedRecords() call fails.
//
// Use this method to:
//   - Retrieve unacknowledged records after stream failure for retry logic
//   - Inspect payloads that were queued but not acked, before Close()
//   - Implement custom retry strategies after stream errors
//
// Returns a slice where each element is either:
//   - []byte for Protocol Buffer encoded records
//   - string for JSON encoded records
//
// Returns an empty slice if there are no unacknowledged records.
//
// Example:
//
//	if err := stream.Flush(); err != nil {
//	    unacked, err := stream.GetUnackedRecords()
//	    if err != nil {
//	        log.Printf("could not inspect unacked records: %v", err)
//	    } else {
//	        log.Printf("Failed to acknowledge %d records", len(unacked))
//	    }
//	}
//	_ = stream.Close()
func (st *ZerobusStream) GetUnackedRecords() ([]interface{}, error) {
	if st.ptr == nil {
		return nil, &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}

	return streamGetUnackedRecords(st.ptr)
}

// Flush blocks until all pending records have been acknowledged by the server.
// This ensures durability guarantees before proceeding.
//
// This is the idiomatic way to confirm durability for high-throughput ingestion:
// ingest many records via IngestRecordOffset()/IngestRecordsOffset() in a loop,
// then call Flush() once. Use WaitForOffset() instead when you only need to
// confirm a specific record rather than everything queued so far.
//
// Returns an error if:
//   - Flush timeout is exceeded
//   - Any record fails with a non-retryable error
//
// Example:
//
//	for _, r := range records {
//	    if _, err := stream.IngestRecordOffset(r); err != nil {
//	        log.Printf("Ingest failed: %v", err)
//	    }
//	}
//	if err := stream.Flush(); err != nil {
//	    log.Printf("Flush failed: %v", err)
//	}
func (st *ZerobusStream) Flush() error {
	if st.ptr == nil {
		return &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}

	return streamFlush(st.ptr)
}

// Close gracefully closes the stream after flushing all pending records.
// This method ensures all records are durably stored before closing the connection.
//
// The stream cannot be used after calling Close().
// Note: This is automatically called by the garbage collector, but should be called explicitly
// when done with the stream to ensure timely resource cleanup and proper error handling.
//
// Returns an error if:
//   - Flush fails
//   - Unable to close the gRPC connection
//
// Example:
//
//	defer stream.Close()
func (st *ZerobusStream) Close() error {
	if st.ptr == nil {
		return nil // Already closed
	}

	// Always free resources, even if close fails
	// The FFI layer now properly cleans up pending acks and aborts background tasks
	ptr := st.ptr
	st.ptr = nil // Mark as closed immediately to prevent double-close

	err := streamClose(ptr)
	streamFree(ptr) // Always free to prevent resource leaks

	return err
}
