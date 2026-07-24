package tests

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	zerobus "github.com/databricks/zerobus-sdk/go"
	"google.golang.org/grpc/codes"
)

func TestMain(m *testing.M) {
	// Ignore SIGPIPE to prevent the Rust FFI layer from crashing the
	// process when the mock server writes to a connection that the
	// client already closed (e.g. after a timeout).
	signal.Ignore(syscall.SIGPIPE)
	os.Exit(m.Run())
}

const testTableName = "test_catalog.test_schema.test_table"

// TestSuccessfulStreamCreation tests that a stream can be created successfully
func TestSuccessfulStreamCreation(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	// Inject responses before creating the stream
	mockServer.InjectResponses(testTableName, []MockResponse{
		CreateStreamResponse("test_stream_1", 0),
	})

	// Give the server time to be fully ready
	time.Sleep(200 * time.Millisecond)

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	tableProps := zerobus.TableProperties{
		TableName:       testTableName,
		DescriptorProto: CreateTestDescriptorProto(),
	}

	options := &zerobus.StreamConfigurationOptions{
		MaxInflightRequests: 100,
		Recovery:            false,
	}

	headersProvider := &TestHeadersProvider{}

	stream, err := sdk.CreateStreamWithHeadersProvider(tableProps, headersProvider, options)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Close()

	t.Log("✓ Stream created successfully")
}

// TestTimeoutedStreamCreation tests that stream creation times out when the server is slow
func TestTimeoutedStreamCreation(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	// Inject a response with 300ms delay
	mockServer.InjectResponses(testTableName, []MockResponse{
		CreateStreamResponse("test_stream_1", 300),
	})

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	tableProps := zerobus.TableProperties{
		TableName:       testTableName,
		DescriptorProto: CreateTestDescriptorProto(),
	}

	// Set a timeout shorter than the response delay
	options := &zerobus.StreamConfigurationOptions{
		MaxInflightRequests: 100,
		RecoveryTimeoutMs:   100,
		Recovery:            false,
	}

	headersProvider := &TestHeadersProvider{}

	_, err = sdk.CreateStreamWithHeadersProvider(tableProps, headersProvider, options)
	if err == nil {
		t.Fatal("Expected stream creation to fail due to timeout, but it succeeded")
	}

	// Wait for the mock server's delayed response to complete before
	// grpcServer.Stop() tears down the connection.
	time.Sleep(500 * time.Millisecond)
}

// TestNonRetriableErrorDuringStreamCreation tests that non-retriable errors fail immediately
func TestNonRetriableErrorDuringStreamCreation(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	// A non-auth non-retriable error (auth rejections may get one initial-setup
	// retry; a permanent error like InvalidArgument must still fail at once).
	mockServer.InjectResponses(testTableName, []MockResponse{
		ErrorResponse(codes.InvalidArgument, "Non-retriable error", 0),
	})

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	tableProps := zerobus.TableProperties{
		TableName:       testTableName,
		DescriptorProto: CreateTestDescriptorProto(),
	}

	options := &zerobus.StreamConfigurationOptions{
		MaxInflightRequests: 100,
		Recovery:            true,
	}

	headersProvider := &TestHeadersProvider{}

	_, err = sdk.CreateStreamWithHeadersProvider(tableProps, headersProvider, options)
	if err == nil {
		t.Fatal("Expected stream creation to fail, but it succeeded")
	}
}

// TestInitialAuthRejectionRetriesOnceThenSucceeds verifies that a single
// Unauthenticated response during initial setup consumes one recovery retry and
// the stream is then created.
func TestInitialAuthRejectionRetriesOnceThenSucceeds(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	mockServer.InjectResponses(testTableName, []MockResponse{
		ErrorResponse(codes.Unauthenticated, "initial authentication rejected", 0),
		CreateStreamResponse("test_stream_1", 0),
	})

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	tableProps := zerobus.TableProperties{
		TableName:       testTableName,
		DescriptorProto: CreateTestDescriptorProto(),
	}

	options := &zerobus.StreamConfigurationOptions{
		MaxInflightRequests: 100,
		Recovery:            true,
		RecoveryBackoffMs:   0,
		RecoveryRetries:     1,
	}

	headersProvider := &TestHeadersProvider{}

	stream, err := sdk.CreateStreamWithHeadersProvider(tableProps, headersProvider, options)
	if err != nil {
		t.Fatalf("Expected stream creation to succeed after one retry, got: %v", err)
	}
	defer stream.Close()
}

// TestRepeatedInitialAuthRejectionIsTerminal verifies that a second authentication
// rejection during initial setup is terminal: only one setup retry is allowed.
func TestRepeatedInitialAuthRejectionIsTerminal(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	mockServer.InjectResponses(testTableName, []MockResponse{
		ErrorResponse(codes.Unauthenticated, "initial authentication rejected", 0),
		ErrorResponse(codes.PermissionDenied, "second authentication rejected", 0),
		// A regression that retries auth failures repeatedly would consume this
		// third response instead of surfacing the second rejection.
		CreateStreamResponse("unexpected_third_attempt", 0),
	})

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	tableProps := zerobus.TableProperties{
		TableName:       testTableName,
		DescriptorProto: CreateTestDescriptorProto(),
	}

	options := &zerobus.StreamConfigurationOptions{
		MaxInflightRequests: 100,
		Recovery:            true,
		RecoveryBackoffMs:   0,
		RecoveryRetries:     5,
	}

	headersProvider := &TestHeadersProvider{}

	_, err = sdk.CreateStreamWithHeadersProvider(tableProps, headersProvider, options)
	if err == nil {
		t.Fatal("Expected the second auth rejection to terminate stream creation")
	}
}

// TestRetriableErrorWithoutRecoveryDuringStreamCreation tests that retriable errors fail without recovery
func TestRetriableErrorWithoutRecoveryDuringStreamCreation(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	mockServer.InjectResponses(testTableName, []MockResponse{
		ErrorResponse(codes.Unavailable, "Retriable error", 0),
	})

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	tableProps := zerobus.TableProperties{
		TableName:       testTableName,
		DescriptorProto: CreateTestDescriptorProto(),
	}

	options := &zerobus.StreamConfigurationOptions{
		MaxInflightRequests: 100,
		Recovery:            false,
		RecoveryTimeoutMs:   200,
		RecoveryBackoffMs:   200,
	}

	headersProvider := &TestHeadersProvider{}

	startTime := time.Now()
	_, err = sdk.CreateStreamWithHeadersProvider(tableProps, headersProvider, options)
	duration := time.Since(startTime)

	if err == nil {
		t.Fatal("Expected stream creation to fail, but it succeeded")
	}

	// Should fail quickly without retry
	if duration.Milliseconds() > 300 {
		t.Fatalf("Expected quick failure, but took %v", duration)
	}
}

// TestGracefulClose tests that a stream can be closed gracefully
func TestGracefulClose(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	mockServer.InjectResponses(testTableName, []MockResponse{
		CreateStreamResponse("test_stream_1", 0),
		RecordAckResponse(0, 100),
	})

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	tableProps := zerobus.TableProperties{
		TableName:       testTableName,
		DescriptorProto: CreateTestDescriptorProto(),
	}

	options := &zerobus.StreamConfigurationOptions{
		MaxInflightRequests: 100,
		Recovery:            false,
	}

	headersProvider := &TestHeadersProvider{}

	stream, err := sdk.CreateStreamWithHeadersProvider(tableProps, headersProvider, options)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	testRecord := []byte("test record data")
	offsetID, err := stream.IngestRecordOffset(testRecord)
	if err != nil {
		t.Fatalf("Failed to ingest record: %v", err)
	}

	if offsetID != 0 {
		t.Fatalf("Expected offset 0, got %d", offsetID)
	}

	err = stream.Close()
	if err != nil {
		t.Fatalf("Failed to close stream: %v", err)
	}

	writeCount := mockServer.GetWriteCount()
	maxOffset := mockServer.GetMaxOffsetSent()

	if writeCount != 1 {
		t.Fatalf("Expected write count 1, got %d", writeCount)
	}

	if maxOffset != 0 {
		t.Fatalf("Expected max offset 0, got %d", maxOffset)
	}

	t.Logf("✓ Successfully ingested record (offset=%d, writeCount=%d)", maxOffset, writeCount)
}

// TestIdempotentClose tests that close can be called multiple times
func TestIdempotentClose(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	mockServer.InjectResponses(testTableName, []MockResponse{
		CreateStreamResponse("test_stream_1", 0),
	})

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	tableProps := zerobus.TableProperties{
		TableName:       testTableName,
		DescriptorProto: CreateTestDescriptorProto(),
	}

	options := &zerobus.StreamConfigurationOptions{
		MaxInflightRequests: 100,
		Recovery:            false,
	}

	headersProvider := &TestHeadersProvider{}

	stream, err := sdk.CreateStreamWithHeadersProvider(tableProps, headersProvider, options)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	err = stream.Close()
	if err != nil {
		t.Fatalf("First close failed: %v", err)
	}

	err = stream.Close()
	if err != nil {
		t.Fatalf("Second close failed: %v", err)
	}
}

// TestIngestAfterClose tests that ingesting after close returns an error
func TestIngestAfterClose(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	mockServer.InjectResponses(testTableName, []MockResponse{
		CreateStreamResponse("test_stream_1", 0),
	})

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	tableProps := zerobus.TableProperties{
		TableName:       testTableName,
		DescriptorProto: CreateTestDescriptorProto(),
	}

	options := &zerobus.StreamConfigurationOptions{
		MaxInflightRequests: 100,
		Recovery:            false,
	}

	headersProvider := &TestHeadersProvider{}

	stream, err := sdk.CreateStreamWithHeadersProvider(tableProps, headersProvider, options)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	err = stream.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	_, err = stream.IngestRecordOffset([]byte("test record data"))
	if err == nil {
		t.Fatal("Expected ingest after close to fail, but it succeeded")
	}
}

// TestIngestSingleRecord tests ingesting a single record and receiving acknowledgment
func TestIngestSingleRecord(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	mockServer.InjectResponses(testTableName, []MockResponse{
		CreateStreamResponse("test_stream_1", 0),
		RecordAckResponse(0, 0), // Ack for offset 0
	})

	time.Sleep(200 * time.Millisecond)

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	tableProps := zerobus.TableProperties{
		TableName:       testTableName,
		DescriptorProto: CreateTestDescriptorProto(),
	}

	options := &zerobus.StreamConfigurationOptions{
		MaxInflightRequests: 100,
		Recovery:            false,
	}

	headersProvider := &TestHeadersProvider{}

	stream, err := sdk.CreateStreamWithHeadersProvider(tableProps, headersProvider, options)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Close()

	// Ingest a record
	testRecord := []byte("test record data")
	offsetID, err := stream.IngestRecordOffset(testRecord)
	if err != nil {
		t.Fatalf("Failed to ingest record: %v", err)
	}

	if offsetID != 0 {
		t.Fatalf("Expected offset 0, got %d", offsetID)
	}

	// Give the server time to process
	time.Sleep(100 * time.Millisecond)

	// Verify the mock server received it
	writeCount := mockServer.GetWriteCount()
	maxOffset := mockServer.GetMaxOffsetSent()

	if writeCount != 1 {
		t.Errorf("Expected write count 1, got %d", writeCount)
	}

	if maxOffset != 0 {
		t.Errorf("Expected max offset 0, got %d", maxOffset)
	}

	t.Logf("✓ Successfully ingested record at offset %d", offsetID)
}

// TestIngestMultipleRecords tests ingesting multiple records and receiving acknowledgments
func TestIngestMultipleRecords(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	mockServer.InjectResponses(testTableName, []MockResponse{
		CreateStreamResponse("test_stream_1", 0),
		RecordAckResponse(0, 0), // Ack for offset 0
		RecordAckResponse(1, 0), // Ack for offset 1
		RecordAckResponse(2, 0), // Ack for offset 2
	})

	time.Sleep(200 * time.Millisecond)

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	tableProps := zerobus.TableProperties{
		TableName:       testTableName,
		DescriptorProto: CreateTestDescriptorProto(),
	}

	options := &zerobus.StreamConfigurationOptions{
		MaxInflightRequests: 100,
		Recovery:            false,
	}

	headersProvider := &TestHeadersProvider{}

	stream, err := sdk.CreateStreamWithHeadersProvider(tableProps, headersProvider, options)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Close()

	// Ingest multiple records
	numRecords := 3
	for i := 0; i < numRecords; i++ {
		testRecord := []byte(fmt.Sprintf("test record %d", i))
		offsetID, err := stream.IngestRecordOffset(testRecord)
		if err != nil {
			t.Fatalf("Failed to ingest record %d: %v", i, err)
		}

		if offsetID != int64(i) {
			t.Fatalf("Expected offset %d, got %d", i, offsetID)
		}

		t.Logf("✓ Ingested record %d at offset %d", i, offsetID)
	}

	// Give the server time to process
	time.Sleep(100 * time.Millisecond)

	// Verify the mock server received all records
	writeCount := mockServer.GetWriteCount()
	maxOffset := mockServer.GetMaxOffsetSent()

	if writeCount != uint64(numRecords) {
		t.Errorf("Expected write count %d, got %d", numRecords, writeCount)
	}

	if maxOffset != int64(numRecords-1) {
		t.Errorf("Expected max offset %d, got %d", numRecords-1, maxOffset)
	}

	t.Logf("✓ Successfully ingested %d records", numRecords)
}

// TestIngestBatchRecords tests ingesting a batch of records
func TestIngestBatchRecords(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	mockServer.InjectResponses(testTableName, []MockResponse{
		CreateStreamResponse("test_stream_1", 0),
		RecordAckResponse(0, 0), // Ack for the batch at offset 0
	})

	time.Sleep(200 * time.Millisecond)

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	tableProps := zerobus.TableProperties{
		TableName:       testTableName,
		DescriptorProto: CreateTestDescriptorProto(),
	}

	options := &zerobus.StreamConfigurationOptions{
		MaxInflightRequests: 100,
		Recovery:            false,
	}

	headersProvider := &TestHeadersProvider{}

	stream, err := sdk.CreateStreamWithHeadersProvider(tableProps, headersProvider, options)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Close()

	// Create a batch of records
	batch := []interface{}{
		[]byte("record 1"),
		[]byte("record 2"),
		[]byte("record 3"),
		[]byte("record 4"),
		[]byte("record 5"),
	}

	// Ingest the batch
	offsetID, err := stream.IngestRecordsOffset(batch)
	if err != nil {
		t.Fatalf("Failed to ingest batch: %v", err)
	}

	if offsetID != 0 {
		t.Fatalf("Expected offset 0, got %d", offsetID)
	}

	// Give the server time to process
	time.Sleep(100 * time.Millisecond)

	// Verify the mock server received all records
	writeCount := mockServer.GetWriteCount()
	maxOffset := mockServer.GetMaxOffsetSent()

	if writeCount != uint64(len(batch)) {
		t.Errorf("Expected write count %d, got %d", len(batch), writeCount)
	}

	if maxOffset != 0 {
		t.Errorf("Expected max offset 0, got %d", maxOffset)
	}

	t.Logf("✓ Successfully ingested batch of %d records at offset %d", len(batch), offsetID)
}

// TestIngestRecordNowait tests that IngestRecordNowait returns immediately and the
// record is eventually delivered to the server.
func TestIngestRecordNowait(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	mockServer.InjectResponses(testTableName, []MockResponse{
		CreateStreamResponse("test_stream_nowait", 0),
		RecordAckResponse(0, 0),
	})

	time.Sleep(200 * time.Millisecond)

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	tableProps := zerobus.TableProperties{
		TableName:       testTableName,
		DescriptorProto: CreateTestDescriptorProto(),
	}

	options := &zerobus.StreamConfigurationOptions{
		MaxInflightRequests: 100,
		Recovery:            false,
	}

	stream, err := sdk.CreateStreamWithHeadersProvider(tableProps, &TestHeadersProvider{}, options)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Close()

	err = stream.IngestRecordNowait([]byte("nowait proto record"))
	if err != nil {
		t.Fatalf("IngestRecordNowait returned unexpected error: %v", err)
	}

	// Give the background task time to queue and the server time to receive the record.
	time.Sleep(300 * time.Millisecond)

	if writeCount := mockServer.GetWriteCount(); writeCount != 1 {
		t.Errorf("Expected write count 1, got %d", writeCount)
	}

	t.Log("✓ IngestRecordNowait: record delivered to server")
}

// TestIngestRecordNowaitJSON tests IngestRecordNowait with a JSON payload.
func TestIngestRecordNowaitJSON(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	mockServer.InjectResponses(testTableName, []MockResponse{
		CreateStreamResponse("test_stream_nowait_json", 0),
		RecordAckResponse(0, 0),
	})

	time.Sleep(200 * time.Millisecond)

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	tableProps := zerobus.TableProperties{
		TableName:       testTableName,
		DescriptorProto: CreateTestDescriptorProto(),
	}

	options := &zerobus.StreamConfigurationOptions{
		MaxInflightRequests: 100,
		Recovery:            false,
		RecordType:          zerobus.RecordTypeJson,
	}

	stream, err := sdk.CreateStreamWithHeadersProvider(tableProps, &TestHeadersProvider{}, options)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Close()

	err = stream.IngestRecordNowait(`{"id": 1, "message": "hello"}`)
	if err != nil {
		t.Fatalf("IngestRecordNowait (JSON) returned unexpected error: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	if writeCount := mockServer.GetWriteCount(); writeCount != 1 {
		t.Errorf("Expected write count 1, got %d", writeCount)
	}

	t.Log("✓ IngestRecordNowait (JSON): record delivered to server")
}

// TestIngestRecordNowaitInvalidPayload tests that IngestRecordNowait rejects invalid payload types.
func TestIngestRecordNowaitInvalidPayload(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	mockServer.InjectResponses(testTableName, []MockResponse{
		CreateStreamResponse("test_stream_nowait_invalid", 0),
	})

	time.Sleep(200 * time.Millisecond)

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	tableProps := zerobus.TableProperties{
		TableName:       testTableName,
		DescriptorProto: CreateTestDescriptorProto(),
	}

	options := &zerobus.StreamConfigurationOptions{
		MaxInflightRequests: 100,
		Recovery:            false,
	}

	stream, err := sdk.CreateStreamWithHeadersProvider(tableProps, &TestHeadersProvider{}, options)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Close()

	err = stream.IngestRecordNowait(12345) // invalid type
	if err == nil {
		t.Fatal("Expected error for invalid payload type, got nil")
	}

	t.Logf("✓ IngestRecordNowait correctly rejected invalid payload: %v", err)
}

// TestIngestRecordsAfterClose tests that batch ingesting after close returns an error
func TestIngestRecordsAfterClose(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	mockServer.InjectResponses(testTableName, []MockResponse{
		CreateStreamResponse("test_stream_batch_after_close", 0),
	})

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	tableProps := zerobus.TableProperties{
		TableName:       testTableName,
		DescriptorProto: CreateTestDescriptorProto(),
	}

	options := &zerobus.StreamConfigurationOptions{
		MaxInflightRequests: 100,
		Recovery:            false,
	}

	headersProvider := &TestHeadersProvider{}

	stream, err := sdk.CreateStreamWithHeadersProvider(tableProps, headersProvider, options)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	err = stream.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	batch := []interface{}{[]byte("record 1"), []byte("record 2")}
	_, err = stream.IngestRecordsOffset(batch)
	if err == nil {
		t.Fatal("Expected ingest after close to fail, but it succeeded")
	}
}
