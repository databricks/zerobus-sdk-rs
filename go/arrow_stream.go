package zerobus

// Experimental/Unsupported: Arrow Flight ingestion is experimental and not yet
// supported for production use. The API may change in future releases.
//
// Arrow Flight support lets you ingest Apache Arrow RecordBatches directly into
// Databricks Delta tables using the high-performance Arrow Flight protocol.
//
// See examples/arrow/ for a complete working example.

import (
	"runtime"
	"unsafe"
)

// Arrow IPC compression options for ArrowStreamConfigurationOptions.IPCCompression.
const (
	// IPCCompressionNone disables Arrow IPC compression (default).
	IPCCompressionNone int32 = -1
	// IPCCompressionLZ4Frame enables LZ4 frame compression.
	IPCCompressionLZ4Frame int32 = 0
	// IPCCompressionZstd enables Zstandard compression.
	IPCCompressionZstd int32 = 1
)

// ArrowStreamConfigurationOptions holds configuration for an Arrow Flight stream.
type ArrowStreamConfigurationOptions struct {
	// Maximum number of batches in-flight (pending acknowledgment). Default: 1,000
	MaxInflightBatches uint64
	// Enable automatic stream recovery on retryable failures. Default: true
	Recovery bool
	// Timeout per recovery attempt in milliseconds. Default: 15,000
	RecoveryTimeoutMs uint64
	// Backoff between recovery attempts in milliseconds. Default: 2,000
	RecoveryBackoffMs uint64
	// Maximum recovery retry attempts. Default: 4
	RecoveryRetries uint32
	// Server acknowledgment timeout in milliseconds. Default: 60,000
	ServerLackOfAckTimeoutMs uint64
	// Flush timeout in milliseconds. Default: 300,000
	FlushTimeoutMs uint64
	// Connection establishment timeout in milliseconds. Default: 30,000
	ConnectionTimeoutMs uint64
	// Arrow IPC compression codec. Default: IPCCompressionNone
	IPCCompression int32
}

// DefaultArrowStreamConfigurationOptions returns default configuration for Arrow streams.
func DefaultArrowStreamConfigurationOptions() *ArrowStreamConfigurationOptions {
	return &ArrowStreamConfigurationOptions{
		MaxInflightBatches:       1_000,
		Recovery:                 true,
		RecoveryTimeoutMs:        15_000,
		RecoveryBackoffMs:        2_000,
		RecoveryRetries:          4,
		ServerLackOfAckTimeoutMs: 60_000,
		FlushTimeoutMs:           300_000,
		ConnectionTimeoutMs:      30_000,
		IPCCompression:           IPCCompressionNone,
	}
}

// ZerobusArrowStream is an active Arrow Flight stream for ingesting Arrow RecordBatches.
// Batches are supplied as Arrow IPC stream bytes produced by the Apache Arrow Go library's ipc.Writer.
type ZerobusArrowStream struct {
	ptr unsafe.Pointer
}

// CreateArrowStream creates an Arrow Flight stream authenticated with OAuth client credentials.
//
// schemaIpcBytes must be Arrow IPC stream bytes containing only the schema (no data batches).
// Produce these with ipc.NewWriter followed by Close without writing any batches.
func (s *ZerobusSdk) CreateArrowStream(
	tableName string,
	schemaIpcBytes []byte,
	clientID string,
	clientSecret string,
	options *ArrowStreamConfigurationOptions,
) (*ZerobusArrowStream, error) {
	if s.ptr == nil {
		return nil, &ZerobusError{Message: "SDK has been freed", IsRetryable: false}
	}
	if len(schemaIpcBytes) == 0 {
		return nil, &ZerobusError{Message: "schemaIpcBytes must not be empty", IsRetryable: false}
	}

	ptr, err := sdkCreateArrowStream(s.ptr, tableName, schemaIpcBytes, clientID, clientSecret, options)
	if err != nil {
		return nil, err
	}

	stream := &ZerobusArrowStream{ptr: ptr}
	runtime.SetFinalizer(stream, func(st *ZerobusArrowStream) {
		st.Close() //nolint:errcheck
	})
	return stream, nil
}

// CreateArrowStreamWithHeadersProvider creates an Arrow Flight stream with a custom
// headers provider for authentication.
func (s *ZerobusSdk) CreateArrowStreamWithHeadersProvider(
	tableName string,
	schemaIpcBytes []byte,
	headersProvider HeadersProvider,
	options *ArrowStreamConfigurationOptions,
) (*ZerobusArrowStream, error) {
	if s.ptr == nil {
		return nil, &ZerobusError{Message: "SDK has been freed", IsRetryable: false}
	}
	if len(schemaIpcBytes) == 0 {
		return nil, &ZerobusError{Message: "schemaIpcBytes must not be empty", IsRetryable: false}
	}

	ptr, err := sdkCreateArrowStreamWithHeadersProvider(s.ptr, tableName, schemaIpcBytes, headersProvider, options)
	if err != nil {
		return nil, err
	}

	stream := &ZerobusArrowStream{ptr: ptr}
	runtime.SetFinalizer(stream, func(st *ZerobusArrowStream) {
		st.Close() //nolint:errcheck
	})
	return stream, nil
}

// IngestBatch queues one Arrow RecordBatch for ingestion and returns its logical offset.
// ipcBytes must be Arrow IPC stream bytes containing exactly one RecordBatch.
// Use WaitForOffset or Flush to wait for server acknowledgment.
func (st *ZerobusArrowStream) IngestBatch(ipcBytes []byte) (int64, error) {
	if st.ptr == nil {
		return -1, &ZerobusError{Message: "Arrow stream has been closed", IsRetryable: false}
	}
	return arrowStreamIngestBatch(st.ptr, ipcBytes)
}

// WaitForOffset blocks until the server acknowledges the batch at the given offset.
func (st *ZerobusArrowStream) WaitForOffset(offset int64) error {
	if st.ptr == nil {
		return &ZerobusError{Message: "Arrow stream has been closed", IsRetryable: false}
	}
	return arrowStreamWaitForOffset(st.ptr, offset)
}

// Flush waits until all pending batches have been acknowledged by the server.
func (st *ZerobusArrowStream) Flush() error {
	if st.ptr == nil {
		return &ZerobusError{Message: "Arrow stream has been closed", IsRetryable: false}
	}
	return arrowStreamFlush(st.ptr)
}

// Close gracefully shuts down the stream after flushing all pending batches.
// The stream must not be used after Close returns. Close is idempotent.
func (st *ZerobusArrowStream) Close() error {
	if st.ptr == nil {
		return nil
	}
	ptr := st.ptr
	st.ptr = nil

	err := arrowStreamClose(ptr)
	arrowStreamFree(ptr)
	return err
}

// GetUnackedBatches returns all unacknowledged batches as Arrow IPC bytes.
// Each []byte is a self-contained IPC stream (schema + one RecordBatch) that can
// be re-ingested into a new stream. Only call after the stream has closed or failed.
func (st *ZerobusArrowStream) GetUnackedBatches() ([][]byte, error) {
	if st.ptr == nil {
		return nil, &ZerobusError{Message: "Arrow stream has been closed", IsRetryable: false}
	}
	return arrowStreamGetUnackedBatches(st.ptr)
}

// IsClosed reports whether the stream has been closed or failed.
func (st *ZerobusArrowStream) IsClosed() bool {
	if st.ptr == nil {
		return true
	}
	return arrowStreamIsClosed(st.ptr)
}
