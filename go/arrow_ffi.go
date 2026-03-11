package zerobus

/*
#cgo linux,amd64 LDFLAGS: ${SRCDIR}/lib/linux_amd64/libzerobus_ffi.a -ldl -lpthread -lm -lresolv -lgcc_s
#cgo linux,arm64 LDFLAGS: ${SRCDIR}/lib/linux_arm64/libzerobus_ffi.a -ldl -lpthread -lm -lresolv -lgcc_s
#cgo darwin,amd64 LDFLAGS: ${SRCDIR}/lib/darwin_amd64/libzerobus_ffi.a -framework CoreFoundation -framework Security -liconv
#cgo darwin,arm64 LDFLAGS: ${SRCDIR}/lib/darwin_arm64/libzerobus_ffi.a -framework CoreFoundation -framework Security -liconv
#cgo windows,amd64 LDFLAGS: ${SRCDIR}/lib/windows_amd64/libzerobus_ffi.a -lws2_32 -luserenv -lbcrypt -lntdll
#cgo CFLAGS: -I${SRCDIR}/../rust/ffi -Wno-implicit-function-declaration

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>

// Opaque SDK handle (forward declaration matches ffi.go's translation unit).
typedef struct CZerobusSdk { uint8_t _private[0]; } CZerobusSdk;

// Result type (identical layout to CResult in ffi.go).
typedef struct CResult {
    bool success;
    char *error_message;
    bool is_retryable;
} CResult;

// Header types used by the headers-provider callback (same as ffi.go).
typedef struct CHeader {
    char *key;
    char *value;
} CHeader;

typedef struct CHeaders {
    CHeader *headers;
    uintptr_t count;
    char *error_message;
} CHeaders;

typedef CHeaders (*HeadersProviderCallback)(void *user_data);

// Arrow stream opaque handle.
typedef struct CArrowStream { uint8_t _private[0]; } CArrowStream;

// Arrow stream configuration options.
// ipc_compression: -1 = None, 0 = LZ4_FRAME, 1 = ZSTD
typedef struct CArrowStreamConfigurationOptions {
    uintptr_t max_inflight_batches;
    bool      recovery;
    uint64_t  recovery_timeout_ms;
    uint64_t  recovery_backoff_ms;
    uint32_t  recovery_retries;
    uint64_t  server_lack_of_ack_timeout_ms;
    uint64_t  flush_timeout_ms;
    uint64_t  connection_timeout_ms;
    int32_t   ipc_compression;
} CArrowStreamConfigurationOptions;

// Array of Arrow IPC-encoded batches returned by get_unacked_batches.
typedef struct CArrowBatchArray {
    uint8_t  **batches;
    uintptr_t *lengths;
    uintptr_t  count;
} CArrowBatchArray;

// Forward declare the Go-exported headers callback (defined in ffi.go).
extern void goGetHeaders(void *userData, CHeader **headers, uintptr_t *count, char **error);

// C callback wrapper for arrow streams — calls the same Go function as the
// regular stream callback. We define it here as a static so it exists in this
// translation unit without conflicting with ffi.go's cHeadersCallback.
static CHeaders cArrowHeadersCallback(void *userData) {
    CHeader   *headers = NULL;
    uintptr_t  count   = 0;
    char      *error   = NULL;
    goGetHeaders(userData, &headers, &count, &error);
    CHeaders result;
    result.headers       = headers;
    result.count         = count;
    result.error_message = error;
    return result;
}

static HeadersProviderCallback getArrowHeadersCallback() {
    return (HeadersProviderCallback)cArrowHeadersCallback;
}

// Arrow FFI function declarations.
extern CArrowStream *zerobus_sdk_create_arrow_stream(
    CZerobusSdk *sdk,
    const char *table_name,
    const uint8_t *schema_ipc_bytes,
    uintptr_t schema_ipc_len,
    const char *client_id,
    const char *client_secret,
    const CArrowStreamConfigurationOptions *options,
    CResult *result);

extern CArrowStream *zerobus_sdk_create_arrow_stream_with_headers_provider(
    CZerobusSdk *sdk,
    const char *table_name,
    const uint8_t *schema_ipc_bytes,
    uintptr_t schema_ipc_len,
    HeadersProviderCallback headers_callback,
    void *user_data,
    const CArrowStreamConfigurationOptions *options,
    CResult *result);

extern void    zerobus_arrow_stream_free(CArrowStream *stream);
extern int64_t zerobus_arrow_stream_ingest_batch(CArrowStream *stream, const uint8_t *ipc_bytes, uintptr_t ipc_len, CResult *result);
extern bool    zerobus_arrow_stream_wait_for_offset(CArrowStream *stream, int64_t offset, CResult *result);
extern bool    zerobus_arrow_stream_flush(CArrowStream *stream, CResult *result);
extern bool    zerobus_arrow_stream_close(CArrowStream *stream, CResult *result);
extern CArrowBatchArray zerobus_arrow_stream_get_unacked_batches(CArrowStream *stream, CResult *result);
extern void    zerobus_arrow_free_batch_array(CArrowBatchArray array);
extern bool    zerobus_arrow_stream_is_closed(CArrowStream *stream);
extern CArrowStreamConfigurationOptions zerobus_arrow_get_default_config(void);

// Shared cleanup helper (from ffi.go's translation unit).
extern void zerobus_free_error_message(char *message);
*/
import "C"
import (
	"runtime"
	"runtime/cgo"
	"sync"
	"unsafe"
)

// arrowStreamHandleRegistry holds cgo.Handles for arrow streams created with a
// custom headers provider so the Go object stays alive for the stream lifetime.
var (
	arrowStreamHandleRegistry   = make(map[unsafe.Pointer]cgo.Handle)
	arrowStreamHandleRegistryMu sync.Mutex
)

// arrowFfiResult converts a C.CResult (from this translation unit) to a Go error.
func arrowFfiResult(cres C.CResult) error {
	if cres.success {
		return nil
	}
	var message string
	if cres.error_message != nil {
		message = C.GoString(cres.error_message)
		C.zerobus_free_error_message(cres.error_message)
	} else {
		message = "unknown error"
	}
	return &ZerobusError{
		Message:     message,
		IsRetryable: bool(cres.is_retryable),
	}
}

// convertArrowConfigToC converts Go arrow config to the C representation.
// Zero-valued fields fall back to defaults.
func convertArrowConfigToC(opts *ArrowStreamConfigurationOptions) C.CArrowStreamConfigurationOptions {
	if opts == nil {
		return C.zerobus_arrow_get_default_config()
	}
	d := DefaultArrowStreamConfigurationOptions()

	maxInflight := opts.MaxInflightBatches
	if maxInflight == 0 {
		maxInflight = d.MaxInflightBatches
	}
	recovery := opts.Recovery
	if opts.RecoveryTimeoutMs == 0 && opts.RecoveryBackoffMs == 0 && opts.RecoveryRetries == 0 {
		recovery = d.Recovery
	}
	recoveryTimeout := opts.RecoveryTimeoutMs
	if recoveryTimeout == 0 {
		recoveryTimeout = d.RecoveryTimeoutMs
	}
	recoveryBackoff := opts.RecoveryBackoffMs
	if recoveryBackoff == 0 {
		recoveryBackoff = d.RecoveryBackoffMs
	}
	recoveryRetries := opts.RecoveryRetries
	if recoveryRetries == 0 {
		recoveryRetries = d.RecoveryRetries
	}
	serverAckTimeout := opts.ServerLackOfAckTimeoutMs
	if serverAckTimeout == 0 {
		serverAckTimeout = d.ServerLackOfAckTimeoutMs
	}
	flushTimeout := opts.FlushTimeoutMs
	if flushTimeout == 0 {
		flushTimeout = d.FlushTimeoutMs
	}
	connTimeout := opts.ConnectionTimeoutMs
	if connTimeout == 0 {
		connTimeout = d.ConnectionTimeoutMs
	}
	ipcCompression := opts.IPCCompression
	if ipcCompression == 0 && opts.IPCCompression == 0 {
		ipcCompression = d.IPCCompression
	}

	return C.CArrowStreamConfigurationOptions{
		max_inflight_batches:         C.uintptr_t(maxInflight),
		recovery:                     C.bool(recovery),
		recovery_timeout_ms:          C.uint64_t(recoveryTimeout),
		recovery_backoff_ms:          C.uint64_t(recoveryBackoff),
		recovery_retries:             C.uint32_t(recoveryRetries),
		server_lack_of_ack_timeout_ms: C.uint64_t(serverAckTimeout),
		flush_timeout_ms:             C.uint64_t(flushTimeout),
		connection_timeout_ms:        C.uint64_t(connTimeout),
		ipc_compression:              C.int32_t(ipcCompression),
	}
}

// sdkCreateArrowStream creates an Arrow Flight stream with OAuth authentication.
func sdkCreateArrowStream(
	sdkPtr unsafe.Pointer,
	tableName string,
	schemaIpcBytes []byte,
	clientID string,
	clientSecret string,
	options *ArrowStreamConfigurationOptions,
) (unsafe.Pointer, error) {
	cTableName := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTableName))
	cClientID := C.CString(clientID)
	defer C.free(unsafe.Pointer(cClientID))
	cClientSecret := C.CString(clientSecret)
	defer C.free(unsafe.Pointer(cClientSecret))

	var pinner runtime.Pinner
	defer pinner.Unpin()

	cSchema := (*C.uint8_t)(unsafe.SliceData(schemaIpcBytes))
	pinner.Pin(cSchema)

	cOpts := convertArrowConfigToC(options)
	var cres C.CResult
	ptr := C.zerobus_sdk_create_arrow_stream(
		(*C.CZerobusSdk)(sdkPtr),
		cTableName,
		cSchema,
		C.uintptr_t(len(schemaIpcBytes)),
		cClientID,
		cClientSecret,
		&cOpts,
		&cres,
	)
	if ptr == nil {
		return nil, arrowFfiResult(cres)
	}
	return unsafe.Pointer(ptr), nil
}

// sdkCreateArrowStreamWithHeadersProvider creates an Arrow Flight stream with a custom headers provider.
func sdkCreateArrowStreamWithHeadersProvider(
	sdkPtr unsafe.Pointer,
	tableName string,
	schemaIpcBytes []byte,
	headersProvider HeadersProvider,
	options *ArrowStreamConfigurationOptions,
) (unsafe.Pointer, error) {
	cTableName := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTableName))

	var pinner runtime.Pinner
	defer pinner.Unpin()

	cSchema := (*C.uint8_t)(unsafe.SliceData(schemaIpcBytes))
	pinner.Pin(cSchema)

	handle := cgo.NewHandle(headersProvider)
	handlePtr := *(*unsafe.Pointer)(unsafe.Pointer(&handle))

	cOpts := convertArrowConfigToC(options)
	var cres C.CResult
	ptr := C.zerobus_sdk_create_arrow_stream_with_headers_provider(
		(*C.CZerobusSdk)(sdkPtr),
		cTableName,
		cSchema,
		C.uintptr_t(len(schemaIpcBytes)),
		C.getArrowHeadersCallback(),
		handlePtr,
		&cOpts,
		&cres,
	)
	if ptr == nil {
		handle.Delete()
		return nil, arrowFfiResult(cres)
	}

	arrowStreamHandleRegistryMu.Lock()
	arrowStreamHandleRegistry[unsafe.Pointer(ptr)] = handle
	arrowStreamHandleRegistryMu.Unlock()

	return unsafe.Pointer(ptr), nil
}

// arrowStreamFree frees an Arrow Flight stream and its associated handle.
func arrowStreamFree(ptr unsafe.Pointer) {
	if ptr == nil {
		return
	}
	arrowStreamHandleRegistryMu.Lock()
	if handle, ok := arrowStreamHandleRegistry[ptr]; ok {
		handle.Delete()
		delete(arrowStreamHandleRegistry, ptr)
	}
	arrowStreamHandleRegistryMu.Unlock()
	C.zerobus_arrow_stream_free((*C.CArrowStream)(ptr))
}

// arrowStreamIngestBatch sends one Arrow IPC-encoded batch to the stream.
// Returns the logical offset assigned to this batch.
func arrowStreamIngestBatch(streamPtr unsafe.Pointer, ipcBytes []byte) (int64, error) {
	if len(ipcBytes) == 0 {
		return -1, &ZerobusError{Message: "empty IPC bytes", IsRetryable: false}
	}

	var pinner runtime.Pinner
	defer pinner.Unpin()
	cBytes := (*C.uint8_t)(unsafe.SliceData(ipcBytes))
	pinner.Pin(cBytes)

	var cres C.CResult
	offset := C.zerobus_arrow_stream_ingest_batch(
		(*C.CArrowStream)(streamPtr),
		cBytes,
		C.uintptr_t(len(ipcBytes)),
		&cres,
	)
	if offset < 0 {
		return -1, arrowFfiResult(cres)
	}
	return int64(offset), nil
}

// arrowStreamWaitForOffset blocks until the server acknowledges the given offset.
func arrowStreamWaitForOffset(streamPtr unsafe.Pointer, offset int64) error {
	var cres C.CResult
	ok := C.zerobus_arrow_stream_wait_for_offset(
		(*C.CArrowStream)(streamPtr),
		C.int64_t(offset),
		&cres,
	)
	if !ok {
		return arrowFfiResult(cres)
	}
	return nil
}

// arrowStreamFlush flushes all pending batches and waits for acknowledgment.
func arrowStreamFlush(streamPtr unsafe.Pointer) error {
	var cres C.CResult
	ok := C.zerobus_arrow_stream_flush((*C.CArrowStream)(streamPtr), &cres)
	if !ok {
		return arrowFfiResult(cres)
	}
	return nil
}

// arrowStreamClose gracefully closes the stream.
func arrowStreamClose(streamPtr unsafe.Pointer) error {
	var cres C.CResult
	ok := C.zerobus_arrow_stream_close((*C.CArrowStream)(streamPtr), &cres)
	if !ok {
		return arrowFfiResult(cres)
	}
	return nil
}

// arrowStreamGetUnackedBatches retrieves unacknowledged batches as Arrow IPC bytes.
// Each []byte is a self-contained IPC stream (schema + one record batch).
func arrowStreamGetUnackedBatches(streamPtr unsafe.Pointer) ([][]byte, error) {
	var cres C.CResult
	cArray := C.zerobus_arrow_stream_get_unacked_batches((*C.CArrowStream)(streamPtr), &cres)

	if cArray.count == 0 {
		if err := arrowFfiResult(cres); err != nil {
			return nil, err
		}
		return [][]byte{}, nil
	}

	batchPtrs := unsafe.Slice(cArray.batches, cArray.count)
	batchLens := unsafe.Slice(cArray.lengths, cArray.count)

	result := make([][]byte, cArray.count)
	for i := range result {
		result[i] = C.GoBytes(unsafe.Pointer(batchPtrs[i]), C.int(batchLens[i]))
	}

	C.zerobus_arrow_free_batch_array(cArray)
	return result, nil
}

// arrowStreamIsClosed returns true if the stream has been closed or failed.
func arrowStreamIsClosed(streamPtr unsafe.Pointer) bool {
	return bool(C.zerobus_arrow_stream_is_closed((*C.CArrowStream)(streamPtr)))
}
