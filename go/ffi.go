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

// Forward declare opaque types
typedef struct CZerobusSdk CZerobusSdk;
typedef struct CZerobusStream CZerobusStream;

// Define result type
typedef struct CResult {
    bool success;
    char *error_message;
    bool is_retryable;
} CResult;

typedef struct CRecord {
    bool is_json;
    uint8_t *data;
    uintptr_t data_len;
} CRecord;

typedef struct CRecordArray {
    CRecord *records;
    uintptr_t len;
} CRecordArray;

// Define headers types for callback
typedef struct CHeader {
    char *key;
    char *value;
} CHeader;

typedef struct CHeaders {
    struct CHeader *headers;
    uintptr_t count;
    char *error_message;
} CHeaders;

typedef struct CHeaders (*HeadersProviderCallback)(void *user_data);

// Define stream configuration options
typedef struct CStreamConfigurationOptions {
    uintptr_t max_inflight_requests;
    bool recovery;
    uint64_t recovery_timeout_ms;
    uint64_t recovery_backoff_ms;
    uint32_t recovery_retries;
    uint64_t server_lack_of_ack_timeout_ms;
    uint64_t flush_timeout_ms;
    int32_t record_type;
    uint64_t stream_paused_max_wait_time_ms;
    bool has_stream_paused_max_wait_time_ms;
    uint64_t callback_max_wait_time_ms;
    bool has_callback_max_wait_time_ms;
} CStreamConfigurationOptions;

// Forward declare functions we need
extern CZerobusSdk* zerobus_sdk_new(const char* zerobus_endpoint,
                                     const char* unity_catalog_url,
                                     CResult* result);
extern void zerobus_sdk_free(CZerobusSdk* sdk);
extern void zerobus_sdk_set_use_tls(CZerobusSdk* sdk, bool use_tls);
extern CZerobusStream* zerobus_sdk_create_stream(CZerobusSdk* sdk,
                                                   const char* table_name,
                                                   const uint8_t* descriptor_proto_bytes,
                                                   uintptr_t descriptor_proto_len,
                                                   const char* client_id,
                                                   const char* client_secret,
                                                   const CStreamConfigurationOptions* options,
                                                   CResult* result);
extern CZerobusStream* zerobus_sdk_create_stream_with_headers_provider(
    CZerobusSdk* sdk,
    const char* table_name,
    const uint8_t* descriptor_proto_bytes,
    uintptr_t descriptor_proto_len,
    HeadersProviderCallback headers_callback,
    void* user_data,
    const CStreamConfigurationOptions* options,
    CResult* result);
extern void zerobus_stream_free(CZerobusStream* stream);
extern int64_t zerobus_stream_ingest_proto_record(CZerobusStream* stream,
                                                   const uint8_t* data,
                                                   uintptr_t data_len,
                                                   CResult* result);
extern int64_t zerobus_stream_ingest_json_record(CZerobusStream* stream,
                                                  const char* json_data,
                                                  CResult* result);
extern int64_t zerobus_stream_ingest_proto_records(CZerobusStream* stream,
                                                     const uint8_t** records,
                                                     const uintptr_t* record_lens,
                                                     uintptr_t num_records,
                                                     CResult* result);
extern int64_t zerobus_stream_ingest_json_records(CZerobusStream* stream,
                                                    const char** json_records,
                                                    uintptr_t num_records,
                                                    CResult* result);
extern void zerobus_stream_ingest_proto_record_nowait(CZerobusStream* stream,
                                                       const uint8_t* data,
                                                       uintptr_t data_len,
                                                       CResult* result);
extern void zerobus_stream_ingest_json_record_nowait(CZerobusStream* stream,
                                                      const char* json_data,
                                                      CResult* result);
extern void zerobus_stream_ingest_proto_records_nowait(CZerobusStream* stream,
                                                        const uint8_t** records,
                                                        const uintptr_t* record_lens,
                                                        uintptr_t num_records,
                                                        CResult* result);
extern void zerobus_stream_ingest_json_records_nowait(CZerobusStream* stream,
                                                       const char** json_records,
                                                       uintptr_t num_records,
                                                       CResult* result);
extern bool zerobus_stream_wait_for_offset(CZerobusStream* stream,
                                             int64_t offset,
                                             CResult* result);
extern CRecordArray zerobus_stream_get_unacked_records(CZerobusStream* stream, CResult* result);
extern void zerobus_free_record_array(CRecordArray array);
extern bool zerobus_stream_flush(CZerobusStream* stream, CResult* result);
extern bool zerobus_stream_close(CZerobusStream* stream, CResult* result);
extern void zerobus_free_error_message(char* error_message);
extern CStreamConfigurationOptions zerobus_get_default_config();

// Forward declaration of Go function
extern void goGetHeaders(void* userData, CHeader** headers, uintptr_t* count, char** error);

// C callback that matches the HeadersProviderCallback signature
static CHeaders cHeadersCallback(void* userData) {
    CHeader* headers = NULL;
    uintptr_t count = 0;
    char* error = NULL;

    // Call Go function
    goGetHeaders(userData, &headers, &count, &error);

    CHeaders result;
    result.headers = headers;
    result.count = count;
    result.error_message = error;
    return result;
}

// Helper function to get the C callback function pointer
static HeadersProviderCallback getHeadersCallback() {
    return (HeadersProviderCallback)cHeadersCallback;
}
*/
import "C"
import (
	"runtime"
	"runtime/cgo"
	"sync"
	"unsafe"
)

// Registry to map stream pointers to their handles for cleanup
// This allows us to properly release cgo.Handle when streams are freed
var (
	streamHandleRegistry   = make(map[unsafe.Pointer]cgo.Handle)
	streamHandleRegistryMu sync.Mutex
)

// ffiResult converts a C.CResult to a Go error
func ffiResult(cres C.CResult) error {
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

// convertConfigToC converts Go config to C config
// Applies defaults for any zero-valued fields
func convertConfigToC(opts *StreamConfigurationOptions) C.CStreamConfigurationOptions {
	if opts == nil {
		return C.zerobus_get_default_config()
	}

	// Start with defaults and override with provided values
	defaults := DefaultStreamConfigurationOptions()

	maxInflight := opts.MaxInflightRequests
	if maxInflight == 0 {
		maxInflight = defaults.MaxInflightRequests
	}

	recovery := opts.Recovery
	// BUG: Recovery = false is indistinguishable from the zero value, so if all
	// timeout fields are also zero we fall back to the default (true). This heuristic
	// works when callers start from DefaultStreamConfigurationOptions(), but silently
	// ignores Recovery = false on a zero-value struct. Fixing this properly would
	// require a breaking API change (e.g. a RecoverySetting enum like ArrowStream uses).
	// Callers must use DefaultStreamConfigurationOptions() as a starting point.
	if opts.RecoveryTimeoutMs == 0 && opts.RecoveryBackoffMs == 0 && opts.RecoveryRetries == 0 {
		recovery = defaults.Recovery
	}

	recoveryTimeout := opts.RecoveryTimeoutMs
	if recoveryTimeout == 0 {
		recoveryTimeout = defaults.RecoveryTimeoutMs
	}

	recoveryBackoff := opts.RecoveryBackoffMs
	if recoveryBackoff == 0 {
		recoveryBackoff = defaults.RecoveryBackoffMs
	}

	recoveryRetries := opts.RecoveryRetries
	if recoveryRetries == 0 {
		recoveryRetries = defaults.RecoveryRetries
	}

	serverAckTimeout := opts.ServerLackOfAckTimeoutMs
	if serverAckTimeout == 0 {
		serverAckTimeout = defaults.ServerLackOfAckTimeoutMs
	}

	flushTimeout := opts.FlushTimeoutMs
	if flushTimeout == 0 {
		flushTimeout = defaults.FlushTimeoutMs
	}

	recordType := opts.RecordType
	if recordType == RecordTypeUnspecified {
		recordType = defaults.RecordType
	}

	// Handle optional StreamPausedMaxWaitTimeMs
	var streamPausedMaxWaitMs C.uint64_t
	hasStreamPausedMaxWait := C.bool(false)
	if opts.StreamPausedMaxWaitTimeMs != nil {
		streamPausedMaxWaitMs = C.uint64_t(*opts.StreamPausedMaxWaitTimeMs)
		hasStreamPausedMaxWait = C.bool(true)
	}

	return C.CStreamConfigurationOptions{
		max_inflight_requests:              C.size_t(maxInflight),
		recovery:                           C.bool(recovery),
		recovery_timeout_ms:                C.uint64_t(recoveryTimeout),
		recovery_backoff_ms:                C.uint64_t(recoveryBackoff),
		recovery_retries:                   C.uint32_t(recoveryRetries),
		server_lack_of_ack_timeout_ms:      C.uint64_t(serverAckTimeout),
		flush_timeout_ms:                   C.uint64_t(flushTimeout),
		record_type:                        C.int(recordType),
		stream_paused_max_wait_time_ms:     streamPausedMaxWaitMs,
		has_stream_paused_max_wait_time_ms: hasStreamPausedMaxWait,
		callback_max_wait_time_ms:          0,
		has_callback_max_wait_time_ms:      C.bool(false),
	}
}

// sdkNew creates a new SDK instance via FFI
func sdkNew(zerobusEndpoint, unityCatalogURL string) (unsafe.Pointer, error) {
	cEndpoint := C.CString(zerobusEndpoint)
	defer C.free(unsafe.Pointer(cEndpoint))

	cCatalogURL := C.CString(unityCatalogURL)
	defer C.free(unsafe.Pointer(cCatalogURL))

	var cres C.CResult
	ptr := C.zerobus_sdk_new(cEndpoint, cCatalogURL, &cres)

	if ptr == nil {
		return nil, ffiResult(cres)
	}

	// Disable TLS if using HTTP endpoint (for testing/mock servers)
	if len(zerobusEndpoint) >= 7 && zerobusEndpoint[:7] == "http://" {
		C.zerobus_sdk_set_use_tls(ptr, C.bool(false))
	}

	return unsafe.Pointer(ptr), nil
}

// sdkFree frees an SDK instance
func sdkFree(ptr unsafe.Pointer) {
	if ptr != nil {
		C.zerobus_sdk_free((*C.CZerobusSdk)(ptr))
	}
}

// sdkCreateStream creates a stream via FFI
func sdkCreateStream(
	sdkPtr unsafe.Pointer,
	tableName string,
	descriptorProto []byte,
	clientID string,
	clientSecret string,
	options *StreamConfigurationOptions,
) (unsafe.Pointer, error) {
	cTableName := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTableName))

	cClientID := C.CString(clientID)
	defer C.free(unsafe.Pointer(cClientID))

	cClientSecret := C.CString(clientSecret)
	defer C.free(unsafe.Pointer(cClientSecret))

	var cDescriptor *C.uint8_t
	var descriptorLen C.size_t
	var pinner runtime.Pinner

	if len(descriptorProto) > 0 {
		defer pinner.Unpin()
		cDescriptor = (*C.uint8_t)(unsafe.SliceData(descriptorProto))
		pinner.Pin(cDescriptor)
		descriptorLen = C.size_t(len(descriptorProto))
	}

	cOpts := convertConfigToC(options)

	var cres C.CResult
	ptr := C.zerobus_sdk_create_stream(
		(*C.CZerobusSdk)(sdkPtr),
		cTableName,
		cDescriptor,
		descriptorLen,
		cClientID,
		cClientSecret,
		&cOpts,
		&cres,
	)

	if ptr == nil {
		return nil, ffiResult(cres)
	}

	return unsafe.Pointer(ptr), nil
}

//export goGetHeaders
func goGetHeaders(userData unsafe.Pointer, headers **C.CHeader, count *C.uintptr_t, errorMsg **C.char) {
	// Convert userData back to cgo.Handle and retrieve the provider
	handle := cgo.Handle(userData)
	provider, ok := handle.Value().(HeadersProvider)

	if !ok {
		*errorMsg = C.CString("Invalid headers provider handle")
		*headers = nil
		*count = 0
		return
	}

	// Call the Go interface method
	headersMap, err := provider.GetHeaders()
	if err != nil {
		*errorMsg = C.CString(err.Error())
		*headers = nil
		*count = 0
		return
	}

	// Convert Go map to C array
	if len(headersMap) == 0 {
		*headers = nil
		*count = 0
		*errorMsg = nil
		return
	}

	// Allocate C array
	cHeaders := C.malloc(C.size_t(len(headersMap)) * C.size_t(unsafe.Sizeof(C.CHeader{})))
	if cHeaders == nil {
		*errorMsg = C.CString("out of memory allocating headers")
		*headers = nil
		*count = 0
		return
	}
	cHeadersSlice := (*[1 << 30]C.CHeader)(cHeaders)[:len(headersMap):len(headersMap)]

	idx := 0
	for key, value := range headersMap {
		cHeadersSlice[idx].key = C.CString(key)
		cHeadersSlice[idx].value = C.CString(value)
		idx++
	}

	*headers = (*C.CHeader)(cHeaders)
	*count = C.uintptr_t(len(headersMap))
	*errorMsg = nil
}

// sdkCreateStreamWithHeadersProvider creates a stream with custom headers provider via FFI
func sdkCreateStreamWithHeadersProvider(
	sdkPtr unsafe.Pointer,
	tableName string,
	descriptorProto []byte,
	headersProvider HeadersProvider,
	options *StreamConfigurationOptions,
) (unsafe.Pointer, error) {
	cTableName := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTableName))

	var cDescriptor *C.uint8_t
	var descriptorLen C.size_t
	var pinner runtime.Pinner

	if len(descriptorProto) > 0 {
		defer pinner.Unpin()
		cDescriptor = (*C.uint8_t)(unsafe.SliceData(descriptorProto))
		pinner.Pin(cDescriptor)
		descriptorLen = C.size_t(len(descriptorProto))
	}

	// Create a cgo.Handle for the provider
	// This keeps it alive and gives us a safe uintptr to pass to C
	handle := cgo.NewHandle(headersProvider)
	// Convert handle to unsafe.Pointer using a pattern the linter accepts
	handlePtr := *(*unsafe.Pointer)(unsafe.Pointer(&handle))

	cOpts := convertConfigToC(options)

	var cres C.CResult
	ptr := C.zerobus_sdk_create_stream_with_headers_provider(
		(*C.CZerobusSdk)(sdkPtr),
		cTableName,
		cDescriptor,
		descriptorLen,
		C.getHeadersCallback(),
		handlePtr,
		&cOpts,
		&cres,
	)

	if ptr == nil {
		// Clean up handle on error
		handle.Delete()
		return nil, ffiResult(cres)
	}

	// Store the handle so we can clean it up when the stream is freed
	streamHandleRegistryMu.Lock()
	streamHandleRegistry[unsafe.Pointer(ptr)] = handle
	streamHandleRegistryMu.Unlock()

	return unsafe.Pointer(ptr), nil
}

// streamFree frees a stream instance
func streamFree(ptr unsafe.Pointer) {
	if ptr != nil {
		// Clean up the associated handle BEFORE freeing the stream
		streamHandleRegistryMu.Lock()
		if handle, exists := streamHandleRegistry[ptr]; exists {
			handle.Delete() // This releases the Go object reference
			delete(streamHandleRegistry, ptr)
		}
		streamHandleRegistryMu.Unlock()

		C.zerobus_stream_free((*C.CZerobusStream)(ptr))
	}
}

// streamIngestProtoRecord ingests a protobuf record
// Returns the offset directly
func streamIngestProtoRecord(streamPtr unsafe.Pointer, data []byte) (int64, error) {
	if len(data) == 0 {
		return -1, &ZerobusError{Message: "empty data", IsRetryable: false}
	}

	// Pin the data to prevent GC from moving it while Rust reads it
	var pinner runtime.Pinner
	defer pinner.Unpin()

	cData := (*C.uint8_t)(unsafe.SliceData(data))
	pinner.Pin(cData)

	var cres C.CResult
	offset := C.zerobus_stream_ingest_proto_record(
		(*C.CZerobusStream)(streamPtr),
		cData,
		C.size_t(len(data)),
		&cres,
	)

	if offset < 0 {
		return -1, ffiResult(cres)
	}

	return int64(offset), nil
}

// streamIngestJSONRecord ingests a JSON record
// Returns the offset directly
func streamIngestJSONRecord(streamPtr unsafe.Pointer, jsonData string) (int64, error) {
	cJSON := C.CString(jsonData)
	defer C.free(unsafe.Pointer(cJSON))

	var cres C.CResult
	offset := C.zerobus_stream_ingest_json_record(
		(*C.CZerobusStream)(streamPtr),
		cJSON,
		&cres,
	)

	if offset < 0 {
		return -1, ffiResult(cres)
	}

	return int64(offset), nil
}

// streamIngestProtoRecordNowait ingests a protobuf record without waiting (fire-and-forget).
// Returns immediately; ingestion errors are silently ignored by the background task.
func streamIngestProtoRecordNowait(streamPtr unsafe.Pointer, data []byte) error {
	if len(data) == 0 {
		return &ZerobusError{Message: "empty data", IsRetryable: false}
	}

	var pinner runtime.Pinner
	defer pinner.Unpin()

	cData := (*C.uint8_t)(unsafe.SliceData(data))
	pinner.Pin(cData)

	var cres C.CResult
	C.zerobus_stream_ingest_proto_record_nowait(
		(*C.CZerobusStream)(streamPtr),
		cData,
		C.size_t(len(data)),
		&cres,
	)

	return ffiResult(cres)
}

// streamIngestJSONRecordNowait ingests a JSON record without waiting (fire-and-forget).
// Returns immediately; ingestion errors are silently ignored by the background task.
func streamIngestJSONRecordNowait(streamPtr unsafe.Pointer, jsonData string) error {
	cJSON := C.CString(jsonData)
	defer C.free(unsafe.Pointer(cJSON))

	var cres C.CResult
	C.zerobus_stream_ingest_json_record_nowait(
		(*C.CZerobusStream)(streamPtr),
		cJSON,
		&cres,
	)

	return ffiResult(cres)
}

// streamIngestProtoRecordsNowait ingests a batch of protobuf records without waiting (fire-and-forget).
// The Rust side copies all record data before spawning, so Go memory is safe to release on return.
func streamIngestProtoRecordsNowait(streamPtr unsafe.Pointer, records [][]byte) error {
	if len(records) == 0 {
		return nil
	}

	ptrSize := C.size_t(unsafe.Sizeof((*C.uint8_t)(nil)))
	lenSize := C.size_t(unsafe.Sizeof(C.size_t(0)))
	n := C.size_t(len(records))

	cPtrArray := C.malloc(n * ptrSize)
	if cPtrArray == nil {
		return &ZerobusError{Message: "out of memory allocating record pointer array", IsRetryable: false}
	}
	defer C.free(cPtrArray)

	cLenArray := C.malloc(n * lenSize)
	if cLenArray == nil {
		return &ZerobusError{Message: "out of memory allocating record length array", IsRetryable: false}
	}
	defer C.free(cLenArray)

	recordPtrs := (*[1 << 30]*C.uint8_t)(cPtrArray)[:len(records):len(records)]
	recordLens := (*[1 << 30]C.size_t)(cLenArray)[:len(records):len(records)]

	// Pin Go memory so it isn't moved while Rust copies it (before returning).
	var pinner runtime.Pinner
	defer pinner.Unpin()

	for i, record := range records {
		if len(record) > 0 {
			ptr := (*C.uint8_t)(unsafe.SliceData(records[i]))
			pinner.Pin(ptr)
			recordPtrs[i] = ptr
			recordLens[i] = C.size_t(len(record))
		}
	}

	var cres C.CResult
	C.zerobus_stream_ingest_proto_records_nowait(
		(*C.CZerobusStream)(streamPtr),
		(**C.uint8_t)(cPtrArray),
		(*C.size_t)(cLenArray),
		n,
		&cres,
	)

	return ffiResult(cres)
}

// streamIngestJSONRecordsNowait ingests a batch of JSON records without waiting (fire-and-forget).
// The Rust side copies all strings before spawning, so C strings are safe to free on return.
func streamIngestJSONRecordsNowait(streamPtr unsafe.Pointer, records []string) error {
	if len(records) == 0 {
		return nil
	}

	ptrSize := C.size_t(unsafe.Sizeof((*C.char)(nil)))
	cPtrArray := C.malloc(C.size_t(len(records)) * ptrSize)
	if cPtrArray == nil {
		return &ZerobusError{Message: "out of memory allocating C string array", IsRetryable: false}
	}
	defer C.free(cPtrArray)

	cStrings := (*[1 << 30]*C.char)(cPtrArray)[:len(records):len(records)]
	for i, record := range records {
		cStrings[i] = C.CString(record)
	}
	defer func() {
		for _, cStr := range cStrings {
			if cStr != nil {
				C.free(unsafe.Pointer(cStr))
			}
		}
	}()

	var cres C.CResult
	C.zerobus_stream_ingest_json_records_nowait(
		(*C.CZerobusStream)(streamPtr),
		(**C.char)(cPtrArray),
		C.size_t(len(records)),
		&cres,
	)

	return ffiResult(cres)
}

// streamIngestProtoRecords ingests a batch of protobuf records
func streamIngestProtoRecords(streamPtr unsafe.Pointer, records [][]byte) (int64, error) {
	if len(records) == 0 {
		return -1, nil // Return special value for empty batch
	}

	// Allocate pointer and length arrays in C memory so they are invisible to
	// the Go GC: no pinning needed for the arrays themselves, and no Go heap
	// allocation pressure per call (issue #271).
	ptrSize := C.size_t(unsafe.Sizeof((*C.uint8_t)(nil)))
	lenSize := C.size_t(unsafe.Sizeof(C.size_t(0)))
	n := C.size_t(len(records))

	cPtrArray := C.malloc(n * ptrSize)
	if cPtrArray == nil {
		return -1, &ZerobusError{Message: "out of memory allocating record pointer array", IsRetryable: false}
	}
	defer C.free(cPtrArray)

	cLenArray := C.malloc(n * lenSize)
	if cLenArray == nil {
		return -1, &ZerobusError{Message: "out of memory allocating record length array", IsRetryable: false}
	}
	defer C.free(cLenArray)

	recordPtrs := (*[1 << 30]*C.uint8_t)(cPtrArray)[:len(records):len(records)]
	recordLens := (*[1 << 30]C.size_t)(cLenArray)[:len(records):len(records)]

	// The record data itself lives in caller-owned Go []byte slices and must
	// be pinned so the GC does not move it while Rust reads it.
	var pinner runtime.Pinner
	defer pinner.Unpin()

	for i, record := range records {
		if len(record) > 0 {
			ptr := (*C.uint8_t)(unsafe.SliceData(records[i]))
			pinner.Pin(ptr)
			recordPtrs[i] = ptr
			recordLens[i] = C.size_t(len(record))
		}
	}

	var cres C.CResult
	offset := C.zerobus_stream_ingest_proto_records(
		(*C.CZerobusStream)(streamPtr),
		(**C.uint8_t)(cPtrArray),
		(*C.size_t)(cLenArray),
		n,
		&cres,
	)

	if offset == -2 {
		return -1, nil // Empty batch
	}
	if offset < 0 {
		return -1, ffiResult(cres)
	}

	return int64(offset), nil
}

// streamIngestJSONRecords ingests a batch of JSON records
func streamIngestJSONRecords(streamPtr unsafe.Pointer, records []string) (int64, error) {
	if len(records) == 0 {
		return -1, nil // Return special value for empty batch
	}

	// Allocate the pointer array in C memory so it is invisible to the Go GC:
	// no pinning needed, and no Go heap allocation pressure per call.
	ptrSize := C.size_t(unsafe.Sizeof((*C.char)(nil)))
	cPtrArray := C.malloc(C.size_t(len(records)) * ptrSize)
	if cPtrArray == nil {
		return -1, &ZerobusError{Message: "out of memory allocating C string array", IsRetryable: false}
	}
	defer C.free(cPtrArray)

	// View the C allocation as a slice for convenient indexing.
	cStrings := (*[1 << 30]*C.char)(cPtrArray)[:len(records):len(records)]

	// Convert each Go string to a C string. Free them all in a single deferred
	// closure — one defer record regardless of batch size, avoiding the
	// per-element defer accumulation that caused GC pressure (issue #271).
	for i, record := range records {
		cStrings[i] = C.CString(record)
	}
	defer func() {
		for _, cStr := range cStrings {
			if cStr != nil {
				C.free(unsafe.Pointer(cStr))
			}
		}
	}()

	var cres C.CResult
	offset := C.zerobus_stream_ingest_json_records(
		(*C.CZerobusStream)(streamPtr),
		(**C.char)(cPtrArray),
		C.size_t(len(records)),
		&cres,
	)

	if offset == -2 {
		return -1, nil // Empty batch
	}
	if offset < 0 {
		return -1, ffiResult(cres)
	}

	return int64(offset), nil
}

// streamWaitForOffset waits for a specific offset to be acknowledged
func streamWaitForOffset(streamPtr unsafe.Pointer, offset int64) error {
	var cres C.CResult
	success := C.zerobus_stream_wait_for_offset(
		(*C.CZerobusStream)(streamPtr),
		C.int64_t(offset),
		&cres,
	)

	if !success {
		return ffiResult(cres)
	}
	return nil
}

// streamFlush flushes pending records
func streamFlush(streamPtr unsafe.Pointer) error {
	var cres C.CResult
	success := C.zerobus_stream_flush((*C.CZerobusStream)(streamPtr), &cres)

	if !success {
		return ffiResult(cres)
	}

	return nil
}

// streamGetUnackedRecords retrieves all unacknowledged records
func streamGetUnackedRecords(streamPtr unsafe.Pointer) ([]interface{}, error) {
	var cres C.CResult
	cArray := C.zerobus_stream_get_unacked_records(
		(*C.CZerobusStream)(streamPtr),
		&cres,
	)

	// Check if there was an error (null pointer indicates error)
	if cArray.records == nil {
		if cArray.len == 0 {
			// Could be an error or empty - check result
			err := ffiResult(cres)
			if err != nil {
				return nil, err
			}
			// Empty result, no error
			return []interface{}{}, nil
		}
		return nil, ffiResult(cres)
	}

	// Convert C array to Go slice
	if cArray.len == 0 {
		return []interface{}{}, nil
	}

	records := make([]interface{}, cArray.len)
	cRecords := unsafe.Slice(cArray.records, cArray.len)

	for i, cRecord := range cRecords {
		if cRecord.is_json {
			// Convert C data to Go string for JSON
			data := C.GoBytes(unsafe.Pointer(cRecord.data), C.int(cRecord.data_len))
			records[i] = string(data)
		} else {
			// Convert C data to Go []byte for protobuf
			data := C.GoBytes(unsafe.Pointer(cRecord.data), C.int(cRecord.data_len))
			records[i] = data
		}
	}

	// Free the C array memory
	C.zerobus_free_record_array(cArray)

	return records, nil
}

// streamClose closes the stream
func streamClose(streamPtr unsafe.Pointer) error {
	var cres C.CResult
	success := C.zerobus_stream_close((*C.CZerobusStream)(streamPtr), &cres)

	if !success {
		return ffiResult(cres)
	}

	return nil
}
