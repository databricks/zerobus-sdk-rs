//go:build avro

package zerobus

/*
#define ZEROBUS_AVRO
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

// Forward declare opaque types
typedef struct CZerobusSdk CZerobusSdk;
typedef struct CZerobusStream CZerobusStream;

// Define result type
typedef struct CResult {
    bool success;
    char *error_message;
    bool is_retryable;
} CResult;

// Define stream configuration options
typedef struct CStreamConfigurationOptions CStreamConfigurationOptions;

// Declare headers types for callback
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
typedef void (*HeadersProviderFreeCallback)(void* user_data);

// Forward declarations for callbacks from ffi.go
extern void goGetHeaders(void* userData, CHeader** headers, uintptr_t* count, char** error);
extern void goFreeHeadersProvider(void* userData);

// C callback that matches the HeadersProviderCallback signature
static CHeaders cHeadersCallback(void* userData) {
    CHeader* headers = NULL;
    uintptr_t count = 0;
    char* error = NULL;

    goGetHeaders(userData, &headers, &count, &error);

    CHeaders result;
    result.headers = headers;
    result.count = count;
    result.error_message = error;
    return result;
}

// Helper function to get the C callback function pointer
static HeadersProviderCallback getHeadersCallback(void) {
    return (HeadersProviderCallback)cHeadersCallback;
}

// C callback matching HeadersProviderFreeCallback
static void cFreeHeadersProvider(void* userData) {
    goFreeHeadersProvider(userData);
}

static HeadersProviderFreeCallback getFreeHeadersProviderCallback(void) {
    return (HeadersProviderFreeCallback)cFreeHeadersProvider;
}

// Avro stream creation functions
extern CZerobusStream* zerobus_sdk_create_avro_stream(CZerobusSdk* sdk,
                                                       const char* table_name,
                                                       const char* avro_schema_json,
                                                       const char* client_id,
                                                       const char* client_secret,
                                                       const CStreamConfigurationOptions* options,
                                                       CResult* result);

extern CZerobusStream* zerobus_sdk_create_avro_stream_with_headers_provider(
    CZerobusSdk* sdk,
    const char* table_name,
    const char* avro_schema_json,
    HeadersProviderCallback headers_callback,
    void* user_data,
    HeadersProviderFreeCallback free_user_data,
    const CStreamConfigurationOptions* options,
    CResult* result);

// Avro record ingestion functions
extern int64_t zerobus_stream_ingest_avro_record(CZerobusStream* stream,
                                                  const uint8_t* data,
                                                  uintptr_t data_len,
                                                  CResult* result);

extern int64_t zerobus_stream_ingest_avro_records(CZerobusStream* stream,
                                                   const uint8_t* const* records,
                                                   const uintptr_t* record_lens,
                                                   uintptr_t num_records,
                                                   CResult* result);

extern void zerobus_stream_ingest_avro_record_nowait(CZerobusStream* stream,
                                                      const uint8_t* data,
                                                      uintptr_t data_len,
                                                      CResult* result);

extern void zerobus_stream_ingest_avro_records_nowait(CZerobusStream* stream,
                                                       const uint8_t* const* records,
                                                       const uintptr_t* record_lens,
                                                       uintptr_t num_records,
                                                       CResult* result);
*/
import "C"

import (
	"runtime"
	"runtime/cgo"
	"unsafe"
)

// sdkCreateAvroStream creates an Avro stream via FFI
func sdkCreateAvroStream(
	sdkPtr unsafe.Pointer,
	tableName string,
	schemaJSON string,
	clientID string,
	clientSecret string,
	options *StreamConfigurationOptions,
) (unsafe.Pointer, error) {
	cTableName := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTableName))

	cSchemaJSON := C.CString(schemaJSON)
	defer C.free(unsafe.Pointer(cSchemaJSON))

	cClientID := C.CString(clientID)
	defer C.free(unsafe.Pointer(cClientID))

	cClientSecret := C.CString(clientSecret)
	defer C.free(unsafe.Pointer(cClientSecret))

	cOpts := convertConfigToC(options)

	var cres C.CResult
	ptr := C.zerobus_sdk_create_avro_stream(
		(*C.CZerobusSdk)(sdkPtr),
		cTableName,
		cSchemaJSON,
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

// sdkCreateAvroStreamWithHeadersProvider creates an Avro stream with custom headers provider via FFI
func sdkCreateAvroStreamWithHeadersProvider(
	sdkPtr unsafe.Pointer,
	tableName string,
	schemaJSON string,
	headersProvider HeadersProvider,
	options *StreamConfigurationOptions,
) (unsafe.Pointer, error) {
	cTableName := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTableName))

	cSchemaJSON := C.CString(schemaJSON)
	defer C.free(unsafe.Pointer(cSchemaJSON))

	// Create a cgo.Handle for the provider and hand its ownership to the FFI
	handle := cgo.NewHandle(headersProvider)
	handlePtr := *(*unsafe.Pointer)(unsafe.Pointer(&handle))

	cOpts := convertConfigToC(options)

	var cres C.CResult
	ptr := C.zerobus_sdk_create_avro_stream_with_headers_provider(
		(*C.CZerobusSdk)(sdkPtr),
		cTableName,
		cSchemaJSON,
		C.getHeadersCallback(),
		handlePtr,
		C.getFreeHeadersProviderCallback(),
		&cOpts,
		&cres,
	)

	if ptr == nil {
		return nil, ffiResult(cres)
	}

	return unsafe.Pointer(ptr), nil
}

// streamIngestAvroRecord ingests a pre-encoded Avro datum
func streamIngestAvroRecord(streamPtr unsafe.Pointer, data []byte) (int64, error) {
	if len(data) == 0 {
		return -1, &ZerobusError{Message: "empty data", IsRetryable: false}
	}

	var pinner runtime.Pinner
	defer pinner.Unpin()

	cData := (*C.uint8_t)(unsafe.SliceData(data))
	pinner.Pin(cData)

	var cres C.CResult
	offset := C.zerobus_stream_ingest_avro_record(
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

// streamIngestAvroRecords ingests a batch of pre-encoded Avro datums
func streamIngestAvroRecords(streamPtr unsafe.Pointer, records [][]byte) (int64, error) {
	if len(records) == 0 {
		return -1, nil
	}

	ptrSize := C.size_t(unsafe.Sizeof((*C.uint8_t)(nil)))
	lenSize := C.size_t(unsafe.Sizeof(C.size_t(0)))
	n := C.size_t(len(records))

	cPtrArray := C.calloc(n, ptrSize)
	if cPtrArray == nil {
		return -1, &ZerobusError{Message: "out of memory allocating record pointer array", IsRetryable: false}
	}
	defer C.free(cPtrArray)

	cLenArray := C.calloc(n, lenSize)
	if cLenArray == nil {
		return -1, &ZerobusError{Message: "out of memory allocating record length array", IsRetryable: false}
	}
	defer C.free(cLenArray)

	recordPtrs := (*[1 << 30]*C.uint8_t)(cPtrArray)[:len(records):len(records)]
	recordLens := (*[1 << 30]C.size_t)(cLenArray)[:len(records):len(records)]

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
	offset := C.zerobus_stream_ingest_avro_records(
		(*C.CZerobusStream)(streamPtr),
		(**C.uint8_t)(cPtrArray),
		(*C.size_t)(cLenArray),
		n,
		&cres,
	)

	if offset == -2 {
		return -1, nil
	}
	if offset < 0 {
		return -1, ffiResult(cres)
	}

	return int64(offset), nil
}

// streamIngestAvroRecordNowait ingests a pre-encoded Avro datum without waiting
func streamIngestAvroRecordNowait(streamPtr unsafe.Pointer, data []byte) error {
	if len(data) == 0 {
		return &ZerobusError{Message: "empty data", IsRetryable: false}
	}

	var pinner runtime.Pinner
	defer pinner.Unpin()

	cData := (*C.uint8_t)(unsafe.SliceData(data))
	pinner.Pin(cData)

	var cres C.CResult
	C.zerobus_stream_ingest_avro_record_nowait(
		(*C.CZerobusStream)(streamPtr),
		cData,
		C.size_t(len(data)),
		&cres,
	)

	return ffiResult(cres)
}

// streamIngestAvroRecordsNowait ingests a batch of pre-encoded Avro datums without waiting
func streamIngestAvroRecordsNowait(streamPtr unsafe.Pointer, records [][]byte) error {
	if len(records) == 0 {
		return nil
	}

	ptrSize := C.size_t(unsafe.Sizeof((*C.uint8_t)(nil)))
	lenSize := C.size_t(unsafe.Sizeof(C.size_t(0)))
	n := C.size_t(len(records))

	cPtrArray := C.calloc(n, ptrSize)
	if cPtrArray == nil {
		return &ZerobusError{Message: "out of memory allocating record pointer array", IsRetryable: false}
	}
	defer C.free(cPtrArray)

	cLenArray := C.calloc(n, lenSize)
	if cLenArray == nil {
		return &ZerobusError{Message: "out of memory allocating record length array", IsRetryable: false}
	}
	defer C.free(cLenArray)

	recordPtrs := (*[1 << 30]*C.uint8_t)(cPtrArray)[:len(records):len(records)]
	recordLens := (*[1 << 30]C.size_t)(cLenArray)[:len(records):len(records)]

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
	C.zerobus_stream_ingest_avro_records_nowait(
		(*C.CZerobusStream)(streamPtr),
		(**C.uint8_t)(cPtrArray),
		(*C.size_t)(cLenArray),
		n,
		&cres,
	)

	return ffiResult(cres)
}
