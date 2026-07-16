#[cfg(test)]
mod tests {
    use crate::{
        c_record_type, ffi_guard, intern_header_key, validate_sdk_ptr, validate_stream_ptr,
        write_error_result, write_success_result, zerobus_free_error_message,
        zerobus_get_default_config, zerobus_sdk_builder_application_name,
        zerobus_sdk_builder_build, zerobus_sdk_builder_disable_tls, zerobus_sdk_builder_endpoint,
        zerobus_sdk_builder_free, zerobus_sdk_builder_new, zerobus_sdk_builder_sdk_identifier,
        zerobus_sdk_builder_unity_catalog_url, zerobus_sdk_free, CHeaders, CRecordArray, CResult,
        CallbackHeadersProvider, RecordType, ZerobusError,
    };
    use databricks_zerobus_ingest_sdk::HeadersProvider;
    use std::ffi::{CStr, CString};
    use std::ptr;

    // Helper for c_str_to_string since it's private
    unsafe fn test_c_str_to_string(
        c_str: *const std::os::raw::c_char,
    ) -> Result<String, &'static str> {
        if c_str.is_null() {
            return Err("Null pointer passed");
        }
        CStr::from_ptr(c_str)
            .to_str()
            .map(|s| s.to_string())
            .map_err(|_| "Invalid UTF-8 string")
    }

    // ========================================================================
    // Safety Wrapper Tests
    // ========================================================================

    #[test]
    fn test_validate_sdk_ptr_null() {
        let result = validate_sdk_ptr(ptr::null_mut());
        assert!(result.is_err());
        assert_eq!(result.err().unwrap(), "SDK pointer is null");
    }

    #[test]
    fn test_validate_stream_ptr_null() {
        let result = validate_stream_ptr(ptr::null_mut());
        assert!(result.is_err());
        assert_eq!(result.err().unwrap(), "Stream pointer is null");
    }

    #[test]
    fn test_write_error_result() {
        let mut result = CResult {
            success: true,
            error_message: ptr::null_mut(),
            is_retryable: false,
        };

        write_error_result(&mut result as *mut CResult, "Test error", true);

        assert!(!result.success);
        assert!(!result.error_message.is_null());
        assert!(result.is_retryable);

        // Clean up
        unsafe {
            if !result.error_message.is_null() {
                let _ = CString::from_raw(result.error_message);
            }
        }
    }

    #[test]
    fn test_write_success_result() {
        let mut result = CResult {
            success: false,
            error_message: CString::new("error").unwrap().into_raw(),
            is_retryable: true,
        };

        write_success_result(&mut result as *mut CResult);

        assert!(result.success);
        assert!(result.error_message.is_null());
        assert!(!result.is_retryable);
    }

    #[test]
    fn test_write_error_result_with_null_pointer() {
        // Should not panic when result pointer is null
        write_error_result(ptr::null_mut(), "Test error", false);
        // If we get here, test passed
    }

    #[test]
    fn test_write_success_result_with_null_pointer() {
        // Should not panic when result pointer is null
        write_success_result(ptr::null_mut());
        // If we get here, test passed
    }

    // ========================================================================
    // Header Key Cache Tests
    // ========================================================================

    #[test]
    fn test_intern_header_key_caches_keys() {
        // First call - should create new entry
        let key1 = intern_header_key("Authorization".to_string());

        // Second call with same string - should return cached entry
        let key2 = intern_header_key("Authorization".to_string());

        // Should be the same pointer (same address in memory)
        assert_eq!(key1.as_ptr(), key2.as_ptr());
    }

    #[test]
    fn test_intern_header_key_different_keys() {
        let key1 = intern_header_key("Authorization".to_string());
        let key2 = intern_header_key("Content-Type".to_string());

        // Different keys should have different pointers
        assert_ne!(key1.as_ptr(), key2.as_ptr());
        assert_eq!(key1, "Authorization");
        assert_eq!(key2, "Content-Type");
    }

    #[test]
    fn test_intern_header_key_prevents_duplicate_leaks() {
        // Clear the cache first (can't actually do this safely in test, but we can verify behavior)
        let initial_key = intern_header_key("X-Test-Header".to_string());

        // Call many times
        for _ in 0..100 {
            let key = intern_header_key("X-Test-Header".to_string());
            // All should point to the same memory location
            assert_eq!(initial_key.as_ptr(), key.as_ptr());
        }
    }

    // ========================================================================
    // CResult Tests
    // ========================================================================

    #[test]
    fn test_cresult_success() {
        let result = CResult::success();
        assert!(result.success);
        assert!(result.error_message.is_null());
        assert!(!result.is_retryable);
    }

    #[test]
    fn test_cresult_error() {
        let error = ZerobusError::InvalidArgument("Test error".to_string());
        let result = CResult::error(error);

        assert!(!result.success);
        assert!(!result.error_message.is_null());

        // Verify error message
        let msg = unsafe { CStr::from_ptr(result.error_message).to_string_lossy() };
        assert!(msg.contains("Test error"));

        // Clean up
        unsafe {
            let _ = CString::from_raw(result.error_message);
        }
    }

    // ========================================================================
    // Configuration Tests
    // ========================================================================

    #[test]
    fn test_c_record_type_mapping() {
        assert_eq!(c_record_type(1), RecordType::Proto);
        assert_eq!(c_record_type(2), RecordType::Json);
        assert_eq!(c_record_type(999), RecordType::Unspecified);
        assert_eq!(c_record_type(0), RecordType::Unspecified);
    }

    // ========================================================================
    // zerobus_sdk_builder Tests
    // ========================================================================

    /// Builds an SDK via the C builder API. Caller frees the SDK and any
    /// error message.
    fn build_via_c_builder(
        endpoint: &str,
        unity_catalog_url: &str,
        sdk_identifier: Option<&str>,
        application_name: Option<&str>,
    ) -> (*mut crate::CZerobusSdk, CResult) {
        let endpoint_c = CString::new(endpoint).unwrap();
        let uc_c = CString::new(unity_catalog_url).unwrap();

        let builder = zerobus_sdk_builder_new();
        assert!(!builder.is_null());

        zerobus_sdk_builder_endpoint(builder, endpoint_c.as_ptr());
        zerobus_sdk_builder_unity_catalog_url(builder, uc_c.as_ptr());

        if let Some(id) = sdk_identifier {
            let id_c = CString::new(id).unwrap();
            zerobus_sdk_builder_sdk_identifier(builder, id_c.as_ptr());
        }
        if let Some(app) = application_name {
            let app_c = CString::new(app).unwrap();
            zerobus_sdk_builder_application_name(builder, app_c.as_ptr());
        }

        let mut result = CResult {
            success: false,
            error_message: ptr::null_mut(),
            is_retryable: false,
        };
        let sdk = zerobus_sdk_builder_build(builder, &mut result);
        (sdk, result)
    }

    #[test]
    fn test_builder_minimal() {
        let (sdk, result) =
            build_via_c_builder("https://workspace.zerobus.databricks.com", "", None, None);
        assert!(result.success, "expected success, got error");
        assert!(!sdk.is_null());
        zerobus_sdk_free(sdk);
    }

    #[test]
    fn test_builder_with_sdk_identifier() {
        let (sdk, result) = build_via_c_builder(
            "https://workspace.zerobus.databricks.com",
            "",
            Some("zerobus-sdk-go/1.3.0"),
            None,
        );
        assert!(result.success);
        assert!(!sdk.is_null());
        zerobus_sdk_free(sdk);
    }

    #[test]
    fn test_builder_with_application_name() {
        let (sdk, result) = build_via_c_builder(
            "https://workspace.zerobus.databricks.com",
            "",
            None,
            Some("my-app/1.0"),
        );
        assert!(result.success);
        assert!(!sdk.is_null());
        zerobus_sdk_free(sdk);
    }

    #[test]
    fn test_builder_both_user_agent_options() {
        let (sdk, result) = build_via_c_builder(
            "https://workspace.zerobus.databricks.com",
            "",
            Some("zerobus-sdk-go/1.3.0"),
            Some("my-app/1.0"),
        );
        assert!(result.success);
        assert!(!sdk.is_null());
        zerobus_sdk_free(sdk);
    }

    #[test]
    fn test_builder_empty_strings_are_noops() {
        // Empty identifier/application_name must not produce a trailing space.
        let (sdk, result) = build_via_c_builder(
            "https://workspace.zerobus.databricks.com",
            "",
            Some(""),
            Some(""),
        );
        assert!(result.success);
        assert!(!sdk.is_null());
        zerobus_sdk_free(sdk);
    }

    #[test]
    fn test_builder_build_consumes_on_error() {
        // Missing endpoint fails build. Builder must still be consumed —
        // don't _free the pointer afterward (use-after-free).
        let builder = zerobus_sdk_builder_new();
        let mut result = CResult {
            success: false,
            error_message: ptr::null_mut(),
            is_retryable: false,
        };
        let sdk = zerobus_sdk_builder_build(builder, &mut result);
        assert!(sdk.is_null());
        assert!(!result.success);
        zerobus_free_error_message(result.error_message);
    }

    #[test]
    fn test_builder_free_without_build() {
        let builder = zerobus_sdk_builder_new();
        zerobus_sdk_builder_free(builder);
    }

    #[test]
    fn test_builder_free_on_null_is_safe() {
        zerobus_sdk_builder_free(ptr::null_mut());
    }

    #[test]
    fn test_builder_setters_on_null_are_safe() {
        let s = CString::new("x").unwrap();
        zerobus_sdk_builder_endpoint(ptr::null_mut(), s.as_ptr());
        zerobus_sdk_builder_unity_catalog_url(ptr::null_mut(), s.as_ptr());
        zerobus_sdk_builder_sdk_identifier(ptr::null_mut(), s.as_ptr());
        zerobus_sdk_builder_application_name(ptr::null_mut(), s.as_ptr());
        zerobus_sdk_builder_disable_tls(ptr::null_mut());
    }

    #[test]
    fn test_builder_disable_tls_for_plain_http() {
        let endpoint_c = CString::new("http://localhost:50051").unwrap();
        let builder = zerobus_sdk_builder_new();
        zerobus_sdk_builder_endpoint(builder, endpoint_c.as_ptr());
        zerobus_sdk_builder_disable_tls(builder);
        let mut result = CResult {
            success: false,
            error_message: ptr::null_mut(),
            is_retryable: false,
        };
        let sdk = zerobus_sdk_builder_build(builder, &mut result);
        assert!(result.success);
        assert!(!sdk.is_null());
        zerobus_sdk_free(sdk);
    }

    #[test]
    fn test_get_default_config() {
        let config = zerobus_get_default_config();

        // Verify it returns reasonable defaults
        assert!(config.max_inflight_requests > 0);
        assert_eq!(config.record_type, 1); // Proto
    }

    // ========================================================================
    // C String Conversion Tests
    // ========================================================================

    #[test]
    fn test_c_str_to_string_valid() {
        let test_str = CString::new("Hello, World!").unwrap();
        let result = unsafe { test_c_str_to_string(test_str.as_ptr()) };

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Hello, World!");
    }

    #[test]
    fn test_c_str_to_string_null() {
        let result = unsafe { test_c_str_to_string(ptr::null()) };
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Null pointer passed");
    }

    #[test]
    fn test_c_str_to_string_empty() {
        let test_str = CString::new("").unwrap();
        let result = unsafe { test_c_str_to_string(test_str.as_ptr()) };

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "");
    }

    // ========================================================================
    // Memory Management Tests
    // ========================================================================

    #[test]
    fn test_zerobus_free_error_message_null() {
        // Should not panic with null pointer
        zerobus_free_error_message(ptr::null_mut());
    }

    #[test]
    fn test_zerobus_free_error_message_valid() {
        let msg = CString::new("Test error").unwrap().into_raw();
        zerobus_free_error_message(msg);
        // If we get here without crashing, test passed
    }

    // ========================================================================
    // Thread Safety Tests
    // ========================================================================

    #[test]
    fn test_callback_headers_provider_sequential() {
        extern "C" fn test_callback(_user_data: *mut std::ffi::c_void) -> CHeaders {
            CHeaders {
                headers: ptr::null_mut(),
                count: 0,
                error_message: ptr::null_mut(),
            }
        }

        let provider = CallbackHeadersProvider::new(test_callback, ptr::null_mut());

        // Sequential calls should work fine
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result1 = rt.block_on(provider.get_headers());
        assert!(result1.is_ok());

        let result2 = rt.block_on(provider.get_headers());
        assert!(result2.is_ok());
    }

    #[test]
    fn test_callback_headers_provider_returns_headers() {
        extern "C" fn test_callback(_user_data: *mut std::ffi::c_void) -> CHeaders {
            // Create simple test headers
            let auth_key = CString::new("Authorization").unwrap().into_raw();
            let auth_val = CString::new("Bearer test-token").unwrap().into_raw();

            let header = Box::new(crate::CHeader {
                key: auth_key,
                value: auth_val,
            });

            CHeaders {
                headers: Box::into_raw(header),
                count: 1,
                error_message: ptr::null_mut(),
            }
        }

        let provider = CallbackHeadersProvider::new(test_callback, ptr::null_mut());

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(provider.get_headers());

        assert!(result.is_ok());
        let headers = result.unwrap();
        assert_eq!(headers.len(), 1);
        assert!(headers.contains_key("Authorization"));
    }

    // ========================================================================
    // Ack callback bridge tests
    // ========================================================================

    use crate::CallbackAckCallback;
    use databricks_zerobus_ingest_sdk::AckCallback as _AckCallbackTrait;
    use std::os::raw::c_char;
    use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};

    // extern "C" callbacks can't capture, so they record into these statics.
    // Tests reset the slots before use.
    static LAST_ACK_OFFSET: AtomicI64 = AtomicI64::new(-1);
    static LAST_ERROR_OFFSET: AtomicI64 = AtomicI64::new(-1);
    static ACK_CALL_COUNT: AtomicI64 = AtomicI64::new(0);

    extern "C" fn record_ack(offset_id: i64, _user_data: *mut std::ffi::c_void) {
        LAST_ACK_OFFSET.store(offset_id, AtomicOrdering::SeqCst);
        ACK_CALL_COUNT.fetch_add(1, AtomicOrdering::SeqCst);
    }

    extern "C" fn record_error(
        offset_id: i64,
        error_message: *const c_char,
        _user_data: *mut std::ffi::c_void,
    ) {
        LAST_ERROR_OFFSET.store(offset_id, AtomicOrdering::SeqCst);
        // The message must be a valid NUL-terminated string for the call's duration.
        assert!(!error_message.is_null());
        let msg = unsafe { CStr::from_ptr(error_message) }
            .to_str()
            .expect("error message must be valid UTF-8");
        assert_eq!(msg, "boom");
    }

    #[test]
    fn test_callback_ack_bridge_on_ack() {
        LAST_ACK_OFFSET.store(-1, AtomicOrdering::SeqCst);
        ACK_CALL_COUNT.store(0, AtomicOrdering::SeqCst);

        let cb = CallbackAckCallback::new(Some(record_ack), None, ptr::null_mut());
        cb.on_ack(42);

        assert_eq!(LAST_ACK_OFFSET.load(AtomicOrdering::SeqCst), 42);
        assert_eq!(ACK_CALL_COUNT.load(AtomicOrdering::SeqCst), 1);
    }

    #[test]
    fn test_callback_ack_bridge_on_error_marshals_message() {
        LAST_ERROR_OFFSET.store(-1, AtomicOrdering::SeqCst);

        let cb = CallbackAckCallback::new(None, Some(record_error), ptr::null_mut());
        cb.on_error(7, "boom");

        assert_eq!(LAST_ERROR_OFFSET.load(AtomicOrdering::SeqCst), 7);
    }

    #[test]
    fn test_callback_ack_bridge_null_callbacks_are_noops() {
        // Neither pointer set: invoking both entry points must not panic.
        let cb = CallbackAckCallback::new(None, None, ptr::null_mut());
        cb.on_ack(1);
        cb.on_error(2, "ignored");
    }

    #[test]
    fn test_default_config_has_no_ack_callback() {
        let config = zerobus_get_default_config();
        assert!(config.ack_on_ack.is_none());
        assert!(config.ack_on_error.is_none());
        assert!(config.ack_user_data.is_null());
    }

    // ========================================================================
    // Ack callback lifetime on failed stream creation
    // ========================================================================
    //
    // When `create_stream` fails, the registered `Arc<CallbackAckCallback>` must
    // drop (with the builder), not leak to a background task. Observed via the
    // `#[cfg(test)]` drop hook in `common.rs`. Hermetic failure trigger: JSON +
    // empty table name registers the Arc in `apply_c_stream_options`, then fails
    // in `build()` validation before any network I/O.
    //
    // The Arc is created internally, so these tests can't hold a `Weak` to
    // confirm it was ever registered. A drop delta of 0 is therefore ambiguous:
    // the Arc was registered but leaked to a task, OR it was never registered
    // (e.g. the registration path or sentinel wiring drifted). The assert
    // messages spell out both causes.

    use crate::common::{ACK_CALLBACK_DROP_COUNT, ACK_DROP_SENTINEL_CREATE_FAIL_TESTS};
    use crate::{zerobus_sdk_create_stream, zerobus_sdk_create_stream_with_headers_provider};

    // Global counter, so serialize this pair's before/after sampling window.
    static ACK_DROP_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    // No-op ack/error callbacks; never invoked (create fails before any ack).
    extern "C" fn noop_ack(_offset_id: i64, _user_data: *mut std::ffi::c_void) {}
    extern "C" fn noop_error(
        _offset_id: i64,
        _error_message: *const c_char,
        _user_data: *mut std::ffi::c_void,
    ) {
    }

    // JSON config with both ack callbacks set, keyed to this test-only sentinel.
    fn json_config_with_ack_callback() -> crate::CStreamConfigurationOptions {
        let mut config = zerobus_get_default_config();
        config.record_type = 2; // RecordType::Json
        config.ack_on_ack = Some(noop_ack);
        config.ack_on_error = Some(noop_error);
        config.ack_user_data =
            (&ACK_DROP_SENTINEL_CREATE_FAIL_TESTS as *const u8) as *mut std::ffi::c_void;
        config
    }

    // Headers callback for the with-headers-provider path; never invoked (create fails first).
    extern "C" fn empty_headers(_user_data: *mut std::ffi::c_void) -> CHeaders {
        CHeaders {
            headers: ptr::null_mut(),
            count: 0,
            error_message: ptr::null_mut(),
        }
    }

    #[test]
    fn test_create_stream_releases_ack_arc_on_failure() {
        let _guard = ACK_DROP_TEST_LOCK.lock().unwrap();

        let (sdk, sdk_result) =
            build_via_c_builder("https://workspace.zerobus.databricks.com", "", None, None);
        assert!(sdk_result.success);
        assert!(!sdk.is_null());

        let empty_table = CString::new("").unwrap();
        let client_id = CString::new("client-id").unwrap();
        let client_secret = CString::new("client-secret").unwrap();
        let options = json_config_with_ack_callback();
        let mut result = presumed_success_result();

        // Delta across the single failing call.
        let before = ACK_CALLBACK_DROP_COUNT.load(AtomicOrdering::SeqCst);
        let stream = zerobus_sdk_create_stream(
            sdk,
            empty_table.as_ptr(),
            ptr::null(),
            0,
            client_id.as_ptr(),
            client_secret.as_ptr(),
            &options as *const crate::CStreamConfigurationOptions,
            &mut result as *mut CResult,
        );
        let after = ACK_CALLBACK_DROP_COUNT.load(AtomicOrdering::SeqCst);

        // Creation failed on the empty table (InvalidArgument in build()); the
        // only invalid field is the table name. Assert failure + a reported
        // message without pinning the exact wording.
        assert!(stream.is_null());
        let (success, _retryable, msg) = drain_result(&mut result);
        assert!(!success, "expected create_stream to fail on empty table");
        assert!(!msg.is_empty(), "expected a non-empty error message");

        // Ack callback Arc was released, not retained by a task.
        assert_eq!(
            after - before,
            1,
            "expected the ack callback Arc to be dropped exactly once on failed \
             create_stream (0 = leaked to a task OR never registered)"
        );

        zerobus_sdk_free(sdk);
    }

    #[test]
    fn test_create_stream_with_headers_provider_releases_ack_arc_on_failure() {
        let _guard = ACK_DROP_TEST_LOCK.lock().unwrap();

        let (sdk, sdk_result) =
            build_via_c_builder("https://workspace.zerobus.databricks.com", "", None, None);
        assert!(sdk_result.success);
        assert!(!sdk.is_null());

        let empty_table = CString::new("").unwrap();
        let options = json_config_with_ack_callback();
        let mut result = presumed_success_result();

        let before = ACK_CALLBACK_DROP_COUNT.load(AtomicOrdering::SeqCst);
        let stream = zerobus_sdk_create_stream_with_headers_provider(
            sdk,
            empty_table.as_ptr(),
            ptr::null(),
            0,
            empty_headers,
            ptr::null_mut(),
            &options as *const crate::CStreamConfigurationOptions,
            &mut result as *mut CResult,
        );
        let after = ACK_CALLBACK_DROP_COUNT.load(AtomicOrdering::SeqCst);

        assert!(stream.is_null());
        let (success, _retryable, msg) = drain_result(&mut result);
        assert!(
            !success,
            "expected create_stream_with_headers_provider to fail on empty table"
        );
        assert!(!msg.is_empty(), "expected a non-empty error message");

        assert_eq!(
            after - before,
            1,
            "expected the ack callback Arc to be dropped exactly once on failed \
             create_stream_with_headers_provider (0 = leaked to a task OR never registered)"
        );

        zerobus_sdk_free(sdk);
    }

    // ========================================================================
    // Dynamic protobuf schema tests
    // ========================================================================

    use crate::{
        zerobus_free_proto_bytes, zerobus_proto_schema_descriptor_bytes,
        zerobus_proto_schema_encode_json, zerobus_proto_schema_free,
        zerobus_proto_schema_from_uc_json,
    };
    use prost::Message;
    use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};

    // Minimal Unity Catalog table-metadata JSON, shaped like the body of
    // GET /api/2.1/unity-catalog/tables/{name}.
    fn sample_uc_table_json() -> CString {
        let json = r#"{
            "name": "events",
            "catalog_name": "main",
            "schema_name": "analytics",
            "columns": [
                {"name": "id", "type_name": "BIGINT", "type_text": "bigint", "nullable": false, "position": 0},
                {"name": "payload", "type_name": "STRING", "type_text": "string", "nullable": true, "position": 1},
                {"name": "ts", "type_name": "TIMESTAMP", "type_text": "timestamp", "nullable": true, "position": 2}
            ]
        }"#;
        CString::new(json).unwrap()
    }

    // A CResult to be written into by the function under test. Starts as a
    // failure so a success-path test proves the call flipped it to success.
    fn unwritten_result() -> CResult {
        CResult {
            success: false,
            error_message: ptr::null_mut(),
            is_retryable: false,
        }
    }

    // As above but starts successful, so an error-path test proves the call
    // flipped it to failure.
    fn presumed_success_result() -> CResult {
        CResult {
            success: true,
            error_message: ptr::null_mut(),
            is_retryable: false,
        }
    }

    // Rebuild a MessageDescriptor from the bare DescriptorProto bytes the handle
    // exposes, so a test can decode encoded records back and assert field values
    // — proving the descriptor given to the server and the encoder agree.
    fn message_descriptor_from_bytes(descriptor_bytes: &[u8]) -> MessageDescriptor {
        let descriptor = prost_types::DescriptorProto::decode(descriptor_bytes).unwrap();
        let name = descriptor.name().to_string();
        let file = prost_types::FileDescriptorProto {
            name: Some("test.proto".to_string()),
            message_type: vec![descriptor],
            ..Default::default()
        };
        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(file).unwrap();
        pool.get_message_by_name(&name).unwrap()
    }

    #[test]
    fn test_proto_schema_from_uc_json_roundtrip() {
        let json = sample_uc_table_json();
        let mut result = unwritten_result();
        let schema = zerobus_proto_schema_from_uc_json(json.as_ptr(), &mut result as *mut CResult);
        assert!(!schema.is_null(), "schema build failed");
        assert!(result.success);

        // Descriptor bytes must decode to the bare DescriptorProto that the
        // server is given via zerobus_sdk_create_stream.
        let mut dlen: usize = 0;
        let dptr = zerobus_proto_schema_descriptor_bytes(schema, &mut dlen as *mut usize);
        assert!(!dptr.is_null());
        assert!(dlen > 0);
        let desc_bytes = unsafe { std::slice::from_raw_parts(dptr, dlen) };
        let descriptor = prost_types::DescriptorProto::decode(desc_bytes).unwrap();
        // schema_name + table_name, sanitized to PascalCase.
        assert_eq!(descriptor.name(), "AnalyticsEvents");
        assert_eq!(descriptor.field.len(), 3);

        // Encode a record; unknown keys are ignored, timestamps are integers.
        let record =
            CString::new(r#"{"id": 7, "payload": "hello", "ts": 1700000000000000, "extra": "x"}"#)
                .unwrap();
        let mut out_data: *mut u8 = ptr::null_mut();
        let mut out_len: usize = 0;
        let mut enc_result = unwritten_result();
        let ok = zerobus_proto_schema_encode_json(
            schema,
            record.as_ptr(),
            &mut out_data as *mut *mut u8,
            &mut out_len as *mut usize,
            &mut enc_result as *mut CResult,
        );
        assert!(ok, "encode failed");
        assert!(enc_result.success);
        assert!(!out_data.is_null());
        assert!(out_len > 0);

        // Decode the encoded bytes against the same descriptor and assert the
        // values round-trip: the encoding is correct, not merely non-empty.
        let encoded = unsafe { std::slice::from_raw_parts(out_data, out_len) };
        let msg_desc = message_descriptor_from_bytes(desc_bytes);
        let decoded = DynamicMessage::decode(msg_desc, encoded).unwrap();
        assert_eq!(decoded.get_field_by_name("id").unwrap().as_i64(), Some(7));
        assert_eq!(
            decoded.get_field_by_name("payload").unwrap().as_str(),
            Some("hello")
        );
        assert_eq!(
            decoded.get_field_by_name("ts").unwrap().as_i64(),
            Some(1700000000000000)
        );

        zerobus_free_proto_bytes(out_data, out_len);
        zerobus_proto_schema_free(schema);
    }

    #[test]
    fn test_proto_schema_from_uc_json_invalid_json_errors() {
        let bad = CString::new("not json").unwrap();
        let mut result = presumed_success_result();
        let schema = zerobus_proto_schema_from_uc_json(bad.as_ptr(), &mut result as *mut CResult);
        assert!(schema.is_null());
        assert!(!result.success);
        // A parse failure is a caller error, not a transient one. Assert on the
        // error code rather than the message text, which is free to change.
        assert!(!result.is_retryable);
        assert!(!result.error_message.is_null());
        zerobus_free_error_message(result.error_message);
    }

    #[test]
    fn test_proto_schema_from_uc_json_unsupported_type_errors() {
        // Parses cleanly into UcTableSchema but carries a column type the
        // descriptor builder rejects — exercises the schema-conversion error
        // path, distinct from a JSON parse failure.
        let json = CString::new(
            r#"{
                "name": "t", "catalog_name": "c", "schema_name": "s",
                "columns": [
                    {"name": "x", "type_name": "GEOGRAPHY", "type_text": "geography", "nullable": true, "position": 0}
                ]
            }"#,
        )
        .unwrap();
        let mut result = presumed_success_result();
        let schema = zerobus_proto_schema_from_uc_json(json.as_ptr(), &mut result as *mut CResult);
        assert!(schema.is_null());
        assert!(!result.success);
        assert!(!result.error_message.is_null());
        zerobus_free_error_message(result.error_message);
    }

    #[test]
    fn test_proto_schema_from_uc_json_null_input_errors() {
        let mut result = presumed_success_result();
        let schema = zerobus_proto_schema_from_uc_json(ptr::null(), &mut result as *mut CResult);
        assert!(schema.is_null());
        assert!(!result.success);
        zerobus_free_error_message(result.error_message);
    }

    #[test]
    fn test_proto_schema_descriptor_bytes_null_handle() {
        // A null handle must yield a null pointer and zero the out-length so the
        // caller never reads a stale length.
        let mut len: usize = 123;
        let dptr = zerobus_proto_schema_descriptor_bytes(ptr::null(), &mut len as *mut usize);
        assert!(dptr.is_null());
        assert_eq!(len, 0);
    }

    #[test]
    fn test_proto_schema_descriptor_bytes_null_out_len() {
        // The descriptor bytes are not null-terminated, so a null out_len leaves
        // the caller no way to size them. A valid handle must still yield a null
        // pointer rather than a length-less buffer.
        let json = sample_uc_table_json();
        let mut build = unwritten_result();
        let schema = zerobus_proto_schema_from_uc_json(json.as_ptr(), &mut build as *mut CResult);
        assert!(!schema.is_null(), "schema build failed");

        let dptr = zerobus_proto_schema_descriptor_bytes(schema, ptr::null_mut());
        assert!(dptr.is_null());

        zerobus_proto_schema_free(schema);
    }

    #[test]
    fn test_proto_schema_encode_null_schema_errors() {
        let record = CString::new(r#"{"id": 1}"#).unwrap();
        // Seed the outputs with non-null/non-zero sentinels: a failed call must
        // clear them so a caller that frees on error hits a no-op. The schema
        // check fails before any encoding, exercising the earliest failure path.
        let mut sentinel: u8 = 0;
        let mut out_data: *mut u8 = &mut sentinel as *mut u8;
        let mut out_len: usize = 999;
        let mut result = presumed_success_result();
        let ok = zerobus_proto_schema_encode_json(
            ptr::null(),
            record.as_ptr(),
            &mut out_data as *mut *mut u8,
            &mut out_len as *mut usize,
            &mut result as *mut CResult,
        );
        assert!(!ok);
        assert!(!result.success);
        assert!(out_data.is_null(), "outputs must be cleared on failure");
        assert_eq!(out_len, 0, "outputs must be cleared on failure");
        zerobus_free_error_message(result.error_message);
    }

    #[test]
    fn test_proto_schema_encode_malformed_record_errors() {
        let json = sample_uc_table_json();
        let mut build = unwritten_result();
        let schema = zerobus_proto_schema_from_uc_json(json.as_ptr(), &mut build as *mut CResult);
        assert!(!schema.is_null());

        let bad_record = CString::new("{ not valid json").unwrap();
        let mut out_data: *mut u8 = ptr::null_mut();
        let mut out_len: usize = 0;
        let mut enc_result = presumed_success_result();
        let ok = zerobus_proto_schema_encode_json(
            schema,
            bad_record.as_ptr(),
            &mut out_data as *mut *mut u8,
            &mut out_len as *mut usize,
            &mut enc_result as *mut CResult,
        );
        assert!(!ok);
        assert!(!enc_result.success);
        assert!(!enc_result.error_message.is_null());
        assert!(out_data.is_null(), "no buffer should be allocated on error");
        assert_eq!(out_len, 0, "length must be cleared on error");
        zerobus_free_error_message(enc_result.error_message);
        zerobus_proto_schema_free(schema);
    }

    #[test]
    fn test_proto_schema_encode_null_out_pointers_errors() {
        let json = sample_uc_table_json();
        let mut build = unwritten_result();
        let schema = zerobus_proto_schema_from_uc_json(json.as_ptr(), &mut build as *mut CResult);
        assert!(!schema.is_null());

        let record = CString::new(r#"{"id": 1}"#).unwrap();
        let mut enc_result = presumed_success_result();
        let ok = zerobus_proto_schema_encode_json(
            schema,
            record.as_ptr(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut enc_result as *mut CResult,
        );
        assert!(!ok);
        assert!(!enc_result.success);
        zerobus_free_error_message(enc_result.error_message);
        zerobus_proto_schema_free(schema);
    }

    #[test]
    fn test_proto_schema_encode_missing_required_field_errors() {
        // `id` is non-nullable (proto2 `required`); a record omitting it must be
        // rejected rather than encoded.
        let json = sample_uc_table_json();
        let mut build = unwritten_result();
        let schema = zerobus_proto_schema_from_uc_json(json.as_ptr(), &mut build as *mut CResult);
        assert!(!schema.is_null());

        let record = CString::new(r#"{"payload": "hello"}"#).unwrap();
        let mut out_data: *mut u8 = ptr::null_mut();
        let mut out_len: usize = 0;
        let mut enc_result = presumed_success_result();
        let ok = zerobus_proto_schema_encode_json(
            schema,
            record.as_ptr(),
            &mut out_data as *mut *mut u8,
            &mut out_len as *mut usize,
            &mut enc_result as *mut CResult,
        );
        assert!(!ok);
        assert!(!enc_result.success);
        // A missing required field is a caller error, not a transient one. Assert
        // on the error code rather than the message text, which is free to change.
        assert!(!enc_result.is_retryable);
        assert!(!enc_result.error_message.is_null());
        assert!(out_data.is_null(), "no buffer should be allocated on error");
        assert_eq!(out_len, 0, "length must be cleared on error");
        zerobus_free_error_message(enc_result.error_message);
        zerobus_proto_schema_free(schema);
    }

    // UC table JSON for the type-contract tests: a required key plus one column
    // of the type under test.
    fn uc_table_json_with_column(col_name: &str, type_name: &str) -> CString {
        let json = format!(
            r#"{{
                "name": "t", "catalog_name": "c", "schema_name": "s",
                "columns": [
                    {{"name": "k", "type_name": "BIGINT", "type_text": "bigint", "nullable": false, "position": 0}},
                    {{"name": "{col_name}", "type_name": "{type_name}", "type_text": "{type_name}", "nullable": true, "position": 1}}
                ]
            }}"#
        );
        CString::new(json).unwrap()
    }

    // Build a schema + encode one record, returning the decoded message so a test
    // can assert how a given JSON value lands on the wire.
    fn encode_and_decode(table_json: &CString, record_json: &str) -> DynamicMessage {
        let mut build = unwritten_result();
        let schema =
            zerobus_proto_schema_from_uc_json(table_json.as_ptr(), &mut build as *mut CResult);
        assert!(!schema.is_null(), "schema build failed");

        let mut dlen: usize = 0;
        let dptr = zerobus_proto_schema_descriptor_bytes(schema, &mut dlen as *mut usize);
        let desc_bytes = unsafe { std::slice::from_raw_parts(dptr, dlen) };
        let msg_desc = message_descriptor_from_bytes(desc_bytes);

        let record = CString::new(record_json).unwrap();
        let mut out_data: *mut u8 = ptr::null_mut();
        let mut out_len: usize = 0;
        let mut enc_result = unwritten_result();
        let ok = zerobus_proto_schema_encode_json(
            schema,
            record.as_ptr(),
            &mut out_data as *mut *mut u8,
            &mut out_len as *mut usize,
            &mut enc_result as *mut CResult,
        );
        assert!(ok, "encode failed");
        let encoded = unsafe { std::slice::from_raw_parts(out_data, out_len) };
        let decoded = DynamicMessage::decode(msg_desc, encoded).unwrap();

        zerobus_free_proto_bytes(out_data, out_len);
        zerobus_proto_schema_free(schema);
        decoded
    }

    #[test]
    fn test_proto_schema_encode_binary_is_base64_string() {
        // BINARY maps to proto `bytes`; prost-reflect's serde layer accepts it
        // only as a base64-encoded string (not a JSON array of byte values).
        let table = uc_table_json_with_column("blob", "BINARY");
        // "aGVsbG8=" is base64 for "hello".
        let decoded = encode_and_decode(&table, r#"{"k": 1, "blob": "aGVsbG8="}"#);
        assert_eq!(
            decoded
                .get_field_by_name("blob")
                .unwrap()
                .as_bytes()
                .map(|b| b.as_ref()),
            Some(b"hello".as_slice())
        );
    }

    #[test]
    fn test_proto_schema_encode_decimal_is_string() {
        // DECIMAL maps to proto `string`; the value must be passed as a JSON
        // string to preserve precision and scale.
        let table = uc_table_json_with_column("price", "DECIMAL");
        let decoded = encode_and_decode(&table, r#"{"k": 1, "price": "123.45"}"#);
        assert_eq!(
            decoded.get_field_by_name("price").unwrap().as_str(),
            Some("123.45")
        );
    }

    #[test]
    fn test_proto_schema_encode_large_int64_as_string_preserves_precision() {
        // int64 above 2^53 loses precision as a JSON number; passing it as a
        // string round-trips exactly.
        let table = uc_table_json_with_column("big", "BIGINT");
        let decoded = encode_and_decode(&table, r#"{"k": 1, "big": "9223372036854775807"}"#);
        assert_eq!(
            decoded.get_field_by_name("big").unwrap().as_i64(),
            Some(9223372036854775807)
        );
    }

    #[test]
    fn test_proto_schema_encode_variant_is_json_encoded_string() {
        // VARIANT maps to proto `string`; the value is a JSON-encoded string
        // (a string whose contents are the variant's JSON).
        let table = uc_table_json_with_column("v", "VARIANT");
        let decoded = encode_and_decode(&table, r#"{"k": 1, "v": "{\"a\":1,\"b\":[2,3]}"}"#);
        assert_eq!(
            decoded.get_field_by_name("v").unwrap().as_str(),
            Some(r#"{"a":1,"b":[2,3]}"#)
        );
    }

    #[test]
    fn test_proto_schema_encode_array_is_json_array() {
        // ARRAY<T> maps to `repeated T`; the value is a JSON array. Complex
        // columns carry their shape in `type_json`, so build the table directly.
        let table = CString::new(
            r#"{
                "name": "t", "catalog_name": "c", "schema_name": "s",
                "columns": [
                    {"name": "k", "type_name": "BIGINT", "type_text": "bigint", "nullable": false, "position": 0},
                    {"name": "tags", "type_name": "ARRAY", "type_text": "array<int>", "nullable": true, "position": 1,
                     "type_json": "{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":false}"}
                ]
            }"#,
        )
        .unwrap();
        let decoded = encode_and_decode(&table, r#"{"k": 1, "tags": [10, 20, 30]}"#);
        let list = decoded.get_field_by_name("tags").unwrap();
        let values: Vec<i64> = list
            .as_list()
            .unwrap()
            .iter()
            .map(|v| v.as_i32().unwrap() as i64)
            .collect();
        assert_eq!(values, vec![10, 20, 30]);
    }

    #[test]
    fn test_proto_schema_encode_map_roundtrip() {
        // MAP<K,V> maps to a synthetic map-entry message + `repeated`; the value
        // is a JSON object. Protobuf-JSON map keys are always strings on the wire.
        let table = CString::new(
            r#"{
                "name": "t", "catalog_name": "c", "schema_name": "s",
                "columns": [
                    {"name": "k", "type_name": "BIGINT", "type_text": "bigint", "nullable": false, "position": 0},
                    {"name": "attrs", "type_name": "MAP", "type_text": "map<string,int>", "nullable": true, "position": 1,
                     "type_json": "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"integer\",\"valueContainsNull\":false}"}
                ]
            }"#,
        )
        .unwrap();
        let decoded = encode_and_decode(&table, r#"{"k": 1, "attrs": {"a": 1, "b": 2}}"#);
        let field = decoded.get_field_by_name("attrs").unwrap();
        let map = field.as_map().unwrap();
        let mut pairs: Vec<(String, i32)> = map
            .iter()
            .map(|(k, v)| (k.as_str().unwrap().to_string(), v.as_i32().unwrap()))
            .collect();
        pairs.sort();
        assert_eq!(pairs, vec![("a".to_string(), 1), ("b".to_string(), 2)]);
    }

    #[test]
    fn test_proto_schema_encode_struct_roundtrip() {
        // STRUCT<...> maps to a nested message; the value is a JSON object.
        let table = CString::new(
            r#"{
                "name": "t", "catalog_name": "c", "schema_name": "s",
                "columns": [
                    {"name": "k", "type_name": "BIGINT", "type_text": "bigint", "nullable": false, "position": 0},
                    {"name": "addr", "type_name": "STRUCT", "type_text": "struct<city:string,zip:int>", "nullable": true, "position": 1,
                     "type_json": "{\"type\":\"struct\",\"fields\":[{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"zip\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}"}
                ]
            }"#,
        )
        .unwrap();
        let decoded =
            encode_and_decode(&table, r#"{"k": 1, "addr": {"city": "NYC", "zip": 10001}}"#);
        let field = decoded.get_field_by_name("addr").unwrap();
        let addr = field.as_message().unwrap();
        assert_eq!(
            addr.get_field_by_name("city").unwrap().as_str(),
            Some("NYC")
        );
        assert_eq!(addr.get_field_by_name("zip").unwrap().as_i32(), Some(10001));
    }

    #[test]
    fn test_proto_schema_encode_array_of_struct_roundtrip() {
        // ARRAY<STRUCT<...>> maps to `repeated <nested message>`; the value is a
        // JSON array of objects.
        let table = CString::new(
            r#"{
                "name": "t", "catalog_name": "c", "schema_name": "s",
                "columns": [
                    {"name": "k", "type_name": "BIGINT", "type_text": "bigint", "nullable": false, "position": 0},
                    {"name": "items", "type_name": "ARRAY", "type_text": "array<struct<id:int>>", "nullable": true, "position": 1,
                     "type_json": "{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]},\"containsNull\":false}"}
                ]
            }"#,
        )
        .unwrap();
        let decoded = encode_and_decode(&table, r#"{"k": 1, "items": [{"id": 1}, {"id": 2}]}"#);
        let field = decoded.get_field_by_name("items").unwrap();
        let ids: Vec<i32> = field
            .as_list()
            .unwrap()
            .iter()
            .map(|v| {
                v.as_message()
                    .unwrap()
                    .get_field_by_name("id")
                    .unwrap()
                    .as_i32()
                    .unwrap()
            })
            .collect();
        assert_eq!(ids, vec![1, 2]);
    }

    #[test]
    fn test_proto_schema_encode_date_is_days_since_epoch() {
        // DATE maps to proto `int32`; the value is days since the Unix epoch, an
        // integer (not an ISO-8601 string). 19000 days ≈ 2022-01-08.
        let table = uc_table_json_with_column("d", "DATE");
        let decoded = encode_and_decode(&table, r#"{"k": 1, "d": 19000}"#);
        assert_eq!(
            decoded.get_field_by_name("d").unwrap().as_i32(),
            Some(19000)
        );
    }

    #[test]
    fn test_proto_schema_encode_timestamp_ntz_is_micros() {
        // TIMESTAMP_NTZ maps to proto `int64` (same wire shape as TIMESTAMP); the
        // value is microseconds since the epoch, an integer.
        let table = uc_table_json_with_column("tsn", "TIMESTAMP_NTZ");
        let decoded = encode_and_decode(&table, r#"{"k": 1, "tsn": 1700000000000000}"#);
        assert_eq!(
            decoded.get_field_by_name("tsn").unwrap().as_i64(),
            Some(1700000000000000)
        );
    }

    #[test]
    fn test_free_proto_bytes_handles_empty_encoding() {
        // A record with no fields set encodes to zero bytes: out_len == 0 but
        // out_data is a non-null, zero-length boxed slice. Freeing it must
        // reclaim that allocation, not leak it.
        let table = CString::new(
            r#"{
                "name": "t", "catalog_name": "c", "schema_name": "s",
                "columns": [
                    {"name": "opt", "type_name": "INT", "type_text": "int", "nullable": true, "position": 0}
                ]
            }"#,
        )
        .unwrap();
        let mut build = unwritten_result();
        let schema = zerobus_proto_schema_from_uc_json(table.as_ptr(), &mut build as *mut CResult);
        assert!(!schema.is_null(), "schema build failed");

        let record = CString::new(r#"{}"#).unwrap();
        let mut out_data: *mut u8 = ptr::null_mut();
        let mut out_len: usize = 0;
        let mut enc_result = unwritten_result();
        let ok = zerobus_proto_schema_encode_json(
            schema,
            record.as_ptr(),
            &mut out_data as *mut *mut u8,
            &mut out_len as *mut usize,
            &mut enc_result as *mut CResult,
        );
        assert!(ok, "encode failed");
        assert_eq!(
            out_len, 0,
            "record with no fields set should encode to zero bytes"
        );
        assert!(
            !out_data.is_null(),
            "buffer pointer should be non-null even when empty"
        );

        // The assertion is the absence of a leak/crash on free.
        zerobus_free_proto_bytes(out_data, out_len);
        zerobus_proto_schema_free(schema);
    }

    #[test]
    fn test_proto_schema_shared_across_threads() {
        // The handle may be shared by concurrent readers: many threads encode
        // through one handle at once. `free` is ordered after every worker has
        // joined, so it never races an in-flight encode.
        use std::thread;

        let json = sample_uc_table_json();
        let mut build = unwritten_result();
        let schema = zerobus_proto_schema_from_uc_json(json.as_ptr(), &mut build as *mut CResult);
        assert!(!schema.is_null(), "schema build failed");

        // Raw pointers aren't Send; pass the address as a usize and rebuild it
        // per thread. Safe: threads only read, and the handle outlives them.
        let handle_addr = schema as usize;
        let mut workers = Vec::new();
        for t in 0..8 {
            workers.push(thread::spawn(move || {
                let handle = handle_addr as *const crate::CZerobusProtoSchema;
                for i in 0..200 {
                    let record = CString::new(format!(
                        r#"{{"id": {}, "payload": "p{}"}}"#,
                        t * 1000 + i,
                        i
                    ))
                    .unwrap();
                    let mut out_data: *mut u8 = ptr::null_mut();
                    let mut out_len: usize = 0;
                    let mut enc = unwritten_result();
                    let ok = zerobus_proto_schema_encode_json(
                        handle,
                        record.as_ptr(),
                        &mut out_data as *mut *mut u8,
                        &mut out_len as *mut usize,
                        &mut enc as *mut CResult,
                    );
                    assert!(ok, "concurrent encode failed");
                    zerobus_free_proto_bytes(out_data, out_len);
                }
            }));
        }
        for w in workers {
            w.join().unwrap();
        }
        zerobus_proto_schema_free(schema);
    }

    // ========================================================================
    // Panic guard (ffi_guard) tests
    // ========================================================================
    //
    // `ffi_guard` is the shared wrapper each `#[no_mangle] extern "C"` entry
    // point runs its body through: a panic inside the body must be caught and
    // turned into (failure sentinel + populated CResult) instead of crossing the
    // `extern "C"` boundary (which aborts the process). These cover each
    // return-signature shape, the no-CResult path, and a panic driven through an
    // `extern "C"` function built like the real entry points.

    // Read a CResult, free its message, and return (success, is_retryable, msg).
    fn drain_result(result: &mut CResult) -> (bool, bool, String) {
        let msg = if result.error_message.is_null() {
            String::new()
        } else {
            let s = unsafe { CStr::from_ptr(result.error_message) }
                .to_string_lossy()
                .into_owned();
            unsafe {
                let _ = CString::from_raw(result.error_message);
            }
            result.error_message = ptr::null_mut();
            s
        };
        (result.success, result.is_retryable, msg)
    }

    #[test]
    fn test_ffi_guard_passes_value_through_on_success() {
        let mut result = unwritten_result();
        let rp = &mut result as *mut CResult;
        let out = ffi_guard(rp, -1, || {
            write_success_result(rp);
            42_i64
        });
        assert_eq!(out, 42);
        let (success, _, msg) = drain_result(&mut result);
        // The body's own success result is preserved; the guard does not touch
        // CResult on the happy path.
        assert!(success);
        assert!(msg.is_empty());
    }

    #[test]
    fn test_ffi_guard_pointer_sentinel_on_panic() {
        let mut result = unwritten_result();
        let out: *mut u8 = ffi_guard(&mut result as *mut CResult, ptr::null_mut(), || {
            panic!("boom from a pointer-returning fn");
        });
        assert!(out.is_null());
        let (success, is_retryable, msg) = drain_result(&mut result);
        assert!(!success);
        // Panics are never automatically retryable.
        assert!(!is_retryable);
        assert!(msg.contains("Rust panic caught at FFI boundary"));
        assert!(msg.contains("boom from a pointer-returning fn"));
    }

    #[test]
    fn test_ffi_guard_bool_sentinel_on_panic() {
        let mut result = unwritten_result();
        let out = ffi_guard(&mut result as *mut CResult, false, || -> bool {
            panic!("boom");
        });
        assert!(!out);
        let (success, is_retryable, msg) = drain_result(&mut result);
        assert!(!success);
        assert!(!is_retryable);
        assert!(msg.contains("Rust panic caught at FFI boundary"));
    }

    #[test]
    fn test_ffi_guard_offset_sentinel_on_panic() {
        let mut result = unwritten_result();
        // Mirrors the i64 ingest functions: -1 is their error sentinel.
        let out = ffi_guard(&mut result as *mut CResult, -1_i64, || -> i64 {
            let v: Vec<u8> = Vec::new();
            // Index out of bounds: a panic from deep in library/std code, not an
            // explicit panic!.
            v[5] as i64
        });
        assert_eq!(out, -1);
        let (success, _, msg) = drain_result(&mut result);
        assert!(!success);
        assert!(msg.contains("Rust panic caught at FFI boundary"));
    }

    #[test]
    fn test_ffi_guard_struct_sentinel_on_panic() {
        let mut result = unwritten_result();
        // Mirrors zerobus_stream_get_unacked_records: the sentinel is an empty
        // array struct, which must be returned (not unwound) on panic.
        let out = ffi_guard(
            &mut result as *mut CResult,
            CRecordArray {
                records: ptr::null_mut(),
                len: 0,
            },
            || -> CRecordArray {
                panic!("boom while building the record array");
            },
        );
        assert!(out.records.is_null());
        assert_eq!(out.len, 0);
        let (success, _, msg) = drain_result(&mut result);
        assert!(!success);
        assert!(msg.contains("Rust panic caught at FFI boundary"));
    }

    #[test]
    fn test_ffi_guard_null_result_pointer_is_safe_on_panic() {
        // Functions without a CResult out-parameter pass a null result pointer;
        // a panic must still be swallowed and the sentinel returned (here the
        // is_closed-style `true`), never aborting.
        let out = ffi_guard(ptr::null_mut(), true, || -> bool {
            panic!("boom with nowhere to report");
        });
        assert!(out);
    }

    #[test]
    fn test_ffi_guard_reports_string_payload_panic() {
        // A `panic!("{}", ..)` carries a String payload (vs. the &'static str of
        // a literal panic). Both must be surfaced in the CResult message; this
        // covers the String arm of panic_message.
        let mut result = unwritten_result();
        let detail = "dynamic detail 123".to_string();
        let out = ffi_guard(&mut result as *mut CResult, false, move || -> bool {
            panic!("formatted: {detail}");
        });
        assert!(!out);
        let (success, _, msg) = drain_result(&mut result);
        assert!(!success);
        assert!(msg.contains("formatted: dynamic detail 123"));
    }

    // A pointer-returning `extern "C"` entry point built exactly like the real
    // ones: its whole body runs inside `ffi_guard`. Driving it into a panic
    // exercises the guard across the actual C ABI, not just the helper in
    // isolation.
    extern "C" fn panicking_entry_point(result: *mut CResult) -> *mut u8 {
        ffi_guard(result, ptr::null_mut(), || {
            // Stand in for an unexpected panic from deep in a dependency.
            let v: Vec<u8> = Vec::new();
            ptr::null_mut::<u8>().wrapping_add(v[0] as usize)
        })
    }

    #[test]
    fn test_ffi_guard_catches_panic_through_extern_c_entry_point() {
        let mut result = unwritten_result();
        // Invoke through the C ABI exactly as a C/Go/Java caller would.
        let entry: extern "C" fn(*mut CResult) -> *mut u8 = panicking_entry_point;
        let out = entry(&mut result as *mut CResult);
        assert!(out.is_null());
        let (success, is_retryable, msg) = drain_result(&mut result);
        assert!(!success);
        assert!(!is_retryable);
        assert!(msg.contains("Rust panic caught at FFI boundary"));
    }

    // Guard-coverage check: every `#[no_mangle] pub extern "C"` entry point must
    // run its body through `ffi_guard`. Scanning the source (rather than the live
    // symbols) is what lets this fail when a *new* entry point is added later
    // without the guard — the regression the panic tests above can't catch on
    // their own. A module gaining entry points must be added to `modules` below.
    #[test]
    fn test_every_extern_c_entry_point_is_guarded() {
        let modules = [
            ("sdk.rs", include_str!("sdk.rs")),
            ("stream.rs", include_str!("stream.rs")),
            ("builder.rs", include_str!("builder.rs")),
            ("proto_schema.rs", include_str!("proto_schema.rs")),
            ("arrow.rs", include_str!("arrow.rs")),
            ("common.rs", include_str!("common.rs")),
        ];
        // Entry points whose body builds a `#[repr(C)]` struct from compile-time
        // constants, does nothing, or is a pure allocator that returns null on
        // failure — no panic-capable operation, so the guard is intentionally
        // omitted (documented at each definition).
        let allowed_unguarded = [
            "zerobus_get_default_config",
            "zerobus_arrow_get_default_config",
            "zerobus_sdk_set_use_tls",
            "zerobus_alloc_header_array",
            "zerobus_alloc_cstring",
        ];

        let mut offenders = Vec::new();
        for (module, src) in modules {
            // Each chunk after the marker spans one entry point's text up to the
            // next one (or EOF), so a guard call cannot leak across functions.
            for chunk in src.split("pub extern \"C\" fn ").skip(1) {
                let name: String = chunk
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                if allowed_unguarded.contains(&name.as_str()) {
                    continue;
                }
                if !chunk.contains("ffi_guard(") {
                    offenders.push(format!("{module}::{name}"));
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "these `extern \"C\"` entry points don't run their body through \
             ffi_guard (wrap them, or add to allowed_unguarded if intentionally \
             panic-free): {offenders:?}"
        );
    }

    // Encode `record_json` against `table_json`, asserting the encode fails
    // cleanly (non-retryable, output pointers cleared) and returning the error.
    fn encode_expecting_error(table_json: &CString, record_json: &str) -> String {
        let mut build = unwritten_result();
        let schema =
            zerobus_proto_schema_from_uc_json(table_json.as_ptr(), &mut build as *mut CResult);
        assert!(!schema.is_null(), "schema build failed");

        let record = CString::new(record_json).unwrap();
        let mut out_data: *mut u8 = ptr::null_mut();
        let mut out_len: usize = 0;
        let mut enc = presumed_success_result();
        let ok = zerobus_proto_schema_encode_json(
            schema,
            record.as_ptr(),
            &mut out_data as *mut *mut u8,
            &mut out_len as *mut usize,
            &mut enc as *mut CResult,
        );
        assert!(!ok, "expected encode to fail");
        assert!(!enc.success);
        assert!(
            !enc.is_retryable,
            "a missing required field is a caller error"
        );
        assert!(out_data.is_null(), "no buffer should be allocated on error");
        assert_eq!(out_len, 0, "length must be cleared on error");
        assert!(!enc.error_message.is_null());
        let msg = unsafe { CStr::from_ptr(enc.error_message) }
            .to_string_lossy()
            .into_owned();
        zerobus_free_error_message(enc.error_message);
        zerobus_proto_schema_free(schema);
        msg
    }

    #[test]
    fn test_proto_schema_encode_missing_required_field_in_struct_errors() {
        // `addr.zip` is non-nullable; a record that supplies `addr` but omits
        // `zip` must fail locally rather than encode bytes the server rejects.
        let table = CString::new(
            r#"{
                "name": "t", "catalog_name": "c", "schema_name": "s",
                "columns": [
                    {"name": "k", "type_name": "BIGINT", "type_text": "bigint", "nullable": false, "position": 0},
                    {"name": "addr", "type_name": "STRUCT", "type_text": "struct", "nullable": false, "position": 1,
                     "type_json": "{\"type\":\"struct\",\"fields\":[{\"name\":\"zip\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"}
                ]
            }"#,
        )
        .unwrap();
        let msg = encode_expecting_error(&table, r#"{"k": 1, "addr": {"city": "boston"}}"#);
        assert!(
            msg.contains("addr.zip"),
            "error should name the nested path, got: {msg}"
        );
    }

    #[test]
    fn test_proto_schema_encode_missing_required_field_in_array_struct_errors() {
        // Each ARRAY<STRUCT> element is validated independently: element [1]
        // omits the required `id` while element [0] is complete.
        let table = CString::new(
            r#"{
                "name": "t", "catalog_name": "c", "schema_name": "s",
                "columns": [
                    {"name": "k", "type_name": "BIGINT", "type_text": "bigint", "nullable": false, "position": 0},
                    {"name": "items", "type_name": "ARRAY", "type_text": "array", "nullable": true, "position": 1,
                     "type_json": "{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},{\"name\":\"label\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"containsNull\":false}"}
                ]
            }"#,
        )
        .unwrap();
        let msg =
            encode_expecting_error(&table, r#"{"k": 1, "items": [{"id": 5}, {"label": "x"}]}"#);
        assert!(
            msg.contains("items[1].id"),
            "error should name the element path, got: {msg}"
        );
    }

    #[test]
    fn test_proto_schema_encode_missing_required_field_in_map_value_errors() {
        // MAP<K, STRUCT> values are validated: the value at key `work` omits the
        // required `v` while the value at `home` is complete.
        let table = CString::new(
            r#"{
                "name": "t", "catalog_name": "c", "schema_name": "s",
                "columns": [
                    {"name": "k", "type_name": "BIGINT", "type_text": "bigint", "nullable": false, "position": 0},
                    {"name": "lookup", "type_name": "MAP", "type_text": "map", "nullable": true, "position": 1,
                     "type_json": "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"v\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}}]},\"valueContainsNull\":false}"}
                ]
            }"#,
        )
        .unwrap();
        let msg = encode_expecting_error(
            &table,
            r#"{"k": 1, "lookup": {"home": {"v": 2}, "work": {}}}"#,
        );
        assert!(
            msg.contains("lookup[work].v"),
            "error should name the map-value path, got: {msg}"
        );
    }

    #[test]
    fn test_proto_schema_encode_nested_required_fields_present_succeeds() {
        // With all nested required fields present, the record is accepted and
        // the nested values round-trip.
        let table = CString::new(
            r#"{
                "name": "t", "catalog_name": "c", "schema_name": "s",
                "columns": [
                    {"name": "k", "type_name": "BIGINT", "type_text": "bigint", "nullable": false, "position": 0},
                    {"name": "addr", "type_name": "STRUCT", "type_text": "struct", "nullable": false, "position": 1,
                     "type_json": "{\"type\":\"struct\",\"fields\":[{\"name\":\"zip\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}]}"},
                    {"name": "items", "type_name": "ARRAY", "type_text": "array", "nullable": true, "position": 2,
                     "type_json": "{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}}]},\"containsNull\":false}"},
                    {"name": "lookup", "type_name": "MAP", "type_text": "map", "nullable": true, "position": 3,
                     "type_json": "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"v\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}}]},\"valueContainsNull\":false}"}
                ]
            }"#,
        )
        .unwrap();
        let decoded = encode_and_decode(
            &table,
            r#"{"k": 1, "addr": {"zip": 90210}, "items": [{"id": 7}], "lookup": {"home": {"v": 3}}}"#,
        );
        let addr = decoded.get_field_by_name("addr").unwrap();
        assert_eq!(
            addr.as_message()
                .unwrap()
                .get_field_by_name("zip")
                .unwrap()
                .as_i32(),
            Some(90210)
        );
    }

    #[test]
    fn test_proto_schema_encode_missing_required_field_deeply_nested_errors() {
        // The walk recurses to arbitrary depth: a required field three levels
        // down (`addr.geo.lat`) and one through an array element's nested struct
        // (`items[0].inner.id`) must both be reported.
        let table = CString::new(
            r#"{
                "name": "t", "catalog_name": "c", "schema_name": "s",
                "columns": [
                    {"name": "addr", "type_name": "STRUCT", "type_text": "struct", "nullable": false, "position": 0,
                     "type_json": "{\"type\":\"struct\",\"fields\":[{\"name\":\"geo\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"lat\",\"type\":\"double\",\"nullable\":false,\"metadata\":{}}]},\"nullable\":false,\"metadata\":{}}]}"},
                    {"name": "items", "type_name": "ARRAY", "type_text": "array", "nullable": true, "position": 1,
                     "type_json": "{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"inner\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}}]},\"nullable\":false,\"metadata\":{}}]},\"containsNull\":false}"}
                ]
            }"#,
        )
        .unwrap();
        let msg =
            encode_expecting_error(&table, r#"{"addr": {"geo": {}}, "items": [{"inner": {}}]}"#);
        assert!(
            msg.contains("addr.geo.lat"),
            "should report the 3-level-deep path, got: {msg}"
        );
        assert!(
            msg.contains("items[0].inner.id"),
            "should report the path through an array element's nested struct, got: {msg}"
        );
    }
}
