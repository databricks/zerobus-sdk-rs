#[allow(dead_code)]
mod mock_arrow_flight;
mod utils;

use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StructArray};
use arrow_schema::Schema;
use databricks_zerobus_ingest_sdk::internal::arrow_c_data::{FFI_ArrowArray, FFI_ArrowSchema};

use crate::mock_arrow_flight::{start_mock_flight_server, MockFlightResponse};
use crate::utils::{create_test_arrow_schema, create_test_record_batch};

const TABLE_NAME: &str = "test_catalog.test_schema.test_table";

mod ffi_c_data_lifetime_tests {
    use std::ffi::{c_void, CStr, CString};
    use std::ptr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tonic::Status;
    use zerobus_ffi::{
        zerobus_arrow_get_default_config, zerobus_arrow_stream_flush, zerobus_arrow_stream_free,
        zerobus_arrow_stream_ingest_c_data, zerobus_free_error_message, zerobus_sdk_builder_build,
        zerobus_sdk_builder_disable_tls, zerobus_sdk_builder_endpoint, zerobus_sdk_builder_new,
        zerobus_sdk_create_arrow_stream_with_headers_provider, zerobus_sdk_free, CArrowArray,
        CArrowSchema, CHeaders, CResult,
    };

    use super::{
        create_test_arrow_schema, create_test_record_batch, start_mock_flight_server, Arc, Array,
        FFI_ArrowArray, FFI_ArrowSchema, MockFlightResponse, RecordBatch, Schema, StructArray,
        TABLE_NAME,
    };

    struct CountingArrayRelease {
        inner_release: Option<unsafe extern "C" fn(*mut FFI_ArrowArray)>,
        inner_private_data: *mut c_void,
        releases: Arc<AtomicUsize>,
    }

    struct CountingSchemaRelease {
        inner_release: Option<unsafe extern "C" fn(*mut FFI_ArrowSchema)>,
        inner_private_data: *mut c_void,
        releases: Arc<AtomicUsize>,
    }

    unsafe extern "C" fn counting_array_release(array: *mut FFI_ArrowArray) {
        let array = unsafe { &mut *array };
        let state = unsafe { Box::from_raw(array.private_data.cast::<CountingArrayRelease>()) };
        array.release = state.inner_release;
        array.private_data = state.inner_private_data;
        state.releases.fetch_add(1, Ordering::SeqCst);
        if let Some(release) = state.inner_release {
            unsafe { release(array) };
        }
    }

    unsafe extern "C" fn counting_schema_release(schema: *mut FFI_ArrowSchema) {
        let schema = unsafe { &mut *schema };
        let state = unsafe { Box::from_raw(schema.private_data.cast::<CountingSchemaRelease>()) };
        schema.release = state.inner_release;
        schema.private_data = state.inner_private_data;
        state.releases.fetch_add(1, Ordering::SeqCst);
        if let Some(release) = state.inner_release {
            unsafe { release(schema) };
        }
    }

    fn exported_counting_batch(
        batch: RecordBatch,
        array_releases: Arc<AtomicUsize>,
        schema_releases: Arc<AtomicUsize>,
    ) -> (FFI_ArrowArray, FFI_ArrowSchema) {
        let schema = batch.schema();
        let struct_array = StructArray::from(batch);
        let mut array = FFI_ArrowArray::new(&struct_array.to_data());
        let array_state = Box::new(CountingArrayRelease {
            inner_release: array.release,
            inner_private_data: array.private_data,
            releases: array_releases,
        });
        array.release = Some(counting_array_release);
        array.private_data = Box::into_raw(array_state).cast();

        let mut schema = FFI_ArrowSchema::try_from(schema.as_ref()).unwrap();
        let schema_state = Box::new(CountingSchemaRelease {
            inner_release: schema.release,
            inner_private_data: schema.private_data,
            releases: schema_releases,
        });
        schema.release = Some(counting_schema_release);
        schema.private_data = Box::into_raw(schema_state).cast();
        (array, schema)
    }

    fn schema_ipc_bytes(schema: &Schema) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, schema).unwrap();
        writer.finish().unwrap();
        bytes
    }

    extern "C" fn empty_headers(_user_data: *mut c_void) -> CHeaders {
        CHeaders {
            headers: ptr::null_mut(),
            count: 0,
            error_message: ptr::null_mut(),
        }
    }

    fn take_result_error(result: &mut CResult) -> Option<String> {
        if result.error_message.is_null() {
            return None;
        }
        let message = unsafe { CStr::from_ptr(result.error_message) }
            .to_string_lossy()
            .into_owned();
        zerobus_free_error_message(result.error_message);
        result.error_message = ptr::null_mut();
        Some(message)
    }

    async fn create_ffi_stream(
        server_url: String,
        schema: Arc<Schema>,
    ) -> Result<(usize, usize), String> {
        tokio::task::spawn_blocking(move || {
            let endpoint = CString::new(server_url).unwrap();
            let builder = zerobus_sdk_builder_new();
            zerobus_sdk_builder_endpoint(builder, endpoint.as_ptr());
            zerobus_sdk_builder_disable_tls(builder);
            let mut result = CResult {
                success: true,
                error_message: ptr::null_mut(),
                is_retryable: false,
            };
            let sdk = zerobus_sdk_builder_build(builder, &mut result);
            if sdk.is_null() {
                return Err(take_result_error(&mut result)
                    .unwrap_or_else(|| "SDK builder returned null".to_string()));
            }

            let table_name = CString::new(TABLE_NAME).unwrap();
            let schema_bytes = schema_ipc_bytes(schema.as_ref());
            let options = zerobus_arrow_get_default_config();
            let stream = zerobus_sdk_create_arrow_stream_with_headers_provider(
                sdk,
                table_name.as_ptr(),
                schema_bytes.as_ptr(),
                schema_bytes.len(),
                empty_headers,
                ptr::null_mut(),
                None,
                &options,
                &mut result,
            );
            if stream.is_null() {
                let error = take_result_error(&mut result)
                    .unwrap_or_else(|| "Arrow stream builder returned null".to_string());
                zerobus_sdk_free(sdk);
                return Err(error);
            }
            Ok((sdk as usize, stream as usize))
        })
        .await
        .unwrap()
    }

    async fn ingest_ffi_batch(
        stream: usize,
        mut array: FFI_ArrowArray,
        mut schema: FFI_ArrowSchema,
    ) -> Result<i64, String> {
        tokio::task::spawn_blocking(move || {
            let mut result = CResult {
                success: true,
                error_message: ptr::null_mut(),
                is_retryable: false,
            };
            let offset = zerobus_arrow_stream_ingest_c_data(
                stream as *mut _,
                (&mut array as *mut FFI_ArrowArray).cast::<CArrowArray>(),
                (&mut schema as *mut FFI_ArrowSchema).cast::<CArrowSchema>(),
                &mut result,
            );
            assert!(array.release.is_none(), "array ownership must transfer");
            assert!(schema.release.is_none(), "schema ownership must transfer");
            if result.success {
                Ok(offset)
            } else {
                Err(take_result_error(&mut result)
                    .unwrap_or_else(|| "C Data ingest failed without a message".to_string()))
            }
        })
        .await
        .unwrap()
    }

    async fn flush_ffi_stream(stream: usize) -> Result<(), String> {
        tokio::task::spawn_blocking(move || {
            let mut result = CResult {
                success: true,
                error_message: ptr::null_mut(),
                is_retryable: false,
            };
            let flushed = zerobus_arrow_stream_flush(stream as *mut _, &mut result);
            if flushed {
                Ok(())
            } else {
                Err(take_result_error(&mut result)
                    .unwrap_or_else(|| "Arrow stream flush failed without a message".to_string()))
            }
        })
        .await
        .unwrap()
    }

    async fn free_ffi_handles(sdk: usize, stream: usize) {
        tokio::task::spawn_blocking(move || {
            zerobus_arrow_stream_free(stream as *mut _);
            zerobus_sdk_free(sdk as *mut _);
        })
        .await
        .unwrap();
    }

    async fn wait_for_release_count(releases: &AtomicUsize, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while releases.load(Ordering::SeqCst) != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("release callback did not run before timeout");
    }

    #[tokio::test]
    async fn ffi_c_data_owner_releases_once_after_ack_and_flush(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (mock_server, server_url) = start_mock_flight_server().await?;
        let schema = create_test_arrow_schema();
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockFlightResponse::BatchAck {
                    ack_up_to_offset: 0,
                    delay_ms: 200,
                    ack_up_to_records: 3,
                }],
            )
            .await;
        let (sdk, stream) = create_ffi_stream(server_url, Arc::clone(&schema)).await?;
        let array_releases = Arc::new(AtomicUsize::new(0));
        let schema_releases = Arc::new(AtomicUsize::new(0));
        let batch =
            create_test_record_batch(schema, vec![1, 2, 3], vec![Some("a"), Some("b"), Some("c")]);
        let (array, schema) = exported_counting_batch(
            batch,
            Arc::clone(&array_releases),
            Arc::clone(&schema_releases),
        );

        assert_eq!(ingest_ffi_batch(stream, array, schema).await?, 0);
        assert_eq!(schema_releases.load(Ordering::SeqCst), 1);
        assert_eq!(
            array_releases.load(Ordering::SeqCst),
            0,
            "array owner must remain alive while the batch is pending"
        );

        flush_ffi_stream(stream).await?;
        wait_for_release_count(array_releases.as_ref(), 1).await;
        assert_eq!(schema_releases.load(Ordering::SeqCst), 1);

        free_ffi_handles(sdk, stream).await;
        assert_eq!(array_releases.load(Ordering::SeqCst), 1);
        assert_eq!(schema_releases.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn ffi_c_data_failed_owner_releases_once_when_stream_is_freed(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (mock_server, server_url) = start_mock_flight_server().await?;
        let schema = create_test_arrow_schema();
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockFlightResponse::Error {
                    status: Status::invalid_argument("terminal C Data test failure"),
                    delay_ms: 50,
                }],
            )
            .await;
        let (sdk, stream) = create_ffi_stream(server_url, Arc::clone(&schema)).await?;
        let array_releases = Arc::new(AtomicUsize::new(0));
        let schema_releases = Arc::new(AtomicUsize::new(0));
        let batch = create_test_record_batch(schema, vec![1], vec![Some("retained until free")]);
        let (array, schema) = exported_counting_batch(
            batch,
            Arc::clone(&array_releases),
            Arc::clone(&schema_releases),
        );

        assert_eq!(ingest_ffi_batch(stream, array, schema).await?, 0);
        assert_eq!(schema_releases.load(Ordering::SeqCst), 1);
        let error = flush_ffi_stream(stream)
            .await
            .expect_err("terminal server error must fail flush");
        assert!(error.contains("terminal C Data test failure"));
        assert_eq!(
            array_releases.load(Ordering::SeqCst),
            0,
            "failed unacknowledged owner must remain available for recovery"
        );

        free_ffi_handles(sdk, stream).await;
        wait_for_release_count(array_releases.as_ref(), 1).await;
        assert_eq!(schema_releases.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
