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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    use databricks_zerobus_ingest_sdk::internal::arrow_c_data::import_c_data_record_batch;
    use databricks_zerobus_ingest_sdk::internal::arrow_stream_has_ingested_c_data;
    use databricks_zerobus_ingest_sdk::ZerobusArrowStream;
    use tonic::Status;
    use zerobus_ffi::{
        zerobus_arrow_get_default_config, zerobus_arrow_stream_close, zerobus_arrow_stream_flush,
        zerobus_arrow_stream_free, zerobus_arrow_stream_ingest_batch,
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

    struct TimingArrayRelease {
        inner_release: Option<unsafe extern "C" fn(*mut FFI_ArrowArray)>,
        inner_private_data: *mut c_void,
        release_count: Arc<AtomicUsize>,
    }

    unsafe extern "C" fn timing_array_release(array: *mut FFI_ArrowArray) {
        let array = unsafe { &mut *array };
        let state = unsafe { Box::from_raw(array.private_data.cast::<TimingArrayRelease>()) };
        array.release = state.inner_release;
        array.private_data = state.inner_private_data;
        state.release_count.fetch_add(1, Ordering::SeqCst);
        if let Some(release) = state.inner_release {
            unsafe { release(array) };
        }
    }

    struct LockCheckingArrayRelease {
        inner_release: Option<unsafe extern "C" fn(*mut FFI_ArrowArray)>,
        inner_private_data: *mut c_void,
        stream: usize,
        locks_available: Arc<AtomicBool>,
        release_count: Arc<AtomicUsize>,
    }

    unsafe extern "C" fn lock_checking_array_release(array: *mut FFI_ArrowArray) {
        let array = unsafe { &mut *array };
        let state = unsafe { Box::from_raw(array.private_data.cast::<LockCheckingArrayRelease>()) };
        // This intentionally mirrors the opaque-handle cast in arrow.rs. If that handle
        // gains an outer wrapper, this test hook and the production cast must change together.
        let stream = unsafe { &*(state.stream as *const ZerobusArrowStream) };
        state.locks_available.store(
            stream.retained_batch_locks_available_for_test(),
            Ordering::SeqCst,
        );
        state.release_count.fetch_add(1, Ordering::SeqCst);
        array.release = state.inner_release;
        array.private_data = state.inner_private_data;
        if let Some(release) = state.inner_release {
            unsafe { release(array) };
        }
    }

    fn exported_lock_checking_batch(
        batch: RecordBatch,
        stream: usize,
        locks_available: Arc<AtomicBool>,
        release_count: Arc<AtomicUsize>,
    ) -> (FFI_ArrowArray, FFI_ArrowSchema) {
        let schema = batch.schema();
        let struct_array = StructArray::from(batch);
        let mut array = FFI_ArrowArray::new(&struct_array.to_data());
        let array_state = Box::new(LockCheckingArrayRelease {
            inner_release: array.release,
            inner_private_data: array.private_data,
            stream,
            locks_available,
            release_count,
        });
        array.release = Some(lock_checking_array_release);
        array.private_data = Box::into_raw(array_state).cast();
        let schema = FFI_ArrowSchema::try_from(schema.as_ref()).unwrap();
        (array, schema)
    }

    fn exported_timing_batch(
        batch: RecordBatch,
        release_count: Arc<AtomicUsize>,
        schema_releases: Arc<AtomicUsize>,
    ) -> (FFI_ArrowArray, FFI_ArrowSchema) {
        let schema = batch.schema();
        let struct_array = StructArray::from(batch);
        let mut array = FFI_ArrowArray::new(&struct_array.to_data());
        let array_state = Box::new(TimingArrayRelease {
            inner_release: array.release,
            inner_private_data: array.private_data,
            release_count,
        });
        array.release = Some(timing_array_release);
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

    fn batch_ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(&mut bytes, batch.schema().as_ref()).unwrap();
        writer.write(batch).unwrap();
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

    async fn ingest_ffi_ipc_batch(stream: usize, ipc: Vec<u8>) -> Result<i64, String> {
        tokio::task::spawn_blocking(move || {
            let mut result = CResult {
                success: true,
                error_message: ptr::null_mut(),
                is_retryable: false,
            };
            let offset = zerobus_arrow_stream_ingest_batch(
                stream as *mut _,
                ipc.as_ptr(),
                ipc.len(),
                &mut result,
            );
            if result.success {
                Ok(offset)
            } else {
                Err(take_result_error(&mut result)
                    .unwrap_or_else(|| "IPC ingest failed without a message".to_string()))
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

    async fn close_ffi_stream(stream: usize) -> Result<(), String> {
        tokio::task::spawn_blocking(move || {
            let mut result = CResult {
                success: true,
                error_message: ptr::null_mut(),
                is_retryable: false,
            };
            if zerobus_arrow_stream_close(stream as *mut _, &mut result) {
                Ok(())
            } else {
                Err(take_result_error(&mut result)
                    .unwrap_or_else(|| "Arrow stream close failed without a message".to_string()))
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

    struct TestBarrierGuard(Option<Arc<tokio::sync::Notify>>);

    impl TestBarrierGuard {
        fn release(&mut self) {
            if let Some(proceed) = self.0.take() {
                proceed.notify_one();
            }
        }
    }

    impl Drop for TestBarrierGuard {
        fn drop(&mut self) {
            self.release();
        }
    }

    async fn wait_for_free_completion(
        completion: &mut mpsc::Receiver<()>,
    ) -> Result<(), &'static str> {
        loop {
            match completion.try_recv() {
                Ok(()) => return Ok(()),
                Err(mpsc::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(1)).await
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err("stream free thread exited without reporting completion");
                }
            }
        }
    }

    async fn assert_mixed_mode_free_waits(
        c_data_first: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_mock_server, server_url) = start_mock_flight_server().await?;
        let schema = create_test_arrow_schema();
        let ipc_batch = create_test_record_batch(Arc::clone(&schema), vec![1], vec![Some("IPC")]);
        let c_data_batch =
            create_test_record_batch(Arc::clone(&schema), vec![2], vec![Some("C Data")]);
        let ipc = batch_ipc_bytes(&ipc_batch);
        let array_releases = Arc::new(AtomicUsize::new(0));
        let schema_releases = Arc::new(AtomicUsize::new(0));
        let (array, schema_ffi) = exported_counting_batch(
            c_data_batch,
            Arc::clone(&array_releases),
            Arc::clone(&schema_releases),
        );
        let (sdk, stream) = create_ffi_stream(server_url, schema).await?;

        if c_data_first {
            assert_eq!(ingest_ffi_batch(stream, array, schema_ffi).await?, 0);
            assert_eq!(ingest_ffi_ipc_batch(stream, ipc).await?, 1);
        } else {
            assert_eq!(ingest_ffi_ipc_batch(stream, ipc).await?, 0);
            assert_eq!(ingest_ffi_batch(stream, array, schema_ffi).await?, 1);
        }

        // This mirrors the opaque-handle cast in arrow.rs and must change with it.
        let stream_ref = unsafe { &*(stream as *const ZerobusArrowStream) };
        assert!(arrow_stream_has_ingested_c_data(stream_ref));
        let (shutdown_reached, shutdown_proceed) =
            stream_ref.arm_free_shutdown_complete_barrier().await;
        let mut shutdown_guard = TestBarrierGuard(Some(shutdown_proceed));

        let (free_completed_tx, mut free_completed_rx) = mpsc::channel();
        drop(std::thread::spawn(move || {
            zerobus_arrow_stream_free(stream as *mut _);
            let _ = free_completed_tx.send(());
        }));

        tokio::time::timeout(Duration::from_secs(5), shutdown_reached.notified())
            .await
            .expect("mixed-mode free must use complete C Data shutdown");
        assert!(matches!(
            free_completed_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        shutdown_guard.release();
        tokio::time::timeout(
            Duration::from_secs(5),
            wait_for_free_completion(&mut free_completed_rx),
        )
        .await
        .expect("mixed-mode free did not complete after shutdown proceeded")?;

        assert_eq!(array_releases.load(Ordering::SeqCst), 1);
        assert_eq!(schema_releases.load(Ordering::SeqCst), 1);
        tokio::task::spawn_blocking(move || zerobus_sdk_free(sdk as *mut _))
            .await
            .expect("sdk free task panicked");
        Ok(())
    }

    #[tokio::test]
    async fn ffi_ipc_only_free_does_not_wait_for_complete_shutdown(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_mock_server, server_url) = start_mock_flight_server().await?;
        let schema = create_test_arrow_schema();
        let batch = create_test_record_batch(Arc::clone(&schema), vec![1], vec![Some("IPC")]);
        let (sdk, stream) = create_ffi_stream(server_url, schema).await?;
        assert_eq!(
            ingest_ffi_ipc_batch(stream, batch_ipc_bytes(&batch)).await?,
            0
        );
        // This mirrors the opaque-handle cast in arrow.rs and must change with it.
        let stream_ref = unsafe { &*(stream as *const ZerobusArrowStream) };
        assert!(!arrow_stream_has_ingested_c_data(stream_ref));
        let (_shutdown_reached, shutdown_proceed) =
            stream_ref.arm_free_shutdown_complete_barrier().await;
        let mut shutdown_guard = TestBarrierGuard(Some(shutdown_proceed));

        let (free_completed_tx, mut free_completed_rx) = mpsc::channel();
        drop(std::thread::spawn(move || {
            zerobus_arrow_stream_free(stream as *mut _);
            let _ = free_completed_tx.send(());
        }));

        let completion = tokio::time::timeout(
            Duration::from_secs(1),
            wait_for_free_completion(&mut free_completed_rx),
        )
        .await;
        shutdown_guard.release();
        if completion.is_err() {
            tokio::time::timeout(
                Duration::from_secs(5),
                wait_for_free_completion(&mut free_completed_rx),
            )
            .await
            .expect("free did not complete after releasing the test barrier")?;
        }

        tokio::task::spawn_blocking(move || zerobus_sdk_free(sdk as *mut _))
            .await
            .expect("sdk free task panicked");
        completion.expect("IPC-only free must preserve best-effort nonblocking destruction")?;
        Ok(())
    }

    #[tokio::test]
    async fn ffi_ipc_then_c_data_free_waits_for_complete_shutdown(
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert_mixed_mode_free_waits(false).await
    }

    #[tokio::test]
    async fn ffi_c_data_then_ipc_free_waits_for_complete_shutdown(
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert_mixed_mode_free_waits(true).await
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
        assert_eq!(
            array_releases.load(Ordering::SeqCst),
            1,
            "failed unacknowledged owner must be released during stream free, not later"
        );
        assert_eq!(schema_releases.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn ffi_c_data_owner_remains_released_after_close_then_free(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (mock_server, server_url) = start_mock_flight_server().await?;
        let schema = create_test_arrow_schema();
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockFlightResponse::BatchAck {
                    ack_up_to_offset: 0,
                    delay_ms: 0,
                    ack_up_to_records: 1,
                }],
            )
            .await;
        let (sdk, stream) = create_ffi_stream(server_url, Arc::clone(&schema)).await?;
        let array_releases = Arc::new(AtomicUsize::new(0));
        let schema_releases = Arc::new(AtomicUsize::new(0));
        let batch = create_test_record_batch(schema, vec![1], vec![Some("closed")]);
        let (array, schema) = exported_counting_batch(
            batch,
            Arc::clone(&array_releases),
            Arc::clone(&schema_releases),
        );

        assert_eq!(ingest_ffi_batch(stream, array, schema).await?, 0);
        close_ffi_stream(stream).await?;
        assert_eq!(array_releases.load(Ordering::SeqCst), 1);
        assert_eq!(schema_releases.load(Ordering::SeqCst), 1);

        free_ffi_handles(sdk, stream).await;
        assert_eq!(array_releases.load(Ordering::SeqCst), 1);
        assert_eq!(schema_releases.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn ffi_c_data_release_callbacks_run_without_retained_batch_locks(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_mock_server, server_url) = start_mock_flight_server().await?;
        let schema = create_test_arrow_schema();
        let (sdk, stream) = create_ffi_stream(server_url, Arc::clone(&schema)).await?;
        let locks_available = Arc::new(AtomicBool::new(false));
        let release_count = Arc::new(AtomicUsize::new(0));
        let batch = create_test_record_batch(schema, vec![1], vec![Some("retained")]);
        let (array, schema_ffi) = exported_lock_checking_batch(
            batch,
            stream,
            Arc::clone(&locks_available),
            Arc::clone(&release_count),
        );
        let batch = unsafe { import_c_data_record_batch(array, schema_ffi) }?;

        // This intentionally mirrors the opaque-handle cast in arrow.rs. If that handle
        // gains an outer wrapper, this test hook and the production cast must change together.
        let stream_ref = unsafe { &*(stream as *const ZerobusArrowStream) };
        stream_ref.retain_failed_batch_for_test(batch).await;
        assert_eq!(release_count.load(Ordering::SeqCst), 0);

        free_ffi_handles(sdk, stream).await;

        assert_eq!(release_count.load(Ordering::SeqCst), 1);
        assert!(
            locks_available.load(Ordering::SeqCst),
            "release callback must not run while retained-batch locks are held"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ffi_c_data_free_from_multithread_runtime_releases_before_return(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_mock_server, server_url) = start_mock_flight_server().await?;
        let schema = create_test_arrow_schema();
        let (sdk, stream) = create_ffi_stream(server_url, Arc::clone(&schema)).await?;
        let array_releases = Arc::new(AtomicUsize::new(0));
        let schema_releases = Arc::new(AtomicUsize::new(0));
        let batch = create_test_record_batch(schema, vec![1], vec![Some("multi-thread free")]);
        let (array, schema_ffi) = exported_counting_batch(
            batch,
            Arc::clone(&array_releases),
            Arc::clone(&schema_releases),
        );
        let batch = unsafe { import_c_data_record_batch(array, schema_ffi) }?;
        // This mirrors the opaque-handle cast in arrow.rs and must change with it.
        let stream_ref = unsafe { &*(stream as *const ZerobusArrowStream) };
        stream_ref.retain_failed_batch_for_test(batch).await;

        zerobus_arrow_stream_free(stream as *mut _);

        assert_eq!(array_releases.load(Ordering::SeqCst), 1);
        assert_eq!(schema_releases.load(Ordering::SeqCst), 1);
        tokio::task::spawn_blocking(move || zerobus_sdk_free(sdk as *mut _))
            .await
            .expect("sdk free task panicked");
        Ok(())
    }

    #[tokio::test]
    async fn ffi_c_data_request_body_owner_releases_before_free_returns(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (mock_server, server_url) = start_mock_flight_server().await?;
        let schema = create_test_arrow_schema();
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockFlightResponse::BatchAck {
                    ack_up_to_offset: 0,
                    delay_ms: 5_000,
                    ack_up_to_records: 3,
                }],
            )
            .await;
        let (sdk, stream) = create_ffi_stream(server_url, Arc::clone(&schema)).await?;
        let release_count = Arc::new(AtomicUsize::new(0));
        let schema_releases = Arc::new(AtomicUsize::new(0));
        let batch =
            create_test_record_batch(schema, vec![1, 2, 3], vec![Some("a"), Some("b"), Some("c")]);
        let (array, schema_ffi) = exported_timing_batch(
            batch,
            Arc::clone(&release_count),
            Arc::clone(&schema_releases),
        );

        let (before_batch_poll_reached, before_batch_poll_proceed) = {
            // This mirrors the opaque-handle cast in arrow.rs and must change with it.
            let stream_ref = unsafe { &*(stream as *const ZerobusArrowStream) };
            stream_ref
                .arm_request_body_before_batch_poll_barrier()
                .await
        };
        let mut before_batch_poll_guard = TestBarrierGuard(Some(before_batch_poll_proceed));
        assert_eq!(ingest_ffi_batch(stream, array, schema_ffi).await?, 0);
        // This mirrors the opaque-handle cast in arrow.rs and must change with it.
        let stream_ref = unsafe { &*(stream as *const ZerobusArrowStream) };
        assert!(arrow_stream_has_ingested_c_data(stream_ref));
        tokio::time::timeout(Duration::from_secs(5), before_batch_poll_reached.notified())
            .await
            .expect("request body must park before consuming the queued batch");
        assert_eq!(schema_releases.load(Ordering::SeqCst), 1);
        assert_eq!(
            release_count.load(Ordering::SeqCst),
            0,
            "array owner must remain alive while the batch is pending"
        );

        let (
            request_shutdown_reached,
            request_shutdown_proceed,
            retained_batches_cleared,
            free_shutdown_complete_reached,
            free_shutdown_complete_proceed,
        ) = {
            // This mirrors the opaque-handle cast in arrow.rs and must change with it.
            let stream_ref = unsafe { &*(stream as *const ZerobusArrowStream) };
            let (request_shutdown_reached, request_shutdown_proceed) =
                stream_ref.arm_request_body_shutdown_barrier().await;
            let retained_batches_cleared = stream_ref.arm_retained_batches_cleared_notify().await;
            let (free_shutdown_complete_reached, free_shutdown_complete_proceed) =
                stream_ref.arm_free_shutdown_complete_barrier().await;
            (
                request_shutdown_reached,
                request_shutdown_proceed,
                retained_batches_cleared,
                free_shutdown_complete_reached,
                free_shutdown_complete_proceed,
            )
        };
        let mut request_shutdown_guard = TestBarrierGuard(Some(request_shutdown_proceed));
        let mut free_shutdown_complete_guard =
            TestBarrierGuard(Some(free_shutdown_complete_proceed));

        let (free_completed_tx, mut free_completed_rx) = mpsc::channel();
        drop(std::thread::spawn(move || {
            zerobus_arrow_stream_free(stream as *mut _);
            let _ = free_completed_tx.send(());
        }));

        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = request_shutdown_reached.notified() => Ok(()),
                _ = free_shutdown_complete_reached.notified() => Err(
                    "free reached its pre-return boundary without shutting down the request body"
                ),
                result = wait_for_free_completion(&mut free_completed_rx) => {
                    result.and(Err(
                        "zerobus_arrow_stream_free returned before request-body shutdown"
                    ))
                },
            }
        })
        .await
        .expect("request body must observe forced shutdown")?;

        tokio::time::timeout(Duration::from_secs(5), retained_batches_cleared.notified())
            .await
            .expect("destructive free must clear retained batches before waiting for request EOF");
        assert_eq!(
            release_count.load(Ordering::SeqCst),
            0,
            "the blocked request body must retain the owner after SDK collections are cleared"
        );

        before_batch_poll_guard.release();
        request_shutdown_guard.release();
        tokio::time::timeout(
            Duration::from_secs(5),
            free_shutdown_complete_reached.notified(),
        )
        .await
        .expect("free must reach its completed pre-return boundary");

        assert_eq!(
            release_count.load(Ordering::SeqCst),
            1,
            "all request-body and SDK owners must release before the pre-return boundary"
        );
        assert!(matches!(
            free_completed_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        free_shutdown_complete_guard.release();
        tokio::time::timeout(
            Duration::from_secs(5),
            wait_for_free_completion(&mut free_completed_rx),
        )
        .await
        .expect("zerobus_arrow_stream_free did not complete after shutdown proceeded")?;

        tokio::task::spawn_blocking(move || {
            zerobus_sdk_free(sdk as *mut _);
        })
        .await
        .expect("sdk free task panicked");
        Ok(())
    }
}
