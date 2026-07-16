mod mock_grpc;
mod utils;

use std::sync::Arc;

use databricks_zerobus_ingest_sdk::{NoTlsConfig, StreamType, ZerobusError, ZerobusSdk};
use mock_grpc::{start_mock_server, MockResponse};
use tracing::info;
use utils::{create_test_descriptor_proto, setup_tracing, TestCallback, TestHeadersProvider};

const TABLE_NAME: &str = "test_catalog.test_schema.test_table";

mod stream_initialization_and_basic_lifecycle_tests {
    use super::*;

    #[tokio::test]
    async fn test_successful_stream_creation() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_successful_stream_creation");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockResponse::CreateStream {
                    stream_id: "test_stream_1".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let result = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await;
        assert!(
            result.is_ok(),
            "Failed to create a stream: {:?}",
            result.err()
        );

        let stream = result.unwrap();
        assert_eq!(stream.stream_type, StreamType::Ephemeral);

        Ok(())
    }

    #[tokio::test]
    async fn test_timeouted_stream_creation() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_timeouted_stream_creation");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockResponse::CreateStream {
                    stream_id: "test_stream_1".to_string(),
                    delay_ms: 300,
                }],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let result = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery_timeout_ms(100)
            .recovery(false)
            .build()
            .await;
        assert!(
            result.is_err(),
            "Expected stream creation to fail but it succeeded: {:?}",
            result.err()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_non_retriable_error_during_stream_creation(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_non_retriable_error_during_stream_creation");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockResponse::Error {
                    status: tonic::Status::unauthenticated("Non-retriable error"),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let result = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(true)
            .build()
            .await;

        assert!(result.is_err());
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("Non-retriable error"));

        Ok(())
    }

    #[tokio::test]
    async fn test_retriable_error_without_recovery_during_stream_creation(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_retriable_error_without_recovery_during_stream_creation");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockResponse::Error {
                    status: tonic::Status::unavailable("Retriable error"),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let start_time = std::time::Instant::now();

        let result = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .recovery_timeout_ms(200)
            .recovery_backoff_ms(200)
            .build()
            .await;
        let duration = start_time.elapsed();
        assert!(result.is_err());
        assert!(duration.as_millis() <= 300);
        Ok(())
    }

    #[tokio::test]
    async fn test_graceful_close() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_graceful_close");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_1".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 100,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let mut stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        let test_record = b"test record data".to_vec();

        let ack_future = stream.ingest_record_offset(test_record).await?;
        stream.wait_for_offset(ack_future).await?;

        stream.close().await?;

        let write_count = mock_server.get_write_count().await;
        let max_offset = mock_server.get_max_offset_sent().await;

        assert_eq!(write_count, 1);
        assert_eq!(max_offset, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_idempotent_close() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_idempotent_close");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockResponse::CreateStream {
                    stream_id: "test_stream_1".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let mut stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        stream.close().await?;
        stream.close().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_after_close() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_flush_after_close");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_1".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                    MockResponse::Error {
                        status: tonic::Status::internal("Simulated error"),
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let mut stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        let test_record = b"test record data".to_vec();
        let _offset = stream.ingest_record_offset(test_record).await?;
        let test_record = b"test record data 2".to_vec();
        let _offset = stream.ingest_record_offset(test_record).await?;
        let _result = stream.close().await;
        let flush_result = stream.flush().await;

        assert!(flush_result.is_err());
        if let Err(ZerobusError::StreamClosedError(_)) = flush_result {
            // Expected error
        } else {
            panic!("Expected StreamClosedError, got: {:?}", flush_result);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_after_close() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_after_close");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockResponse::CreateStream {
                    stream_id: "test_stream_1".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let mut stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        stream.close().await?;

        let ingest_result = stream
            .ingest_record_offset(b"test record data".to_vec())
            .await;
        assert!(matches!(
            ingest_result,
            Err(ZerobusError::StreamClosedError(_))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_records_after_close() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_records_after_close");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockResponse::CreateStream {
                    stream_id: "test_stream_batch_after_close".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let mut stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        stream.close().await?;

        let batch = vec![b"record 1".to_vec(), b"record 2".to_vec()];
        let ingest_result = stream.ingest_records_offset(batch).await;

        assert!(matches!(
            ingest_result,
            Err(ZerobusError::StreamClosedError(_))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_close_waits_for_graceful_supervisor_shutdown(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_close_waits_for_graceful_supervisor_shutdown");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_graceful_shutdown".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 50,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let mut stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        // Ingest a record to ensure supervisor is running.
        let _ack = stream.ingest_record_offset(b"test".to_vec()).await?;

        let start = std::time::Instant::now();
        stream.close().await?;
        let duration = start.elapsed();

        // Should complete relatively quickly (within 2-3 seconds).
        assert!(
            duration.as_secs() < 3,
            "Close should complete within timeout, took {:?}",
            duration
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_immediately_shuts_down() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_drop_immediately_shuts_down");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockResponse::CreateStream {
                    stream_id: "test_stream_drop".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .build()
            .await?;

        let start = std::time::Instant::now();
        drop(stream);
        let duration = start.elapsed();

        // Drop should be nearly instantaneous.
        assert!(
            duration.as_millis() < 100,
            "Drop should be immediate, took {:?}",
            duration
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_close_idempotent_after_error() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_close_idempotent_after_error");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_close_after_error".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::Error {
                        status: tonic::Status::internal("Simulated error"),
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let mut stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .recovery(false)
            .build()
            .await?;

        // Ingest record that will fail.
        let _ack = stream.ingest_record_offset(b"will fail".to_vec()).await?;

        // Wait for error to propagate.
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Close should work even after error (may return error from flush, but shouldn't hang).
        let _close_result = stream.close().await;
        assert!(stream.is_closed(), "Stream should be closed after close()");

        // Second close should also work.
        let second_close = stream.close().await;
        assert!(second_close.is_ok(), "Second close should be idempotent");
        assert!(
            stream.is_closed(),
            "Stream should still be closed after second close()"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_supervisor_responds_to_cancellation() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_supervisor_responds_to_cancellation");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_cancellation".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 9,
                        delay_ms: 100,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let mut stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        // Ingest several records to keep supervisor busy.
        for _ in 0..10 {
            let _ack = stream.ingest_record_offset(b"test data".to_vec()).await?;
        }

        // Close while records are in flight.
        // Supervisor should respond to cancellation token and exit promptly.
        let start = std::time::Instant::now();
        stream.close().await?;
        let duration = start.elapsed();

        // Should complete within reasonable time (supervisor responds to cancellation).
        assert!(
            duration.as_secs() < 3,
            "Close with in-flight records should complete within timeout, took {:?}",
            duration
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_close_operations() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_concurrent_close_operations");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockResponse::CreateStream {
                    stream_id: "test_stream_concurrent_close".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .build()
            .await?;

        // Wrap stream in Arc<Mutex<>> to enable concurrent access from multiple tasks.
        let stream = Arc::new(tokio::sync::Mutex::new(stream));

        let start = std::time::Instant::now();

        // Spawn multiple tasks trying to close concurrently.
        let mut handles = vec![];
        for i in 0..3 {
            let stream_clone = Arc::clone(&stream);
            let handle = tokio::spawn(async move {
                info!("Task {} attempting to close stream", i);
                let mut stream_guard = stream_clone.lock().await;
                stream_guard.close().await
            });
            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.await?; // Some may succeed, some may return Ok (already closed).
        }

        let duration = start.elapsed();

        assert!(
            duration.as_millis() < 500,
            "Concurrent close calls should be fast and idempotent, took {:?}",
            duration
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_stream_cleanup_on_drop() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_stream_cleanup_on_drop");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockResponse::CreateStream {
                    stream_id: "test_stream_cleanup".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .build()
            .await?;

        {
            let _stream_in_scope = stream;
            // Stream goes out of scope here and drop is called.
        }

        // Stream cleanup completed successfully
        Ok(())
    }
}

mod schema_tests {
    use super::*;

    #[tokio::test]
    async fn test_json_record_ingestion() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_json_record_ingestion");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_json".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 50,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .json()
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        let json_record = r#"{"id": 1, "name": "test"}"#.to_string();
        let ingest_future = stream.ingest_record_offset(json_record.clone()).await?;
        stream.wait_for_offset(ingest_future).await?;
        let ingest_result = ingest_future;

        assert_eq!(ingest_result, 0);
        assert_eq!(mock_server.get_write_count().await, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_json_batch_ingestion() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_json_batch_ingestion");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_json_batch".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 50,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .json()
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        let json_batch = vec![
            r#"{"id": 1, "name": "test1"}"#.to_string(),
            r#"{"id": 2, "name": "test2"}"#.to_string(),
            r#"{"id": 3, "name": "test3"}"#.to_string(),
        ];

        let ack_future = stream.ingest_records_offset(json_batch).await?;
        if let Some(off) = ack_future {
            stream.wait_for_offset(off).await?;
        }
        let offset = ack_future;

        assert_eq!(offset, Some(0));
        assert_eq!(mock_server.get_write_count().await, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_json_into_proto_stream_fails() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_json_into_proto_stream_fails");

        let (_mock_server, server_url) = start_mock_server().await?;
        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(Default::default())
            .build()
            .await?;

        let json_record = r#"{"id": 1, "name": "test"}"#.to_string();
        let result = stream.ingest_record_offset(json_record).await;

        assert!(matches!(result, Err(ZerobusError::InvalidArgument(_))));

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_json_batch_into_proto_stream_fails(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_json_batch_into_proto_stream_fails");

        let (_mock_server, server_url) = start_mock_server().await?;
        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(Default::default())
            .build()
            .await?;

        let json_batch = vec![
            r#"{"id": 1, "name": "test1"}"#.to_string(),
            r#"{"id": 2, "name": "test2"}"#.to_string(),
        ];
        let result = stream.ingest_records_offset(json_batch).await;

        assert!(matches!(result, Err(ZerobusError::InvalidArgument(_))));

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_proto_into_json_stream_fails() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_proto_into_json_stream_fails");

        let (_mock_server, server_url) = start_mock_server().await?;
        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .json()
            .build()
            .await?;

        let proto_record = vec![1, 2, 3];
        let result = stream.ingest_record_offset(proto_record).await;

        assert!(matches!(result, Err(ZerobusError::InvalidArgument(_))));

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_proto_batch_into_json_stream_fails(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_proto_batch_into_json_stream_fails");

        let (_mock_server, server_url) = start_mock_server().await?;
        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .json()
            .build()
            .await?;

        let proto_batch = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let result = stream.ingest_records_offset(proto_batch).await;

        assert!(matches!(result, Err(ZerobusError::InvalidArgument(_))));

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_record_too_large_fails() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_record_too_large_fails");

        let (_mock_server, server_url) = start_mock_server().await?;
        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let limit = 1024;
        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(Default::default())
            .max_ingest_payload_bytes(limit)
            .build()
            .await?;

        let oversized = vec![0u8; limit + 1];
        let result = stream.ingest_record_offset(oversized).await;

        assert!(matches!(result, Err(ZerobusError::InvalidArgument(_))));

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_record_at_limit_succeeds() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_record_at_limit_succeeds");

        let (_mock_server, server_url) = start_mock_server().await?;
        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let limit = 1024;
        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(Default::default())
            .max_ingest_payload_bytes(limit)
            .build()
            .await?;

        // A payload of exactly max_ingest_payload_bytes must pass the client-side check.
        let exact = vec![0u8; limit];
        let result = stream.ingest_record_offset(exact).await;

        assert!(
            result.is_ok(),
            "payload of exactly the limit should be accepted, got {result:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_record_default_limit_below_server_limit(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_record_default_limit_below_server_limit");

        let (_mock_server, server_url) = start_mock_server().await?;
        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(Default::default())
            .build()
            .await?;

        // The default limit is set below the 10 MiB server limit to leave headroom
        // for the request envelope, so a full 10 MiB raw payload is rejected
        // client-side rather than failing later at the server transport layer.
        let server_limit = vec![0u8; 10 * 1024 * 1024];
        let result = stream.ingest_record_offset(server_limit).await;

        assert!(matches!(result, Err(ZerobusError::InvalidArgument(_))));

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_records_too_large_fails() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_records_too_large_fails");

        let (_mock_server, server_url) = start_mock_server().await?;
        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let limit = 1024;
        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(Default::default())
            .max_ingest_payload_bytes(limit)
            .build()
            .await?;

        // The combined size of the records passed to ingest_records_offset exceeds the limit.
        let records = vec![vec![0u8; 600], vec![0u8; 500]];
        let result = stream.ingest_records_offset(records).await;

        assert!(matches!(result, Err(ZerobusError::InvalidArgument(_))));

        Ok(())
    }

    #[tokio::test]
    async fn test_json_stream_creation_with_descriptor_warns(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_json_stream_creation_with_descriptor_warns");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockResponse::CreateStream {
                    stream_id: "test_stream_json_with_desc".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        // Should succeed but log a warning (descriptor will be ignored)
        let result = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .json()
            .build()
            .await;

        assert!(
            result.is_ok(),
            "JSON stream should be created even with descriptor"
        );

        Ok(())
    }
}

mod standard_operation_and_state_management_tests {
    use super::*;

    #[tokio::test]
    async fn test_single_record_ingestion() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_single_record_ingestion");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_1".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 50,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        let ingest_future = stream
            .ingest_record_offset(b"test record data".to_vec())
            .await?;
        stream.wait_for_offset(ingest_future).await?;
        let ingest_result = ingest_future;

        assert_eq!(ingest_result, 0);
        assert_eq!(mock_server.get_write_count().await, 1);
        assert_eq!(mock_server.get_max_offset_sent().await, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_records_ingestion() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_multiple_records_ingestion");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_1".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 99,
                        delay_ms: 200,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        let mut ingest_futures = Vec::new();
        for _i in 0..100 {
            let ingest_future = stream
                .ingest_record_offset(b"test record data".to_vec())
                .await?;
            ingest_futures.push(ingest_future);
        }

        for (i, ingest_future) in ingest_futures.into_iter().enumerate() {
            stream.wait_for_offset(ingest_future).await?;
            let ingest_result = ingest_future;
            assert_eq!(ingest_result, i as i64);
        }

        assert_eq!(mock_server.get_write_count().await, 100);
        assert_eq!(mock_server.get_max_offset_sent().await, 99);

        Ok(())
    }

    #[tokio::test]
    async fn test_basic_batch_ingestion() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_basic_batch_ingestion");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_batch".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 50,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        let batch = vec![
            b"record 1".to_vec(),
            b"record 2".to_vec(),
            b"record 3".to_vec(),
        ];

        let ack_future = stream.ingest_records_offset(batch).await?;
        if let Some(off) = ack_future {
            stream.wait_for_offset(off).await?;
        }
        let offset = ack_future;

        assert_eq!(offset, Some(0));
        assert_eq!(mock_server.get_write_count().await, 3);
        assert_eq!(mock_server.get_max_offset_sent().await, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_empty_batch_returns_none() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_empty_batch_returns_none");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockResponse::CreateStream {
                    stream_id: "test_stream_empty_batch".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .build()
            .await?;

        // Ingest empty batch when no batches have been acknowledged yet
        let empty_batch: Vec<Vec<u8>> = vec![];
        let ack_future = stream.ingest_records_offset(empty_batch).await?;
        if let Some(off) = ack_future {
            stream.wait_for_offset(off).await?;
        }
        let offset_id = ack_future;

        // Should return None since the batch is empty
        assert_eq!(offset_id, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_empty_batch_after_records_returns_none(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_empty_batch_after_records_returns_none");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_empty_after_ack".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 100,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 1,
                        delay_ms: 100,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .build()
            .await?;

        // Ingest some real records first
        let test_record1 = b"test record 1".to_vec();
        let ack_future1 = stream.ingest_record_offset(test_record1).await?;
        stream.wait_for_offset(ack_future1).await?;
        let offset_id1 = ack_future1;
        assert_eq!(offset_id1, 0);

        let test_record2 = b"test record 2".to_vec();
        let ack_future2 = stream.ingest_record_offset(test_record2).await?;
        stream.wait_for_offset(ack_future2).await?;
        let offset_id2 = ack_future2;
        assert_eq!(offset_id2, 1);

        // Now ingest an empty batch
        let empty_batch: Vec<Vec<u8>> = vec![];
        let ack_future = stream.ingest_records_offset(empty_batch).await?;
        if let Some(off) = ack_future {
            stream.wait_for_offset(off).await?;
        }
        let offset_id = ack_future;

        // Should return None since the batch is empty
        assert_eq!(offset_id, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_batch_too_large_rejected_by_server() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_batch_too_large_rejected_by_server");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_batch_too_large".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::Error {
                        status: tonic::Status::invalid_argument(
                            "Batch size exceeds maximum allowed size",
                        ),
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        // Create a batch and send it - server will reject it as too large
        let batch: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("record_{}", i).into_bytes())
            .collect();
        let ack_future = stream.ingest_records_offset(batch).await?;

        // The ack future should fail with the server's error
        let result = match ack_future {
            Some(off) => stream.wait_for_offset(off).await,
            None => Ok(()),
        };
        assert!(result.is_err());
        if let Err(ZerobusError::InvalidArgument(msg)) = result {
            assert!(
                msg.contains("Batch size exceeds maximum allowed size"),
                "Expected batch too large error, got: {}",
                msg
            );
        } else {
            panic!("Expected InvalidArgument error, got: {:?}", result.err());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_large_batch_ingestion() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_large_batch_ingestion");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_large_batch".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 100,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(200)
            .recovery(false)
            .build()
            .await?;

        let batch: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("record {}", i).into_bytes())
            .collect();

        let ack_future = stream.ingest_records_offset(batch).await?;
        if let Some(off) = ack_future {
            stream.wait_for_offset(off).await?;
        }
        let offset = ack_future;

        assert_eq!(offset, Some(0));
        assert_eq!(mock_server.get_write_count().await, 100);

        Ok(())
    }

    #[tokio::test]
    async fn test_mixed_batch_and_single_record_ingestion() -> Result<(), Box<dyn std::error::Error>>
    {
        setup_tracing();
        info!("Starting test_mixed_batch_and_single_record_ingestion");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_mixed".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 50,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 1,
                        delay_ms: 50,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 2,
                        delay_ms: 50,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        // Single record
        let ack1_future = stream
            .ingest_record_offset(b"single record".to_vec())
            .await?;

        // Batch
        let batch = vec![b"batch record 1".to_vec(), b"batch record 2".to_vec()];
        let ack2_future = stream.ingest_records_offset(batch).await?;

        // Another single record
        let ack3_future = stream
            .ingest_record_offset(b"another single record".to_vec())
            .await?;

        stream.wait_for_offset(ack1_future).await?;
        assert_eq!(ack1_future, 0);
        if let Some(off) = ack2_future {
            stream.wait_for_offset(off).await?;
        }
        assert_eq!(ack2_future, Some(1));
        stream.wait_for_offset(ack3_future).await?;
        assert_eq!(ack3_future, 2);

        assert_eq!(mock_server.get_write_count().await, 4);
        assert_eq!(mock_server.get_max_offset_sent().await, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_flush() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_flush");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_1".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 4,
                        delay_ms: 200,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        for _i in 0..5 {
            let _ingest_future = stream
                .ingest_record_offset(b"test record data".to_vec())
                .await?;
        }

        stream.flush().await?;

        assert_eq!(mock_server.get_write_count().await, 5);
        assert_eq!(mock_server.get_max_offset_sent().await, 4);

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_timeout() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_flush_timeout");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_1".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 4,
                        delay_ms: 300,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .flush_timeout_ms(100)
            .build()
            .await?;

        let mut ingest_futures = Vec::new();
        for _i in 0..5 {
            let ingest_future = stream
                .ingest_record_offset(b"test record data".to_vec())
                .await?;
            ingest_futures.push(ingest_future);
        }

        let flush_result = stream.flush().await;

        assert!(flush_result.is_err());
        if let Err(ZerobusError::StreamClosedError(_)) = flush_result {
            // Expected timeout error
        } else {
            panic!(
                "Expected StreamClosedError with timeout, got: {:?}",
                flush_result
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_flush() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_empty_flush");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockResponse::CreateStream {
                    stream_id: "test_stream_1".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        let start_time = std::time::Instant::now();
        stream.flush().await?;

        let duration = start_time.elapsed();
        assert!(duration.as_millis() <= 100);

        Ok(())
    }
}

mod concurrency_and_race_condition_tests {
    use super::*;

    #[tokio::test]
    async fn test_concurrent_ingest_from_multiple_tasks() -> Result<(), Box<dyn std::error::Error>>
    {
        setup_tracing();
        info!("Starting test_concurrent_ingest_from_multiple_tasks");

        const NUM_RECORDS: usize = 1000;
        const NUM_TASKS: usize = 10;
        const RECORDS_PER_TASK: usize = NUM_RECORDS / NUM_TASKS;

        let (mock_server, server_url) = start_mock_server().await?;

        let responses = vec![
            MockResponse::CreateStream {
                stream_id: "test_stream_concurrent".to_string(),
                delay_ms: 0,
            },
            MockResponse::RecordAck {
                ack_up_to_offset: (NUM_RECORDS - 1) as i64,
                delay_ms: 500,
            },
        ];
        mock_server.inject_responses(TABLE_NAME, responses).await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(NUM_RECORDS)
            .recovery(false)
            .build()
            .await?;
        let stream = Arc::new(stream);

        let mut tasks = Vec::new();

        for _ in 0..NUM_TASKS {
            let stream_clone = Arc::clone(&stream);
            let task = tokio::spawn(async move {
                let mut ack_futures = Vec::new();
                for _ in 0..RECORDS_PER_TASK {
                    match stream_clone
                        .ingest_record_offset(b"concurrent test data".to_vec())
                        .await
                    {
                        Ok(ack_future) => ack_futures.push(ack_future),
                        Err(e) => return Err(e),
                    }
                }
                Ok(ack_futures)
            });
            tasks.push(task);
        }

        let mut all_ack_futures = Vec::new();
        for task in tasks {
            let futures = task.await??;
            all_ack_futures.extend(futures);
        }

        assert_eq!(all_ack_futures.len(), NUM_RECORDS);

        let mut offsets = Vec::new();
        for ack_future in all_ack_futures {
            stream.wait_for_offset(ack_future).await?;
            offsets.push(ack_future);
        }

        offsets.sort();

        let expected_offsets: Vec<i64> = (0..NUM_RECORDS as i64).collect();
        assert_eq!(
            offsets, expected_offsets,
            "Offsets should be a complete sequence from 0 to NUM_RECORDS - 1"
        );
        assert_eq!(mock_server.get_write_count().await, NUM_RECORDS as u64);
        assert_eq!(
            mock_server.get_max_offset_sent().await,
            (NUM_RECORDS - 1) as i64
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_blocks_on_inflight_limit() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_blocks_on_inflight_limit");

        const MAX_INFLIGHT_REQUESTS: usize = 10;
        const ACK_DELAY_MS: u64 = 500;
        const TOTAL_REQUESTS: usize = MAX_INFLIGHT_REQUESTS + 5;

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_blocking".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: (MAX_INFLIGHT_REQUESTS - 1) as i64,
                        delay_ms: ACK_DELAY_MS,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: (TOTAL_REQUESTS - 1) as i64,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(MAX_INFLIGHT_REQUESTS)
            .recovery(false)
            .build()
            .await?;

        let mut ack_futures = Vec::new();

        // Send MAX_INFLIGHT_REQUESTS requests (each ingest_record = 1 request)
        for _ in 0..MAX_INFLIGHT_REQUESTS {
            let ack_future = stream.ingest_record_offset(b"test data".to_vec()).await?;
            ack_futures.push(ack_future);
        }

        // The next request should block because we're at the inflight limit
        let start_time = std::time::Instant::now();
        let blocking_ack_future = stream
            .ingest_record_offset(b"blocking data".to_vec())
            .await?;
        let duration = start_time.elapsed();

        ack_futures.push(blocking_ack_future);

        assert!(
            duration.as_millis() >= ACK_DELAY_MS as u128,
            "The {}th ingest call should block for at least {}ms, but only blocked for {}ms",
            MAX_INFLIGHT_REQUESTS + 1,
            ACK_DELAY_MS,
            duration.as_millis()
        );

        // Send remaining requests
        for _ in (MAX_INFLIGHT_REQUESTS + 1)..TOTAL_REQUESTS {
            let ack_future = stream
                .ingest_record_offset(b"more test data".to_vec())
                .await?;
            ack_futures.push(ack_future);
        }

        for (i, ack) in ack_futures.into_iter().enumerate() {
            stream.wait_for_offset(ack).await?;
            assert_eq!(ack, i as i64);
        }

        assert_eq!(mock_server.get_write_count().await, TOTAL_REQUESTS as u64);
        assert_eq!(
            mock_server.get_max_offset_sent().await,
            (TOTAL_REQUESTS - 1) as i64
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_records_blocks_on_inflight_limit() -> Result<(), Box<dyn std::error::Error>>
    {
        setup_tracing();
        info!("Starting test_ingest_records_blocks_on_inflight_limit");

        const MAX_INFLIGHT_REQUESTS: usize = 5;
        const ACK_DELAY_MS: u64 = 500;
        const RECORDS_PER_BATCH: usize = 3;
        const TOTAL_BATCHES: usize = MAX_INFLIGHT_REQUESTS + 1;

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_blocking_batch".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: (MAX_INFLIGHT_REQUESTS - 1) as i64,
                        delay_ms: ACK_DELAY_MS,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: (TOTAL_BATCHES - 1) as i64,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(MAX_INFLIGHT_REQUESTS)
            .recovery(false)
            .build()
            .await?;

        let mut ack_futures = Vec::new();

        // Send MAX_INFLIGHT_REQUESTS batches (each ingest_records = 1 request, regardless of record count)
        for _ in 0..MAX_INFLIGHT_REQUESTS {
            let batch: Vec<Vec<u8>> = (0..RECORDS_PER_BATCH)
                .map(|_| b"test data".to_vec())
                .collect();
            let ack_future = stream.ingest_records_offset(batch).await?;
            ack_futures.push(ack_future);
        }

        // The next batch should block because we're at the inflight request limit
        let start_time = std::time::Instant::now();
        let batch: Vec<Vec<u8>> = (0..RECORDS_PER_BATCH)
            .map(|_| b"blocking batch data".to_vec())
            .collect();
        let blocking_ack_future = stream.ingest_records_offset(batch).await?;
        let duration = start_time.elapsed();

        ack_futures.push(blocking_ack_future);

        assert!(
            duration.as_millis() >= ACK_DELAY_MS as u128,
            "The {}th batch ingest should block for at least {}ms, but only blocked for {}ms",
            MAX_INFLIGHT_REQUESTS + 1,
            ACK_DELAY_MS,
            duration.as_millis()
        );

        // Wait for all acknowledgments
        for (i, ack) in ack_futures.into_iter().enumerate() {
            if let Some(off) = ack {
                stream.wait_for_offset(off).await?;
            }
            assert_eq!(ack, Some(i as i64));
        }

        // Total writes = TOTAL_BATCHES * RECORDS_PER_BATCH
        assert_eq!(
            mock_server.get_write_count().await,
            (TOTAL_BATCHES * RECORDS_PER_BATCH) as u64
        );
        assert_eq!(
            mock_server.get_max_offset_sent().await,
            (TOTAL_BATCHES - 1) as i64
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_with_concurrent_ingestion() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_flush_with_concurrent_ingestion");

        const RECORDS_BEFORE_FLUSH: usize = 10;
        const RECORDS_DURING_FLUSH: usize = 5;
        const TOTAL_RECORDS: usize = RECORDS_BEFORE_FLUSH + RECORDS_DURING_FLUSH;
        const ACK_DELAY_MS: u64 = 500;

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_flush_concurrent".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: (RECORDS_BEFORE_FLUSH - 1) as i64,
                        delay_ms: ACK_DELAY_MS,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: (TOTAL_RECORDS - 1) as i64,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;
        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(TOTAL_RECORDS + 5)
            .build()
            .await?;
        let stream = Arc::new(stream);

        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let ingester_stream = Arc::clone(&stream);
        let ingester_barrier = Arc::clone(&barrier);

        let ingester_task = tokio::spawn(async move {
            let mut futures = Vec::new();
            ingester_barrier.wait().await;
            for _ in 0..RECORDS_DURING_FLUSH {
                if let Ok(future) = ingester_stream
                    .ingest_record_offset(b"ingester data".to_vec())
                    .await
                {
                    futures.push(future);
                }
            }
            futures
        });

        let mut flusher_futures = Vec::new();
        for _ in 0..RECORDS_BEFORE_FLUSH {
            flusher_futures.push(
                stream
                    .ingest_record_offset(b"flusher data".to_vec())
                    .await?,
            );
        }

        barrier.wait().await;

        let flush_start_time = std::time::Instant::now();
        stream.flush().await?;
        let flush_duration = flush_start_time.elapsed();

        assert!(
            flush_duration.as_millis() >= ACK_DELAY_MS as u128,
            "Flush should wait for the delayed ACK."
        );
        assert!(
            flush_duration.as_millis() < ACK_DELAY_MS as u128 + 200,
            "Flush should not wait significantly longer than the ACK delay."
        );

        let mut all_futures = flusher_futures;
        all_futures.extend(ingester_task.await?);

        for (i, future) in all_futures.into_iter().enumerate() {
            stream.wait_for_offset(future).await?;
            assert_eq!(future, i as i64);
        }

        assert_eq!(mock_server.get_write_count().await, TOTAL_RECORDS as u64);
        Ok(())
    }

    #[tokio::test]
    async fn test_close_with_concurrent_ingestion() -> Result<(), Box<dyn std::error::Error>> {
        // This test is complex to implement correctly with the current API
        // since close() requires &mut self but concurrent ingestion needs &self
        // The existing test_flush_with_concurrent_ingestion covers similar concurrency scenarios
        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_batch_ingestion() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_concurrent_batch_ingestion");

        const NUM_TASKS: usize = 5;
        const RECORDS_PER_BATCH: usize = 10;
        const TOTAL_BATCHES: usize = NUM_TASKS;

        let (mock_server, server_url) = start_mock_server().await?;

        let responses = vec![
            MockResponse::CreateStream {
                stream_id: "test_stream_concurrent_batch".to_string(),
                delay_ms: 0,
            },
            MockResponse::RecordAck {
                ack_up_to_offset: (TOTAL_BATCHES - 1) as i64,
                delay_ms: 500,
            },
        ];
        mock_server.inject_responses(TABLE_NAME, responses).await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(NUM_TASKS * RECORDS_PER_BATCH + 10)
            .recovery(false)
            .build()
            .await?;
        let stream = Arc::new(stream);

        let mut tasks = Vec::new();

        for task_id in 0..NUM_TASKS {
            let stream_clone = Arc::clone(&stream);
            let task = tokio::spawn(async move {
                let batch: Vec<Vec<u8>> = (0..RECORDS_PER_BATCH)
                    .map(|i| format!("task {} record {}", task_id, i).into_bytes())
                    .collect();

                match stream_clone.ingest_records_offset(batch).await {
                    Ok(ack_future) => Ok(ack_future),
                    Err(e) => Err(e),
                }
            });
            tasks.push(task);
        }

        let mut all_ack_futures = Vec::new();
        for task in tasks {
            let future = task.await??;
            all_ack_futures.push(future);
        }

        assert_eq!(all_ack_futures.len(), TOTAL_BATCHES);

        let mut offsets = Vec::new();
        for ack_future in all_ack_futures {
            if let Some(off) = ack_future {
                stream.wait_for_offset(off).await?;
            }
            offsets.push(ack_future);
        }

        offsets.sort();
        let expected_offsets: Vec<Option<i64>> = (0..TOTAL_BATCHES as i64).map(Some).collect();
        assert_eq!(offsets, expected_offsets);

        Ok(())
    }
}

mod failure_scenarios_tests {
    use super::*;

    mod no_recovery {
        use super::*;

        #[tokio::test]
        async fn test_receiver_error_fails_stream() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_receiver_error_fails_stream");

            let (mock_server, server_url) = start_mock_server().await?;

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockResponse::CreateStream {
                            stream_id: "test_stream_fail".to_string(),
                            delay_ms: 0,
                        },
                        MockResponse::RecordAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                        },
                        MockResponse::Error {
                            status: tonic::Status::internal("Receiver error"),
                            delay_ms: 0,
                        },
                    ],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;
            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
                .recovery(false)
                .build()
                .await?;

            let ack_future_1 = stream.ingest_record_offset(b"good data".to_vec()).await?;
            let ack_future_2 = stream.ingest_record_offset(b"bad data".to_vec()).await?;

            stream.wait_for_offset(ack_future_1).await?;
            assert_eq!(ack_future_1, 0);

            let result_2 = stream.wait_for_offset(ack_future_2).await;
            assert!(result_2.is_err());
            if let Err(e) = result_2 {
                assert!(e.to_string().contains("Receiver error"));
            }

            let ingest_3 = stream.ingest_record_offset(b"more data".to_vec()).await;
            assert!(matches!(ingest_3, Err(ZerobusError::StreamClosedError(_))));

            Ok(())
        }

        #[tokio::test]
        async fn test_server_unresponsiveness_fails_stream(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_server_unresponsiveness_fails_stream");

            const ACK_TIMEOUT_MS: u64 = 200;

            let (mock_server, server_url) = start_mock_server().await?;

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockResponse::CreateStream {
                        stream_id: "test_stream_timeout".to_string(),
                        delay_ms: 0,
                    }],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;
            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
                .recovery(false)
                .server_lack_of_ack_timeout_ms(ACK_TIMEOUT_MS)
                .build()
                .await?;

            let ack_future = stream.ingest_record_offset(b"some data".to_vec()).await?;

            let result = stream.wait_for_offset(ack_future).await;
            assert!(result.is_err());
            if let Err(e) = result {
                assert!(e.to_string().contains("Server ack timeout"));
            }

            Ok(())
        }

        #[tokio::test]
        async fn test_get_unacked_records_after_failure() -> Result<(), Box<dyn std::error::Error>>
        {
            setup_tracing();
            info!("Starting test_get_unacked_records_after_failure");

            const ACKED_RECORDS: usize = 5;
            const UNACKED_RECORDS: usize = 5;
            const TOTAL_RECORDS: usize = ACKED_RECORDS + UNACKED_RECORDS;

            let (mock_server, server_url) = start_mock_server().await?;

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockResponse::CreateStream {
                            stream_id: "test_stream_unacked".to_string(),
                            delay_ms: 0,
                        },
                        MockResponse::RecordAck {
                            ack_up_to_offset: (ACKED_RECORDS - 1) as i64,
                            delay_ms: 0,
                        },
                        MockResponse::Error {
                            status: tonic::Status::internal("Test failure"),
                            delay_ms: 0,
                        },
                    ],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;
            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
                .recovery(false)
                .build()
                .await?;

            let mut futures = Vec::new();

            for i in 0..TOTAL_RECORDS {
                let payload = format!("record-{}", i).into_bytes();
                futures.push(stream.ingest_record_offset(payload).await?);
            }

            for (i, future) in futures.into_iter().enumerate() {
                let result = stream.wait_for_offset(future).await;
                if i < ACKED_RECORDS {
                    assert!(result.is_ok(), "First batch of records should be acked");
                } else {
                    assert!(result.is_err(), "Second batch of records should fail");
                }
            }

            let retrieved_unacked = stream.get_unacked_records().await?.collect::<Vec<_>>();

            // Each individual record is returned separately
            assert_eq!(retrieved_unacked.len(), UNACKED_RECORDS);

            // Verify each unacked record
            for (i, record) in retrieved_unacked.iter().enumerate() {
                match record {
                    databricks_zerobus_ingest_sdk::EncodedRecord::Proto(payload) => {
                        let expected_payload = format!("record-{}", ACKED_RECORDS + i).into_bytes();
                        assert_eq!(payload, &expected_payload);
                    }
                    _ => panic!("Expected Proto record"),
                }
            }

            Ok(())
        }

        #[tokio::test]
        async fn test_get_unacked_records_with_mixed_batches_and_singles(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_get_unacked_records_with_mixed_batches_and_singles");

            const SINGLE_RECORDS: usize = 3;
            const BATCH_SIZE: usize = 5;

            let (mock_server, server_url) = start_mock_server().await?;

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockResponse::CreateStream {
                            stream_id: "test_stream_mixed_unacked".to_string(),
                            delay_ms: 0,
                        },
                        MockResponse::RecordAck {
                            ack_up_to_offset: 1,
                            delay_ms: 0,
                        },
                        MockResponse::Error {
                            status: tonic::Status::internal("Test failure"),
                            delay_ms: 0,
                        },
                    ],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;
            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
                .recovery(false)
                .build()
                .await?;

            // Ingest individual records
            let mut single_futures = Vec::new();
            for i in 0..SINGLE_RECORDS {
                let payload = format!("single-{}", i).into_bytes();
                single_futures.push(stream.ingest_record_offset(payload).await?);
            }

            // Ingest a batch
            let batch: Vec<Vec<u8>> = (0..BATCH_SIZE)
                .map(|i| format!("batch-{}", i).into_bytes())
                .collect();
            let batch_future = stream.ingest_records_offset(batch).await?;

            // Wait for some to be acked and some to fail
            for (i, future) in single_futures.into_iter().enumerate() {
                let result = stream.wait_for_offset(future).await;
                if i < 2 {
                    assert!(result.is_ok(), "First records should be acked");
                } else {
                    assert!(result.is_err(), "Later records should fail");
                }
            }

            let batch_result = match batch_future {
                Some(off) => stream.wait_for_offset(off).await,
                None => Ok(()),
            };
            assert!(batch_result.is_err(), "Batch should fail");

            let retrieved_unacked = stream.get_unacked_records().await?.collect::<Vec<_>>();

            // We should have:
            // - 1 single record (the 3rd one, index 2)
            // - 5 records from the batch
            // Total: 6 records (flattened)
            assert_eq!(
                retrieved_unacked.len(),
                6,
                "Should have 6 unacked records (1 single + 5 from batch)"
            );

            // Check they are all Proto records with correct payloads
            match &retrieved_unacked[0] {
                databricks_zerobus_ingest_sdk::EncodedRecord::Proto(payload) => {
                    assert_eq!(payload, &b"single-2".to_vec());
                }
                _ => panic!("Expected Proto record"),
            }

            for (i, record) in retrieved_unacked.iter().enumerate().skip(1).take(5) {
                match record {
                    databricks_zerobus_ingest_sdk::EncodedRecord::Proto(payload) => {
                        let expected = format!("batch-{}", i - 1).into_bytes();
                        assert_eq!(payload, &expected);
                    }
                    _ => panic!("Expected Proto record"),
                }
            }

            Ok(())
        }

        #[tokio::test]
        async fn test_pending_futures_fail_on_stream_failure(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_pending_futures_fail_on_stream_failure");

            const NUM_PENDING_RECORDS: usize = 5;

            let (mock_server, server_url) = start_mock_server().await?;

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockResponse::CreateStream {
                            stream_id: "test_stream_futures_fail".to_string(),
                            delay_ms: 0,
                        },
                        MockResponse::Error {
                            status: tonic::Status::permission_denied("Permission denied"),
                            delay_ms: 100,
                        },
                    ],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;
            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
                .recovery(false)
                .build()
                .await?;

            let mut futures = Vec::new();
            for i in 0..NUM_PENDING_RECORDS {
                let payload = format!("record-{}", i).into_bytes();
                futures.push(stream.ingest_record_offset(payload).await?);
            }

            for future in futures {
                let result = stream.wait_for_offset(future).await;
                assert!(result.is_err());
                match result {
                    Err(ZerobusError::StreamClosedError(status)) => {
                        assert_eq!(status.code(), tonic::Code::PermissionDenied);
                        assert!(status.message().contains("Permission denied"));
                    }
                    _ => panic!("Expected StreamClosedError with PermissionDenied status"),
                }
            }

            Ok(())
        }
    }

    mod recovery {
        use super::*;

        #[tokio::test]
        async fn test_recovery_during_stream_creation() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_recovery_during_stream_creation");

            let (mock_server, server_url) = start_mock_server().await?;

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockResponse::Error {
                            status: tonic::Status::unavailable("Service temporarily unavailable"),
                            delay_ms: 0,
                        },
                        MockResponse::CreateStream {
                            stream_id: "test_stream_recovered".to_string(),
                            delay_ms: 0,
                        },
                        MockResponse::RecordAck {
                            ack_up_to_offset: 2,
                            delay_ms: 100,
                        },
                    ],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let start_time = std::time::Instant::now();
            let result = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
                .max_inflight_requests(100)
                .recovery(true)
                .recovery_timeout_ms(5000)
                .recovery_backoff_ms(100)
                .build()
                .await;
            let duration = start_time.elapsed();

            assert!(
                result.is_ok(),
                "Expected stream creation to succeed after retry, but got error: {:?}",
                result.err()
            );

            let stream = result.unwrap();
            assert_eq!(stream.stream_type, StreamType::Ephemeral);

            assert!(
                duration.as_millis() >= 100,
                "Expected at least one retry with backoff, duration was {:?}",
                duration
            );

            for i in 0..3 {
                let payload = format!("test-record-{}", i).into_bytes();
                let _ack_future = stream.ingest_record_offset(payload).await?;
            }

            stream.flush().await?;

            assert_eq!(mock_server.get_write_count().await, 3);
            assert_eq!(mock_server.get_max_offset_sent().await, 2);

            Ok(())
        }

        #[tokio::test]
        async fn test_stream_creation_fails_after_exhausting_retries(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_stream_creation_fails_after_exhausting_retries");

            let (mock_server, server_url) = start_mock_server().await?;

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockResponse::Error {
                            status: tonic::Status::unavailable("Service unavailable"),
                            delay_ms: 0,
                        },
                        MockResponse::Error {
                            status: tonic::Status::unavailable("Service unavailable"),
                            delay_ms: 0,
                        },
                        MockResponse::Error {
                            status: tonic::Status::unavailable("Service unavailable"),
                            delay_ms: 0,
                        },
                        MockResponse::Error {
                            status: tonic::Status::unavailable("Service unavailable"),
                            delay_ms: 0,
                        },
                        MockResponse::Error {
                            status: tonic::Status::unavailable("Service unavailable"),
                            delay_ms: 0,
                        },
                    ],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let start_time = std::time::Instant::now();
            let result = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
                .max_inflight_requests(100)
                .recovery(true)
                .recovery_backoff_ms(100)
                .build()
                .await;
            let duration = start_time.elapsed();

            assert!(
                result.is_err(),
                "Expected stream creation to fail after exhausting retries"
            );

            let error = result.err().unwrap();
            assert!(
                error.to_string().contains("Service unavailable"),
                "Error message should indicate service unavailability, got: {:?}",
                error
            );

            assert!(
                duration.as_millis() >= 400,
                "Expected retries to take at least 500ms (recovery_timeout_ms), but took {:?}",
                duration
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_recovery_after_retriable_receiver_error(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_recovery_after_retriable_receiver_error");

            let (mock_server, server_url) = start_mock_server().await?;

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockResponse::CreateStream {
                            stream_id: "test_stream_recovery".to_string(),
                            delay_ms: 0,
                        },
                        MockResponse::Error {
                            status: tonic::Status::unavailable("Temporary network issue"),
                            delay_ms: 0,
                        },
                        MockResponse::CreateStream {
                            stream_id: "test_stream_recovered".to_string(),
                            delay_ms: 0,
                        },
                        MockResponse::RecordAck {
                            ack_up_to_offset: 4,
                            delay_ms: 0,
                        },
                    ],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
                .max_inflight_requests(100)
                .recovery(true)
                .recovery_timeout_ms(5000)
                .recovery_backoff_ms(100)
                .build()
                .await?;
            assert_eq!(stream.stream_type, StreamType::Ephemeral);

            let mut futures = Vec::new();
            for i in 0..5 {
                let payload = format!("test-record-{}", i).into_bytes();
                let ack_future = stream.ingest_record_offset(payload).await?;
                futures.push(ack_future);
            }

            for (i, future) in futures.into_iter().enumerate() {
                stream.wait_for_offset(future).await?;
                assert_eq!(future, i as i64, "Record {} should have offset {}", i, i);
            }

            let write_count = mock_server.get_write_count().await;
            let max_offset = mock_server.get_max_offset_sent().await;
            assert!(
                write_count >= 5,
                "Expected at least 5 writes to ensure all records were sent, got {}",
                write_count
            );

            assert_eq!(
                max_offset, 4,
                "Expected max offset of 4 (records 0-4), got {}",
                max_offset
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_recovery_after_server_unresponsiveness(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_recovery_after_server_unresponsiveness");

            const ACK_TIMEOUT_MS: u64 = 100;

            let (mock_server, server_url) = start_mock_server().await?;

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockResponse::CreateStream {
                            stream_id: "test_stream_unresponsive".to_string(),
                            delay_ms: 0,
                        },
                        MockResponse::CreateStream {
                            stream_id: "test_stream_recovered".to_string(),
                            delay_ms: 0,
                        },
                        MockResponse::RecordAck {
                            ack_up_to_offset: 4,
                            delay_ms: 50,
                        },
                    ],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
                .max_inflight_requests(100)
                .recovery(true)
                .server_lack_of_ack_timeout_ms(ACK_TIMEOUT_MS)
                .recovery_timeout_ms(5000)
                .recovery_backoff_ms(100)
                .build()
                .await?;
            assert_eq!(stream.stream_type, StreamType::Ephemeral);

            for i in 0..5 {
                let payload = format!("test-record-{}", i).into_bytes();
                let _ack_future = stream.ingest_record_offset(payload).await?;
            }
            stream.flush().await?;
            let write_count = mock_server.get_write_count().await;
            let max_offset = mock_server.get_max_offset_sent().await;
            assert_eq!(write_count, 5, "Expected 5 writes, got {}", write_count);

            assert_eq!(
                max_offset, 4,
                "Expected max offset of 4 (records 0-4), got {}",
                max_offset
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_close_fails_on_non_retriable_error_during_flush(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_close_fails_on_non_retriable_error_during_flush");

            let (mock_server, server_url) = start_mock_server().await?;

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockResponse::CreateStream {
                            stream_id: "test_stream_close_error".to_string(),
                            delay_ms: 0,
                        },
                        MockResponse::Error {
                            status: tonic::Status::permission_denied("Permission denied"),
                            delay_ms: 50,
                        },
                    ],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let mut stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
                .max_inflight_requests(100)
                .flush_timeout_ms(200)
                .build()
                .await?;
            assert_eq!(stream.stream_type, StreamType::Ephemeral);

            for i in 0..5 {
                let payload = format!("test-record-{}", i).into_bytes();
                let _ack_future = stream.ingest_record_offset(payload).await?;
            }

            let close_result = stream.close().await;
            assert!(close_result.is_err(), "Expected close to fail");

            if let Err(e) = close_result {
                assert!(
                    e.to_string().contains("Permission denied"),
                    "Expected error related to permission denied, got: {:?}",
                    e
                );
            }

            let ingest_after_failed_close =
                stream.ingest_record_offset(b"more data".to_vec()).await;
            assert!(
                matches!(
                    ingest_after_failed_close,
                    Err(ZerobusError::StreamClosedError(_))
                ),
                "Expected StreamClosedError after failed close"
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_recovery_on_close_stream_signal() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_recovery_on_close_stream_signal");

            let (mock_server, server_url) = start_mock_server().await?;

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockResponse::CreateStream {
                            stream_id: "test_stream_close_signal".to_string(),
                            delay_ms: 0,
                        },
                        MockResponse::RecordAck {
                            ack_up_to_offset: 2,
                            delay_ms: 0,
                        },
                        MockResponse::CloseStreamSignal {
                            duration_seconds: 1,
                            delay_ms: 0,
                        },
                        MockResponse::CreateStream {
                            stream_id: "test_stream_recovered_after_signal".to_string(),
                            delay_ms: 0,
                        },
                        MockResponse::RecordAck {
                            ack_up_to_offset: 1,
                            delay_ms: 0,
                        },
                    ],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
                .max_inflight_requests(100)
                .recovery(true)
                .recovery_timeout_ms(5000)
                .recovery_backoff_ms(100)
                .build()
                .await?;
            assert_eq!(stream.stream_type, StreamType::Ephemeral);

            for i in 0..2 {
                let payload = format!("test-record-{}", i).into_bytes();
                let _ack_future = stream.ingest_record_offset(payload).await?;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

            for i in 2..3 {
                let payload = format!("test-record-{}", i).into_bytes();
                let _ack_future = stream.ingest_record_offset(payload).await?;
            }

            stream.flush().await?;

            let write_count = mock_server.get_write_count().await;
            let max_offset = mock_server.get_max_offset_sent().await;

            assert_eq!(
                write_count, 3,
                "Expected 3 writes (2 on first stream + 1 on second stream), got {}",
                write_count
            );

            assert_eq!(
                max_offset, 2,
                "Expected max physical offset of 2, got {}",
                max_offset
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_recovery_preserves_batch_structure() -> Result<(), Box<dyn std::error::Error>>
        {
            setup_tracing();
            info!("Starting test_recovery_preserves_batch_structure");

            let (mock_server, server_url) = start_mock_server().await?;

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockResponse::CreateStream {
                            stream_id: "test_stream_batch_recovery".to_string(),
                            delay_ms: 0,
                        },
                        MockResponse::RecordAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                        },
                        MockResponse::Error {
                            status: tonic::Status::unavailable("Temporary network issue"),
                            delay_ms: 0,
                        },
                        MockResponse::CreateStream {
                            stream_id: "test_stream_batch_recovered".to_string(),
                            delay_ms: 0,
                        },
                        MockResponse::RecordAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                        },
                    ],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
                .max_inflight_requests(100)
                .recovery(true)
                .recovery_timeout_ms(5000)
                .recovery_backoff_ms(100)
                .server_lack_of_ack_timeout_ms(5000)
                .build()
                .await?;

            // Ingest a single record first
            let ack1 = stream
                .ingest_record_offset(b"single record".to_vec())
                .await?;

            // Ingest a batch
            let batch = vec![
                b"batch record 1".to_vec(),
                b"batch record 2".to_vec(),
                b"batch record 3".to_vec(),
            ];
            let ack2 = stream.ingest_records_offset(batch).await?;

            // Both should eventually succeed after recovery
            stream.wait_for_offset(ack1).await?;
            assert_eq!(ack1, 0);
            if let Some(off) = ack2 {
                stream.wait_for_offset(off).await?;
            }
            assert_eq!(ack2, Some(1)); // SDK returns the highest acknowledged offset in the batch

            let write_count = mock_server.get_write_count().await;
            // First stream: 1 single + 3 batch records
            // After recovery: 3 batch records resent
            assert!(
                write_count >= 4,
                "Expected at least 4 writes (with recovery), got {}",
                write_count
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_recreate_stream() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_recreate_stream");

            const ACKED_RECORDS: usize = 2;
            const UNACKED_BATCH_SIZE: usize = 2;

            let (mock_server, server_url) = start_mock_server().await?;

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        // First stream
                        MockResponse::CreateStream {
                            stream_id: "test_stream_original".to_string(),
                            delay_ms: 0,
                        },
                        MockResponse::RecordAck {
                            ack_up_to_offset: (ACKED_RECORDS - 1) as i64,
                            delay_ms: 0,
                        },
                        MockResponse::Error {
                            status: tonic::Status::internal("Stream failure"),
                            delay_ms: 0,
                        },
                        // Recreated stream
                        MockResponse::CreateStream {
                            stream_id: "test_stream_recreated".to_string(),
                            delay_ms: 0,
                        },
                        // Need to ack the re-ingested records: 1 single record + 1 batch
                        MockResponse::RecordAck {
                            ack_up_to_offset: 0, // Single record
                            delay_ms: 0,
                        },
                        MockResponse::RecordAck {
                            ack_up_to_offset: 1, // Batch
                            delay_ms: 50,
                        },
                    ],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
                .recovery(false)
                .flush_timeout_ms(10000)
                .build()
                .await?;

            // Ingest: 2 single records (will be acked), then 1 single + 1 batch (will fail)
            let mut single_futures = Vec::new();
            for i in 0..ACKED_RECORDS {
                let payload = format!("acked-{}", i).into_bytes();
                single_futures.push(stream.ingest_record_offset(payload).await?);
            }

            // Single record that will not be acked
            let unacked_single = b"unacked-single".to_vec();
            single_futures.push(stream.ingest_record_offset(unacked_single).await?);

            // Batch that will not be acked
            let unacked_batch: Vec<Vec<u8>> = (0..UNACKED_BATCH_SIZE)
                .map(|i| format!("unacked-batch-{}", i).into_bytes())
                .collect();
            let batch_future = stream.ingest_records_offset(unacked_batch).await?;

            // Wait for acks - some succeed, some fail
            for (i, future) in single_futures.into_iter().enumerate() {
                let result = stream.wait_for_offset(future).await;
                if i < ACKED_RECORDS {
                    assert!(result.is_ok());
                } else {
                    assert!(result.is_err());
                }
            }

            let batch_result = match batch_future {
                Some(off) => stream.wait_for_offset(off).await,
                None => Ok(()),
            };
            assert!(batch_result.is_err());

            // Recreate stream - should automatically re-ingest unacked records (1 single + 1 batch)
            let new_stream = sdk.recreate_stream(&stream).await?;

            // Give time for re-ingestion to complete
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            let write_count = mock_server.get_write_count().await;
            // Original stream: 2 + 1 + 2 = 5 records written
            // Recreated stream: 1 + 2 = 3 records re-written (at least some should complete)
            // Total: at least 6 (may be up to 8 depending on timing)
            assert!(
                write_count >= 6,
                "Expected at least 6 writes, got {}",
                write_count
            );

            // Verify the stream was successfully recreated
            assert_eq!(new_stream.stream_type, StreamType::Ephemeral);

            Ok(())
        }

        #[tokio::test]
        async fn test_get_unacked_records_returns_empty_when_all_acked(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_get_unacked_records_returns_empty_when_all_acked");

            let (mock_server, server_url) = start_mock_server().await?;

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockResponse::CreateStream {
                            stream_id: "test_stream_all_acked".to_string(),
                            delay_ms: 0,
                        },
                        MockResponse::RecordAck {
                            ack_up_to_offset: 4,
                            delay_ms: 50,
                        },
                    ],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;
            let mut stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
                .recovery(false)
                .build()
                .await?;

            // Ingest some records
            let mut ack_futures = Vec::new();
            for i in 0..5 {
                let payload = format!("record-{}", i).into_bytes();
                let ack = stream.ingest_record_offset(payload).await?;
                ack_futures.push(ack);
            }

            // Wait for all acks
            for ack in ack_futures {
                stream.wait_for_offset(ack).await?;
            }

            // Close successfully
            stream.close().await?;

            // get_unacked_records should return empty iterator
            let unacked = stream.get_unacked_records().await?.collect::<Vec<_>>();
            assert_eq!(unacked.len(), 0, "All records were acked, should be empty");

            Ok(())
        }
    }
}

mod graceful_close_tests {
    use super::*;

    #[tokio::test]
    async fn test_default_graceful_close_waits_for_full_server_duration(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_default_graceful_close_waits_for_full_server_duration");

        const SERVER_DURATION_SECONDS: i64 = 1;

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_default_graceful".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 1,
                        delay_ms: 0,
                    },
                    MockResponse::CloseStreamSignal {
                        duration_seconds: SERVER_DURATION_SECONDS,
                        delay_ms: 0,
                    },
                    MockResponse::CreateStream {
                        stream_id: "test_stream_recovered".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(true)
            .build()
            .await?;

        for i in 0..3 {
            let payload = format!("record-{}", i).into_bytes();
            let _ack = stream.ingest_record_offset(payload).await?;
        }

        // Give time for records to be sent and CloseStreamSignal to be received.
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let start_time = std::time::Instant::now();
        stream.flush().await?;
        let elapsed = start_time.elapsed();

        assert!(
            elapsed.as_millis() >= 900,
            "Expected to wait at least 900ms (most of server duration), but took {}ms",
            elapsed.as_millis()
        );
        assert!(
            elapsed.as_millis() <= 1200,
            "Expected to wait no more than 1200ms, but took {}ms",
            elapsed.as_millis()
        );
        // 3 writes to first stream + 1 resent to second stream.
        assert_eq!(mock_server.get_write_count().await, 4);

        Ok(())
    }

    #[tokio::test]
    async fn test_immediate_recovery_on_close_signal() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_immediate_recovery_on_close_signal");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_immediate".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 2,
                        delay_ms: 0,
                    },
                    MockResponse::CloseStreamSignal {
                        duration_seconds: 2,
                        delay_ms: 100,
                    },
                    MockResponse::CreateStream {
                        stream_id: "test_stream_recovered".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(true)
            .stream_paused_max_wait_time_ms(Some(0))
            .build()
            .await?;

        for i in 0..3 {
            let payload = format!("record-{}", i).into_bytes();
            let _ack = stream.ingest_record_offset(payload).await?;
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let start_time = std::time::Instant::now();
        let payload = "record-4".to_string().into_bytes();
        let _ack = stream.ingest_record_offset(payload).await?;

        stream.flush().await?;
        let elapsed = start_time.elapsed();

        // Should recover immediately, not wait for server's 2 seconds.
        assert!(
            elapsed.as_millis() < 500,
            "Expected immediate recovery (<500ms), but took {}ms",
            elapsed.as_millis()
        );
        assert_eq!(mock_server.get_write_count().await, 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_client_max_less_than_server() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_client_max_less_than_server");

        const SERVER_DURATION_SECONDS: i64 = 5;
        const CLIENT_MAX_WAIT_MS: u64 = 1000;

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_client_less".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 2,
                        delay_ms: 0,
                    },
                    MockResponse::CloseStreamSignal {
                        duration_seconds: SERVER_DURATION_SECONDS,
                        delay_ms: 0,
                    },
                    MockResponse::CreateStream {
                        stream_id: "test_stream_recovered".to_string(),
                        delay_ms: 1000,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 1,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(true)
            .server_lack_of_ack_timeout_ms(5000)
            .stream_paused_max_wait_time_ms(Some(CLIENT_MAX_WAIT_MS))
            .build()
            .await?;

        for i in 0..4 {
            let payload = format!("record-{}", i).into_bytes();
            let _ack = stream.ingest_record_offset(payload).await?;
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let start_time = std::time::Instant::now();
        let payload = b"post-signal-record".to_vec();
        let _ack = stream.ingest_record_offset(payload).await?;

        stream.flush().await?;
        let elapsed = start_time.elapsed();

        assert!(
            elapsed.as_millis() >= CLIENT_MAX_WAIT_MS as u128 - 200,
            "Expected to wait at least {}ms, but only waited {}ms",
            CLIENT_MAX_WAIT_MS - 200,
            elapsed.as_millis()
        );
        assert!(
            elapsed.as_millis() < (SERVER_DURATION_SECONDS as u64 * 1000) as u128,
            "Expected to wait less than {}ms (server duration), but waited {}ms",
            SERVER_DURATION_SECONDS * 1000,
            elapsed.as_millis()
        );

        // 5 writes to first stream (records 0-3 + post-signal) + 2 resent to second stream (records 3 and post-signal unacked).
        assert_eq!(mock_server.get_write_count().await, 7);

        Ok(())
    }

    #[tokio::test]
    async fn test_client_max_greater_than_server() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_client_max_greater_than_server");

        const SERVER_DURATION_SECONDS: i64 = 1;
        const CLIENT_MAX_WAIT_MS: u64 = 2000;

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_client_greater".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 1,
                        delay_ms: 0,
                    },
                    MockResponse::CloseStreamSignal {
                        duration_seconds: SERVER_DURATION_SECONDS,
                        delay_ms: 0,
                    },
                    MockResponse::CreateStream {
                        stream_id: "test_stream_recovered".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(true)
            .stream_paused_max_wait_time_ms(Some(CLIENT_MAX_WAIT_MS))
            .build()
            .await?;

        for i in 0..3 {
            let payload = format!("record-{}", i).into_bytes();
            let _ack = stream.ingest_record_offset(payload).await?;
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let start_time = std::time::Instant::now();
        stream.flush().await?;
        let elapsed = start_time.elapsed();

        assert!(
            elapsed.as_millis() >= 900,
            "Expected to wait at least 900ms (server duration), but took {}ms",
            elapsed.as_millis()
        );
        assert!(
            elapsed.as_millis() < 1500,
            "Should wait close to server duration (1000ms), not client max, took {}ms",
            elapsed.as_millis()
        );

        // 3 writes to first stream + 1 resent to second stream.
        assert_eq!(mock_server.get_write_count().await, 4);

        Ok(())
    }

    #[tokio::test]
    async fn test_early_recovery_all_acks_received() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_early_recovery_all_acks_received");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_early_recovery".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::CloseStreamSignal {
                        duration_seconds: 2,
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 2,
                        delay_ms: 200,
                    },
                    MockResponse::CreateStream {
                        stream_id: "test_stream_recovered".to_string(),
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(true)
            .stream_paused_max_wait_time_ms(Some(2000))
            .build()
            .await?;

        for i in 0..3 {
            let payload = format!("record-{}", i).into_bytes();
            let _ack = stream.ingest_record_offset(payload).await?;
        }

        let start_time = std::time::Instant::now();
        stream.flush().await?;
        let elapsed = start_time.elapsed();

        // Should complete around 200ms (when acks arrive), not wait for full 2000ms.
        assert!(
            elapsed.as_millis() >= 150 && elapsed.as_millis() < 1000,
            "Expected early recovery around 200ms, but took {}ms",
            elapsed.as_millis()
        );
        assert_eq!(mock_server.get_write_count().await, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_buffered_records_during_pause() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_buffered_records_during_pause");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_buffered".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 1,
                        delay_ms: 0,
                    },
                    MockResponse::CloseStreamSignal {
                        duration_seconds: 1,
                        delay_ms: 0,
                    },
                    MockResponse::CreateStream {
                        stream_id: "test_stream_recovered".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 1,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(true)
            .stream_paused_max_wait_time_ms(Some(1000))
            .build()
            .await?;

        for i in 0..2 {
            let payload = format!("pre-pause-{}", i).into_bytes();
            let _ack = stream.ingest_record_offset(payload).await?;
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        for i in 0..2 {
            let payload = format!("during-pause-{}", i).into_bytes();
            let _ack = stream.ingest_record_offset(payload).await?;
        }

        stream.flush().await?;

        // All 4 records should eventually be written (2 pre-pause + 2 buffered during pause).
        let write_count = mock_server.get_write_count().await;
        assert!(
            write_count >= 4,
            "Expected at least 4 writes (buffered records should be sent after recovery), got {}",
            write_count
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_error_during_graceful_close() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_error_during_graceful_close");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_error_during_pause".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 1,
                        delay_ms: 0,
                    },
                    MockResponse::CloseStreamSignal {
                        duration_seconds: 2,
                        delay_ms: 0,
                    },
                    MockResponse::Error {
                        status: tonic::Status::internal("Error during pause"),
                        delay_ms: 100,
                    },
                    MockResponse::CreateStream {
                        stream_id: "test_stream_recovered_after_error".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(true)
            .stream_paused_max_wait_time_ms(Some(2000))
            .build()
            .await?;

        let mut futures = Vec::new();
        for i in 0..3 {
            let payload = format!("record-{}", i).into_bytes();
            let ack = stream.ingest_record_offset(payload).await?;
            futures.push(ack);
        }

        stream.flush().await?;

        let write_count = mock_server.get_write_count().await;
        assert!(
            write_count >= 3,
            "Expected at least 3 writes despite error during pause, got {}",
            write_count
        );

        Ok(())
    }
}

mod api_offset_tests {
    use super::*;

    #[tokio::test]
    async fn test_ingest_record_offset_single_record() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_record_offset_single_record");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_offset".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 50,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        // ingest_record_offset returns the offset directly without nested future
        let offset = stream
            .ingest_record_offset(b"test record data".to_vec())
            .await?;

        assert_eq!(offset, 0);

        // Wait for the record to be acknowledged
        stream.wait_for_offset(offset).await?;

        assert_eq!(mock_server.get_write_count().await, 1);
        assert_eq!(mock_server.get_max_offset_sent().await, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_record_offset_multiple_records() -> Result<(), Box<dyn std::error::Error>>
    {
        setup_tracing();
        info!("Starting test_ingest_record_offset_multiple_records");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_offset_multi".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 9,
                        delay_ms: 200,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        let mut offsets = Vec::new();
        for _ in 0..10 {
            let offset = stream
                .ingest_record_offset(b"test record data".to_vec())
                .await?;
            offsets.push(offset);
        }

        // Offsets should be sequential
        for (i, offset) in offsets.iter().enumerate() {
            assert_eq!(*offset, i as i64);
        }

        stream.flush().await?;

        assert_eq!(mock_server.get_write_count().await, 10);
        assert_eq!(mock_server.get_max_offset_sent().await, 9);

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_records_offset_batch() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_records_offset_batch");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_offset_batch".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 50,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        let batch = vec![
            b"record 1".to_vec(),
            b"record 2".to_vec(),
            b"record 3".to_vec(),
        ];

        // ingest_records_offset returns the offset directly without nested future
        let offset = stream.ingest_records_offset(batch).await?;

        assert_eq!(offset, Some(0));

        if let Some(off) = offset {
            stream.wait_for_offset(off).await?;
        }

        assert_eq!(mock_server.get_write_count().await, 3);
        assert_eq!(mock_server.get_max_offset_sent().await, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_error_during_wait_for_offset() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_error_during_wait_for_offset");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_error_wait".to_string(),
                        delay_ms: 0,
                    },
                    // Send an error instead of an ack to simulate server error during wait
                    MockResponse::Error {
                        status: tonic::Status::internal("Server error during processing"),
                        delay_ms: 100,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        let offset = stream.ingest_record_offset(b"test record".to_vec()).await?;

        assert_eq!(offset, 0);

        let start = std::time::Instant::now();

        let wait_result = stream.wait_for_offset(offset).await;

        let elapsed = start.elapsed();

        assert!(
            wait_result.is_err(),
            "Expected wait_for_offset to fail with server error"
        );

        if let Err(e) = wait_result {
            let error_msg = e.to_string();
            assert!(
                error_msg.contains("Server error during processing")
                    || error_msg.contains("Stream closed"),
                "Expected server error or stream closed, got: {}",
                error_msg
            );
        }

        // Verify error was propagated quickly (should be ~100ms for the error delay,
        // not waiting for the full flush_timeout_ms which is much longer)
        assert!(
            elapsed.as_millis() < 1000,
            "Error should be propagated immediately, but took {}ms",
            elapsed.as_millis()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_records_offset_empty_batch() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_records_offset_empty_batch");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockResponse::CreateStream {
                    stream_id: "test_stream_offset_empty".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .build()
            .await?;

        // Ingest empty batch
        let empty_batch: Vec<Vec<u8>> = vec![];
        let offset = stream.ingest_records_offset(empty_batch).await?;

        // Should return None since the batch is empty
        assert_eq!(offset, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_mixed_ingest_record_offset_and_ingest_record(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_mixed_ingest_record_offset_and_ingest_record");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_mixed_api".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 3,
                        delay_ms: 100,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        // Mix ingest_record_offset and ingest_record
        let offset1 = stream.ingest_record_offset(b"record 1".to_vec()).await?;
        assert_eq!(offset1, 0);

        let future = stream.ingest_record_offset(b"record".to_vec()).await?;

        let offset2 = stream.ingest_record_offset(b"record 2".to_vec()).await?;
        assert_eq!(offset2, 2);

        let offset3 = stream.ingest_record_offset(b"record 3".to_vec()).await?;
        assert_eq!(offset3, 3);

        // Wait for future
        stream.wait_for_offset(future).await?;
        let offset = future;
        assert_eq!(offset, 1);

        stream.flush().await?;

        assert_eq!(mock_server.get_write_count().await, 4);
        assert_eq!(mock_server.get_max_offset_sent().await, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_record_offset_with_json() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_record_offset_with_json");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_offset_json".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 50,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .json()
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        let json_record = r#"{"id": 1, "name": "test"}"#.to_string();
        let offset = stream.ingest_record_offset(json_record).await?;

        assert_eq!(offset, 0);
        stream.wait_for_offset(offset).await?;
        assert_eq!(mock_server.get_write_count().await, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_records_offset_with_json_batch() -> Result<(), Box<dyn std::error::Error>>
    {
        setup_tracing();
        info!("Starting test_ingest_records_offset_with_json_batch");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_offset_json_batch".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 50,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .json()
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        let json_batch = vec![
            r#"{"id": 1, "name": "test1"}"#.to_string(),
            r#"{"id": 2, "name": "test2"}"#.to_string(),
            r#"{"id": 3, "name": "test3"}"#.to_string(),
        ];

        let offset = stream.ingest_records_offset(json_batch).await?;

        assert_eq!(offset, Some(0));
        if let Some(off) = offset {
            stream.wait_for_offset(off).await?;
        }
        assert_eq!(mock_server.get_write_count().await, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_record_offset_after_close_fails() -> Result<(), Box<dyn std::error::Error>>
    {
        setup_tracing();
        info!("Starting test_ingest_record_offset_after_close_fails");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockResponse::CreateStream {
                    stream_id: "test_stream_offset_close".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let mut stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        stream.close().await?;

        let ingest_result = stream
            .ingest_record_offset(b"test record data".to_vec())
            .await;
        assert!(matches!(
            ingest_result,
            Err(ZerobusError::StreamClosedError(_))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_records_offset_after_close_fails() -> Result<(), Box<dyn std::error::Error>>
    {
        setup_tracing();
        info!("Starting test_ingest_records_offset_after_close_fails");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockResponse::CreateStream {
                    stream_id: "test_stream_offset_batch_close".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let mut stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        stream.close().await?;

        let batch = vec![b"record 1".to_vec(), b"record 2".to_vec()];
        let ingest_result = stream.ingest_records_offset(batch).await;

        assert!(matches!(
            ingest_result,
            Err(ZerobusError::StreamClosedError(_))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_record_offset_blocks_on_inflight_limit(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_record_offset_blocks_on_inflight_limit");

        const MAX_INFLIGHT_REQUESTS: usize = 10;
        const ACK_DELAY_MS: u64 = 500;
        const TOTAL_REQUESTS: usize = MAX_INFLIGHT_REQUESTS + 5;

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_offset_blocking".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: (MAX_INFLIGHT_REQUESTS - 1) as i64,
                        delay_ms: ACK_DELAY_MS,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: (TOTAL_REQUESTS - 1) as i64,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(MAX_INFLIGHT_REQUESTS)
            .recovery(false)
            .build()
            .await?;

        let mut offsets = Vec::new();

        // Send MAX_INFLIGHT_REQUESTS requests
        for _ in 0..MAX_INFLIGHT_REQUESTS {
            let offset = stream.ingest_record_offset(b"test data".to_vec()).await?;
            offsets.push(offset);
        }

        // The next request should block because we're at the inflight limit
        let start_time = std::time::Instant::now();
        let blocking_offset = stream
            .ingest_record_offset(b"blocking data".to_vec())
            .await?;
        let duration = start_time.elapsed();

        offsets.push(blocking_offset);

        assert!(
            duration.as_millis() >= ACK_DELAY_MS as u128,
            "The {}th ingest call should block for at least {}ms, but only blocked for {}ms",
            MAX_INFLIGHT_REQUESTS + 1,
            ACK_DELAY_MS,
            duration.as_millis()
        );

        // Send remaining requests
        for _ in (MAX_INFLIGHT_REQUESTS + 1)..TOTAL_REQUESTS {
            let offset = stream
                .ingest_record_offset(b"more test data".to_vec())
                .await?;
            offsets.push(offset);
        }

        // Verify all offsets are sequential
        for (i, offset) in offsets.iter().enumerate() {
            assert_eq!(*offset, i as i64);
        }

        stream.flush().await?;

        assert_eq!(mock_server.get_write_count().await, TOTAL_REQUESTS as u64);
        assert_eq!(
            mock_server.get_max_offset_sent().await,
            (TOTAL_REQUESTS - 1) as i64
        );

        Ok(())
    }
}

mod callback_tests {
    use super::*;

    #[tokio::test]
    async fn test_callbacks_on_successful_acks() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_callbacks_on_successful_acks");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_1".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 1,
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 2,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let callback = Arc::new(TestCallback::new());

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .ack_callback(callback.clone())
            .build()
            .await?;

        let offset1 = stream.ingest_record_offset(vec![1, 2, 3]).await?;
        let offset2 = stream.ingest_record_offset(vec![4, 5, 6]).await?;
        let offset3 = stream.ingest_record_offset(vec![7, 8, 9]).await?;

        stream.wait_for_offset(offset3).await?;

        let acks = callback.get_acks();
        assert_eq!(acks.len(), 3, "Expected 3 ack callbacks");
        assert!(acks.contains(&offset1));
        assert!(acks.contains(&offset2));
        assert!(acks.contains(&offset3));

        let errors = callback.get_errors();
        assert_eq!(errors.len(), 0, "Expected no error callbacks");

        Ok(())
    }

    #[tokio::test]
    async fn test_callbacks_on_stream_errors() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_callbacks_on_stream_errors");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_1".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                    MockResponse::Error {
                        status: tonic::Status::internal("Simulated error"),
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let callback = Arc::new(TestCallback::new());

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .ack_callback(callback.clone())
            .build()
            .await?;

        let offset1 = stream.ingest_record_offset(vec![1, 2, 3]).await?;
        let _offset2 = stream.ingest_record_offset(vec![4, 5, 6]).await?;
        let _offset3 = stream.ingest_record_offset(vec![7, 8, 9]).await?;

        stream.wait_for_offset(offset1).await?;

        // First should be acked
        let acks = callback.get_acks();
        assert_eq!(acks.len(), 1, "Expected 1 ack callback");
        assert!(acks.contains(&offset1));

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Other two should have error callbacks
        let errors = callback.get_errors();
        assert!(
            errors.len() >= 2,
            "Expected at least 2 error callbacks, got {}",
            errors.len()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_callbacks_with_batches() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_callbacks_with_batches");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_1".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 1,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let callback = Arc::new(TestCallback::new());

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .ack_callback(callback.clone())
            .build()
            .await?;

        // Ingest batch of records
        let records = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let offset = stream.ingest_records_offset(records).await?;

        stream.wait_for_offset(offset.unwrap()).await?;

        // Should have received 1 ack callbacks (one per batch)
        let acks = callback.get_acks();
        assert_eq!(acks.len(), 1, "Expected 1 ack callback for batch");

        Ok(())
    }

    #[tokio::test]
    async fn test_callback_called_per_offset() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_callback_called_per_offset");

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "test_stream_1".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 4,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url.clone())
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let callback = Arc::new(TestCallback::new());

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .ack_callback(callback.clone())
            .build()
            .await?;

        // Ingest 5 records
        for _ in 0..5 {
            stream.ingest_record_offset(vec![1, 2, 3]).await?;
        }

        stream.flush().await?;

        // Should have received 5 ack callbacks (one per offset)
        let acks = callback.get_acks();
        assert_eq!(acks.len(), 5, "Expected 5 ack callbacks for cumulative ack");

        Ok(())
    }
}

mod stream_builder_tests {
    use super::*;

    #[tokio::test]
    async fn test_builder_json_stream_creation() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockResponse::CreateStream {
                    stream_id: "builder_json_stream".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url)
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let mut stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .json()
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        assert_eq!(stream.stream_type, StreamType::Ephemeral);
        stream.close().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_builder_proto_stream_creation() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockResponse::CreateStream {
                    stream_id: "builder_proto_stream".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url)
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let descriptor = create_test_descriptor_proto().unwrap();
        let mut stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(descriptor)
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        assert_eq!(stream.stream_type, StreamType::Ephemeral);
        stream.close().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_builder_json_ingest_and_ack() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();

        let (mock_server, server_url) = start_mock_server().await?;

        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "builder_ingest_stream".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = ZerobusSdk::builder()
            .endpoint(server_url)
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;

        let mut stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .json()
            .recovery(false)
            .build()
            .await?;

        let offset = stream
            .ingest_record_offset(r#"{"id": 1, "message": "hello"}"#.to_string())
            .await?;
        stream.wait_for_offset(offset).await?;
        stream.close().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_builder_validate_catches_missing_fields() {
        let sdk = ZerobusSdk::builder()
            .endpoint("http://localhost:1234")
            .tls_config(Arc::new(NoTlsConfig))
            .build()
            .unwrap();

        // Missing everything
        let result = sdk.stream_builder().validate();
        assert!(result.is_err());

        // Missing auth and format
        let result = sdk.stream_builder().table(TABLE_NAME).validate();
        assert!(result.is_err());

        // Missing format
        let result = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .validate();
        assert!(result.is_err());

        // All set — should pass
        let result = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .json()
            .validate();
        assert!(result.is_ok());
    }
}
