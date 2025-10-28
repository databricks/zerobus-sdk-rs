mod mock_grpc;
mod utils;

use std::sync::{Arc, Once};
use utils::TestHeadersProvider;

use databricks_zerobus_ingest_sdk::{
    StreamConfigurationOptions, StreamType, TableProperties, ZerobusError, ZerobusSdk,
};
use mock_grpc::{start_mock_server, MockResponse};
use prost_reflect::prost_types;
use tracing::info;
use tracing_subscriber::EnvFilter;

const TABLE_NAME: &str = "test_catalog.test_schema.test_table";

static SETUP: Once = Once::new();

/// Setup tracing for tests.
fn setup_tracing() {
    SETUP.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_writer(std::io::stdout)
            .with_env_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
            )
            .try_init()
            .ok();
    });
}

/// Helper function to create a simple descriptor proto for testing.
fn create_test_descriptor_proto() -> prost_types::DescriptorProto {
    prost_types::DescriptorProto {
        name: Some("TestMessage".to_string()),
        field: vec![
            prost_types::FieldDescriptorProto {
                name: Some("id".to_string()),
                number: Some(1),
                r#type: Some(prost_types::field_descriptor_proto::Type::Int64 as i32),
                ..Default::default()
            },
            prost_types::FieldDescriptorProto {
                name: Some("message".to_string()),
                number: Some(2),
                r#type: Some(prost_types::field_descriptor_proto::Type::String as i32),
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

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

        let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
        sdk.use_tls = false;

        let table_properties = TableProperties {
            table_name: TABLE_NAME.to_string(),
            descriptor_proto: create_test_descriptor_proto(),
        };

        let options = StreamConfigurationOptions {
            max_inflight_records: 100,
            recovery: false,
            ..Default::default()
        };

        let result = sdk
            .create_stream_with_headers_provider(
                table_properties,
                Arc::new(TestHeadersProvider {}),
                Some(options),
            )
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

        let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
        sdk.use_tls = false;

        let table_properties = TableProperties {
            table_name: TABLE_NAME.to_string(),
            descriptor_proto: create_test_descriptor_proto(),
        };

        let options = StreamConfigurationOptions {
            max_inflight_records: 100,
            recovery_timeout_ms: 100,
            recovery: false,
            ..Default::default()
        };

        let result = sdk
            .create_stream_with_headers_provider(
                table_properties,
                Arc::new(TestHeadersProvider {}),
                Some(options),
            )
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

        let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
        sdk.use_tls = false;

        let table_properties = TableProperties {
            table_name: TABLE_NAME.to_string(),
            descriptor_proto: create_test_descriptor_proto(),
        };

        let options = StreamConfigurationOptions {
            max_inflight_records: 100,
            recovery: true,
            ..Default::default()
        };

        let result = sdk
            .create_stream_with_headers_provider(
                table_properties,
                Arc::new(TestHeadersProvider {}),
                Some(options),
            )
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

        let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
        sdk.use_tls = false;

        let table_properties = TableProperties {
            table_name: TABLE_NAME.to_string(),
            descriptor_proto: create_test_descriptor_proto(),
        };

        let options = StreamConfigurationOptions {
            max_inflight_records: 100,
            recovery: false,
            recovery_timeout_ms: 200,
            recovery_backoff_ms: 200,
            ..Default::default()
        };
        let start_time = std::time::Instant::now();

        let result = sdk
            .create_stream_with_headers_provider(
                table_properties,
                Arc::new(TestHeadersProvider {}),
                Some(options),
            )
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

        let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
        sdk.use_tls = false;

        let table_properties = TableProperties {
            table_name: TABLE_NAME.to_string(),
            descriptor_proto: create_test_descriptor_proto(),
        };

        let options = StreamConfigurationOptions {
            max_inflight_records: 100,
            recovery: false,
            ..Default::default()
        };

        let mut stream = sdk
            .create_stream_with_headers_provider(
                table_properties,
                Arc::new(TestHeadersProvider {}),
                Some(options),
            )
            .await?;

        let test_record = b"test record data".to_vec();

        let ack_future = stream.ingest_record(test_record).await?;
        let _offset_id = ack_future.await?;

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

        let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
        sdk.use_tls = false;

        let table_properties = TableProperties {
            table_name: TABLE_NAME.to_string(),
            descriptor_proto: create_test_descriptor_proto(),
        };

        let options = StreamConfigurationOptions {
            max_inflight_records: 100,
            recovery: false,
            ..Default::default()
        };

        let mut stream = sdk
            .create_stream_with_headers_provider(
                table_properties,
                Arc::new(TestHeadersProvider {}),
                Some(options),
            )
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
                vec![MockResponse::CreateStream {
                    stream_id: "test_stream_1".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
        sdk.use_tls = false;

        let table_properties = TableProperties {
            table_name: TABLE_NAME.to_string(),
            descriptor_proto: create_test_descriptor_proto(),
        };

        let options = StreamConfigurationOptions {
            max_inflight_records: 100,
            recovery: false,
            ..Default::default()
        };

        let mut stream = sdk
            .create_stream_with_headers_provider(
                table_properties,
                Arc::new(TestHeadersProvider {}),
                Some(options),
            )
            .await?;

        stream.close().await?;
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

        let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
        sdk.use_tls = false;

        let table_properties = TableProperties {
            table_name: TABLE_NAME.to_string(),
            descriptor_proto: create_test_descriptor_proto(),
        };

        let options = StreamConfigurationOptions {
            max_inflight_records: 100,
            recovery: false,
            ..Default::default()
        };

        let mut stream = sdk
            .create_stream_with_headers_provider(
                table_properties,
                Arc::new(TestHeadersProvider {}),
                Some(options),
            )
            .await?;

        stream.close().await?;

        let ingest_result = stream.ingest_record(b"test record data".to_vec()).await;
        assert!(matches!(
            ingest_result,
            Err(ZerobusError::StreamClosedError(_))
        ));

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

        let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
        sdk.use_tls = false;

        let table_properties = TableProperties {
            table_name: TABLE_NAME.to_string(),
            descriptor_proto: create_test_descriptor_proto(),
        };

        let options = StreamConfigurationOptions {
            max_inflight_records: 100,
            recovery: false,
            ..Default::default()
        };

        let stream = sdk
            .create_stream_with_headers_provider(
                table_properties,
                Arc::new(TestHeadersProvider {}),
                Some(options),
            )
            .await?;

        let ingest_future = stream.ingest_record(b"test record data".to_vec()).await?;
        let ingest_result = ingest_future.await?;

        assert_eq!(ingest_result, 0);
        assert_eq!(mock_server.get_write_count().await, 1);
        assert_eq!(mock_server.get_max_offset_sent().await, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_batch_record_ingestion() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_batch_record_ingestion");

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

        let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
        sdk.use_tls = false;

        let table_properties = TableProperties {
            table_name: TABLE_NAME.to_string(),
            descriptor_proto: create_test_descriptor_proto(),
        };

        let options = StreamConfigurationOptions {
            max_inflight_records: 100,
            recovery: false,
            ..Default::default()
        };

        let stream = sdk
            .create_stream_with_headers_provider(
                table_properties,
                Arc::new(TestHeadersProvider {}),
                Some(options),
            )
            .await?;

        let mut ingest_futures = Vec::new();
        for _i in 0..100 {
            let ingest_future = stream.ingest_record(b"test record data".to_vec()).await?;
            ingest_futures.push(ingest_future);
        }

        for (i, ingest_future) in ingest_futures.into_iter().enumerate() {
            let ingest_result = ingest_future.await?;
            assert_eq!(ingest_result, i as i64);
        }

        assert_eq!(mock_server.get_write_count().await, 100);
        assert_eq!(mock_server.get_max_offset_sent().await, 99);

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

        let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
        sdk.use_tls = false;

        let table_properties = TableProperties {
            table_name: TABLE_NAME.to_string(),
            descriptor_proto: create_test_descriptor_proto(),
        };

        let options = StreamConfigurationOptions {
            max_inflight_records: 100,
            recovery: false,
            ..Default::default()
        };

        let stream = sdk
            .create_stream_with_headers_provider(
                table_properties,
                Arc::new(TestHeadersProvider {}),
                Some(options),
            )
            .await?;

        for _i in 0..5 {
            let _ingest_future = stream.ingest_record(b"test record data".to_vec()).await?;
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

        let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
        sdk.use_tls = false;

        let table_properties = TableProperties {
            table_name: TABLE_NAME.to_string(),
            descriptor_proto: create_test_descriptor_proto(),
        };

        let options = StreamConfigurationOptions {
            max_inflight_records: 100,
            recovery: false,
            flush_timeout_ms: 100,
            ..Default::default()
        };

        let stream = sdk
            .create_stream_with_headers_provider(
                table_properties,
                Arc::new(TestHeadersProvider {}),
                Some(options),
            )
            .await?;

        let mut ingest_futures = Vec::new();
        for _i in 0..5 {
            let ingest_future = stream.ingest_record(b"test record data".to_vec()).await?;
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

        let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
        sdk.use_tls = false;

        let table_properties = TableProperties {
            table_name: TABLE_NAME.to_string(),
            descriptor_proto: create_test_descriptor_proto(),
        };

        let options = StreamConfigurationOptions {
            max_inflight_records: 100,
            recovery: false,
            ..Default::default()
        };

        let stream = sdk
            .create_stream_with_headers_provider(
                table_properties,
                Arc::new(TestHeadersProvider {}),
                Some(options),
            )
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

        let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
        sdk.use_tls = false;

        let table_properties = TableProperties {
            table_name: TABLE_NAME.to_string(),
            descriptor_proto: create_test_descriptor_proto(),
        };

        let options = StreamConfigurationOptions {
            max_inflight_records: NUM_RECORDS,
            recovery: false,
            ..Default::default()
        };

        let stream = sdk
            .create_stream_with_headers_provider(
                table_properties,
                Arc::new(TestHeadersProvider {}),
                Some(options),
            )
            .await?;
        let stream = Arc::new(stream);

        let mut tasks = Vec::new();

        for _ in 0..NUM_TASKS {
            let stream_clone = Arc::clone(&stream);
            let task = tokio::spawn(async move {
                let mut ack_futures = Vec::new();
                for _ in 0..RECORDS_PER_TASK {
                    match stream_clone
                        .ingest_record(b"concurrent test data".to_vec())
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
            offsets.push(ack_future.await?);
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

        const MAX_INFLIGHT: usize = 10;
        const ACK_DELAY_MS: u64 = 500;
        const TOTAL_RECORDS: usize = MAX_INFLIGHT + 5;

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
                        ack_up_to_offset: (MAX_INFLIGHT - 1) as i64,
                        delay_ms: ACK_DELAY_MS,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: (TOTAL_RECORDS - 1) as i64,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
        sdk.use_tls = false;

        let table_properties = TableProperties {
            table_name: TABLE_NAME.to_string(),
            descriptor_proto: create_test_descriptor_proto(),
        };

        let options = StreamConfigurationOptions {
            max_inflight_records: MAX_INFLIGHT,
            recovery: false,
            ..Default::default()
        };

        let stream = sdk
            .create_stream_with_headers_provider(
                table_properties,
                Arc::new(TestHeadersProvider {}),
                Some(options),
            )
            .await?;

        let mut ack_futures = Vec::new();

        for _ in 0..MAX_INFLIGHT {
            let ack_future = stream.ingest_record(b"test data".to_vec()).await?;
            ack_futures.push(ack_future);
        }

        let start_time = std::time::Instant::now();
        let blocking_ack_future = stream.ingest_record(b"blocking data".to_vec()).await?;
        let duration = start_time.elapsed();

        ack_futures.push(blocking_ack_future);

        assert!(
            duration.as_millis() >= ACK_DELAY_MS as u128,
            "The 11th ingest call should block for at least {}ms, but only blocked for {}ms",
            ACK_DELAY_MS,
            duration.as_millis()
        );

        for _ in (MAX_INFLIGHT + 1)..TOTAL_RECORDS {
            let ack_future = stream.ingest_record(b"more test data".to_vec()).await?;
            ack_futures.push(ack_future);
        }

        for (i, ack) in ack_futures.into_iter().enumerate() {
            let offset = ack.await?;
            assert_eq!(offset, i as i64);
        }

        assert_eq!(mock_server.get_write_count().await, TOTAL_RECORDS as u64);
        assert_eq!(
            mock_server.get_max_offset_sent().await,
            (TOTAL_RECORDS - 1) as i64
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

        let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
        sdk.use_tls = false;
        let stream = sdk
            .create_stream_with_headers_provider(
                TableProperties {
                    table_name: TABLE_NAME.to_string(),
                    descriptor_proto: create_test_descriptor_proto(),
                },
                Arc::new(TestHeadersProvider {}),
                Some(StreamConfigurationOptions {
                    max_inflight_records: TOTAL_RECORDS + 5,
                    ..Default::default()
                }),
            )
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
                    .ingest_record(b"ingester data".to_vec())
                    .await
                {
                    futures.push(future);
                }
            }
            futures
        });

        let mut flusher_futures = Vec::new();
        for _ in 0..RECORDS_BEFORE_FLUSH {
            flusher_futures.push(stream.ingest_record(b"flusher data".to_vec()).await?);
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
            assert_eq!(future.await?, i as i64);
        }

        assert_eq!(mock_server.get_write_count().await, TOTAL_RECORDS as u64);
        Ok(())
    }

    #[tokio::test]
    async fn test_close_with_concurrent_ingestion() -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
        //TODO
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

            let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
            sdk.use_tls = false;
            let stream = sdk
                .create_stream_with_headers_provider(
                    TableProperties {
                        table_name: TABLE_NAME.to_string(),
                        descriptor_proto: create_test_descriptor_proto(),
                    },
                    Arc::new(TestHeadersProvider {}),
                    Some(StreamConfigurationOptions {
                        recovery: false,
                        ..Default::default()
                    }),
                )
                .await?;

            let ack_future_1 = stream.ingest_record(b"good data".to_vec()).await?;
            let ack_future_2 = stream.ingest_record(b"bad data".to_vec()).await?;

            assert_eq!(ack_future_1.await?, 0);

            let result_2 = ack_future_2.await;
            assert!(result_2.is_err());
            if let Err(e) = result_2 {
                assert!(e.to_string().contains("Receiver error"));
            }

            let ingest_3 = stream.ingest_record(b"more data".to_vec()).await;
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

            let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
            sdk.use_tls = false;
            let stream = sdk
                .create_stream_with_headers_provider(
                    TableProperties {
                        table_name: TABLE_NAME.to_string(),
                        descriptor_proto: create_test_descriptor_proto(),
                    },
                    Arc::new(TestHeadersProvider {}),
                    Some(StreamConfigurationOptions {
                        recovery: false,
                        server_lack_of_ack_timeout_ms: ACK_TIMEOUT_MS,
                        ..Default::default()
                    }),
                )
                .await?;

            let ack_future = stream.ingest_record(b"some data".to_vec()).await?;

            let result = ack_future.await;
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

            let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
            sdk.use_tls = false;
            let stream = sdk
                .create_stream_with_headers_provider(
                    TableProperties {
                        table_name: TABLE_NAME.to_string(),
                        descriptor_proto: create_test_descriptor_proto(),
                    },
                    Arc::new(TestHeadersProvider {}),
                    Some(StreamConfigurationOptions {
                        recovery: false,
                        ..Default::default()
                    }),
                )
                .await?;

            let mut futures = Vec::new();
            let mut unacked_payloads = Vec::new();

            for i in 0..TOTAL_RECORDS {
                let payload = format!("record-{}", i).into_bytes();
                if i >= ACKED_RECORDS {
                    unacked_payloads.push(payload.clone());
                }
                futures.push(stream.ingest_record(payload).await?);
            }

            for (i, future) in futures.into_iter().enumerate() {
                let result = future.await;
                if i < ACKED_RECORDS {
                    assert!(result.is_ok(), "First batch of records should be acked");
                } else {
                    assert!(result.is_err(), "Second batch of records should fail");
                }
            }

            let mut retrieved_unacked = stream.get_unacked_records().await?;
            retrieved_unacked.sort();
            unacked_payloads.sort();

            assert_eq!(retrieved_unacked, unacked_payloads);

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

            let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
            sdk.use_tls = false;
            let stream = sdk
                .create_stream_with_headers_provider(
                    TableProperties {
                        table_name: TABLE_NAME.to_string(),
                        descriptor_proto: create_test_descriptor_proto(),
                    },
                    Arc::new(TestHeadersProvider {}),
                    Some(StreamConfigurationOptions {
                        recovery: false,
                        ..Default::default()
                    }),
                )
                .await?;

            let mut futures = Vec::new();
            for i in 0..NUM_PENDING_RECORDS {
                let payload = format!("record-{}", i).into_bytes();
                futures.push(stream.ingest_record(payload).await?);
            }

            for future in futures {
                let result = future.await;
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

            let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
            sdk.use_tls = false;

            let table_properties = TableProperties {
                table_name: TABLE_NAME.to_string(),
                descriptor_proto: create_test_descriptor_proto(),
            };

            let options = StreamConfigurationOptions {
                max_inflight_records: 100,
                recovery: true,
                recovery_timeout_ms: 5000,
                recovery_backoff_ms: 100,
                ..Default::default()
            };

            let start_time = std::time::Instant::now();
            let result = sdk
                .create_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider {}),
                    Some(options),
                )
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
                let _ack_future = stream.ingest_record(payload).await?;
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

            let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
            sdk.use_tls = false;

            let table_properties = TableProperties {
                table_name: TABLE_NAME.to_string(),
                descriptor_proto: create_test_descriptor_proto(),
            };

            let options = StreamConfigurationOptions {
                max_inflight_records: 100,
                recovery: true,
                recovery_backoff_ms: 100,
                ..Default::default()
            };

            let start_time = std::time::Instant::now();
            let result = sdk
                .create_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider {}),
                    Some(options),
                )
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

            let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
            sdk.use_tls = false;

            let table_properties = TableProperties {
                table_name: TABLE_NAME.to_string(),
                descriptor_proto: create_test_descriptor_proto(),
            };

            let options = StreamConfigurationOptions {
                max_inflight_records: 100,
                recovery: true,
                recovery_timeout_ms: 5000,
                recovery_backoff_ms: 100,
                ..Default::default()
            };

            let stream = sdk
                .create_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider {}),
                    Some(options),
                )
                .await?;
            assert_eq!(stream.stream_type, StreamType::Ephemeral);

            let mut futures = Vec::new();
            for i in 0..5 {
                let payload = format!("test-record-{}", i).into_bytes();
                let ack_future = stream.ingest_record(payload).await?;
                futures.push(ack_future);
            }

            for (i, future) in futures.into_iter().enumerate() {
                let offset = future.await?;
                assert_eq!(offset, i as i64, "Record {} should have offset {}", i, i);
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

            let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
            sdk.use_tls = false;

            let table_properties = TableProperties {
                table_name: TABLE_NAME.to_string(),
                descriptor_proto: create_test_descriptor_proto(),
            };

            let options = StreamConfigurationOptions {
                max_inflight_records: 100,
                recovery: true,
                server_lack_of_ack_timeout_ms: ACK_TIMEOUT_MS,
                recovery_timeout_ms: 5000,
                recovery_backoff_ms: 100,
                ..Default::default()
            };

            let stream = sdk
                .create_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider {}),
                    Some(options),
                )
                .await?;
            assert_eq!(stream.stream_type, StreamType::Ephemeral);

            for i in 0..5 {
                let payload = format!("test-record-{}", i).into_bytes();
                let _ack_future = stream.ingest_record(payload).await?;
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

            let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
            sdk.use_tls = false;

            let table_properties = TableProperties {
                table_name: TABLE_NAME.to_string(),
                descriptor_proto: create_test_descriptor_proto(),
            };

            let options = StreamConfigurationOptions {
                max_inflight_records: 100,
                flush_timeout_ms: 200,
                ..Default::default()
            };

            let mut stream = sdk
                .create_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider {}),
                    Some(options),
                )
                .await?;
            assert_eq!(stream.stream_type, StreamType::Ephemeral);

            for i in 0..5 {
                let payload = format!("test-record-{}", i).into_bytes();
                let _ack_future = stream.ingest_record(payload).await?;
            }

            let close_result = stream.close().await;
            assert!(close_result.is_err(), "Expected close to fail");

            if let Err(e) = close_result {
                assert!(
                    e.to_string().contains("Flush timed out")
                        || e.to_string().contains("Stream closed"),
                    "Expected error related to flush timeout or stream closure, got: {:?}",
                    e
                );
            }

            let ingest_after_failed_close = stream.ingest_record(b"more data".to_vec()).await;
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

            let mut sdk = ZerobusSdk::new(server_url.clone(), "https://mock-uc.com".to_string())?;
            sdk.use_tls = false;

            let table_properties = TableProperties {
                table_name: TABLE_NAME.to_string(),
                descriptor_proto: create_test_descriptor_proto(),
            };

            let options = StreamConfigurationOptions {
                max_inflight_records: 100,
                recovery: true,
                recovery_timeout_ms: 5000,
                recovery_backoff_ms: 100,
                ..Default::default()
            };

            let stream = sdk
                .create_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider {}),
                    Some(options),
                )
                .await?;
            assert_eq!(stream.stream_type, StreamType::Ephemeral);

            for i in 0..2 {
                let payload = format!("test-record-{}", i).into_bytes();
                let _ack_future = stream.ingest_record(payload).await?;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

            for i in 2..3 {
                let payload = format!("test-record-{}", i).into_bytes();
                let _ack_future = stream.ingest_record(payload).await?;
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
    }
}
