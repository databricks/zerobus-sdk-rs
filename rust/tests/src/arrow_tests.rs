mod mock_arrow_flight;
mod utils;

mod arrow_flight_tests {
    use std::sync::Arc;

    use databricks_zerobus_ingest_sdk::{NoTlsConfig, ZerobusSdk};
    use tracing::info;

    use crate::mock_arrow_flight::{start_mock_flight_server, MockFlightResponse};
    use crate::utils::{
        create_test_arrow_schema, create_test_dict_record_batch, create_test_dict_schema,
        create_test_record_batch, record_batch_to_ipc_bytes, setup_tracing, TestHeadersProvider,
    };

    const TABLE_NAME: &str = "test_catalog.test_schema.test_table";

    mod stream_creation_tests {
        use super::*;

        #[tokio::test]
        async fn test_successful_arrow_stream_creation() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_successful_arrow_stream_creation");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server.inject_responses(TABLE_NAME, vec![]).await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let result = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema)
                .build_arrow()
                .await;

            assert!(
                result.is_ok(),
                "Failed to create Arrow Flight stream: {:?}",
                result.err()
            );

            let stream = result.unwrap();
            assert_eq!(stream.table_name(), TABLE_NAME);
            assert!(!stream.is_closed());

            Ok(())
        }
    }

    mod ingestion_tests {
        use super::*;

        #[tokio::test]
        async fn test_ingest_single_batch() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_ingest_single_batch");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::BatchAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                        ack_up_to_records: 3,
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
                .arrow(schema.clone())
                .build_arrow()
                .await?;

            let batch = create_test_record_batch(
                schema,
                vec![1, 2, 3],
                vec![Some("hello"), Some("world"), None],
            );

            let offset = stream.ingest_batch(batch).await?;

            assert_eq!(offset, 0, "Expected offset 0 for first batch");

            stream.wait_for_offset(offset).await?;
            assert_eq!(mock_server.get_batch_count().await, 1);

            Ok(())
        }

        #[tokio::test]
        async fn test_ingest_multiple_batches() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_ingest_multiple_batches");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 2,
                        },
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 1,
                            delay_ms: 0,
                            ack_up_to_records: 4,
                        },
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 2,
                            delay_ms: 0,
                            ack_up_to_records: 6,
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
                .arrow(schema.clone())
                .build_arrow()
                .await?;

            let mut offsets = Vec::new();
            for i in 0..3 {
                let batch = create_test_record_batch(
                    schema.clone(),
                    vec![i * 10, i * 10 + 1],
                    vec![Some(&format!("batch-{}", i)), None],
                );
                let offset = stream.ingest_batch(batch).await?;
                assert_eq!(offset, i, "Expected offset {} for batch {}", i, i);
                offsets.push(offset);
            }

            for offset in offsets {
                stream.wait_for_offset(offset).await?;
            }

            assert_eq!(mock_server.get_batch_count().await, 3);
            assert_eq!(mock_server.get_max_offset_received().await, 2);

            Ok(())
        }
    }

    mod flush_and_close_tests {
        use super::*;

        #[tokio::test]
        async fn test_flush_waits_for_acks() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_flush_waits_for_acks");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 50,
                            ack_up_to_records: 1,
                        },
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 1,
                            delay_ms: 50,
                            ack_up_to_records: 2,
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
                .arrow(schema.clone())
                .flush_timeout_ms(5000)
                .build_arrow()
                .await?;

            for i in 0..2 {
                let batch = create_test_record_batch(schema.clone(), vec![i], vec![Some("test")]);
                let _ack = stream.ingest_batch(batch).await?;
            }

            stream.flush().await?;

            assert_eq!(mock_server.get_batch_count().await, 2);
            assert_eq!(mock_server.get_max_offset_received().await, 1);

            Ok(())
        }

        #[tokio::test]
        async fn test_close_flushes_and_closes() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_close_flushes_and_closes");

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

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let mut stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema.clone())
                .build_arrow()
                .await?;

            let batch = create_test_record_batch(schema, vec![1], vec![Some("test")]);
            let _ack = stream.ingest_batch(batch).await?;

            assert!(!stream.is_closed());
            stream.close().await?;
            assert!(stream.is_closed());

            Ok(())
        }
    }

    mod error_handling_tests {
        use tonic::Status;

        use super::*;

        #[tokio::test]
        async fn test_server_error_propagates() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_server_error_propagates");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::Error {
                        status: Status::invalid_argument("Schema mismatch"),
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
                .arrow(schema.clone())
                .server_lack_of_ack_timeout_ms(1000)
                .build_arrow()
                .await?;

            let batch = create_test_record_batch(schema, vec![1], vec![Some("test")]);
            let offset = stream.ingest_batch(batch).await?;

            let result = stream.wait_for_offset(offset).await;
            assert!(result.is_err(), "Expected error from server");

            Ok(())
        }

        #[tokio::test]
        async fn test_schema_mismatch_rejected() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_schema_mismatch_rejected");

            let (_mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema.clone())
                .build_arrow()
                .await?;

            use arrow_array::Int32Array;
            use arrow_schema::{DataType, Field, Schema};

            let wrong_schema = Arc::new(Schema::new(vec![Field::new(
                "different_field",
                DataType::Int32,
                false,
            )]));
            let wrong_batch = arrow_array::RecordBatch::try_new(
                wrong_schema,
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )?;

            let result = stream.ingest_batch(wrong_batch).await;
            assert!(
                result.is_err(),
                "Expected schema mismatch error, but got Ok"
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_ingest_after_close_rejected() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_ingest_after_close_rejected");

            let (_mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let mut stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema.clone())
                .build_arrow()
                .await?;

            stream.close().await?;

            let batch = create_test_record_batch(schema, vec![1], vec![Some("test")]);
            let result = stream.ingest_batch(batch).await;
            assert!(result.is_err(), "Expected error when ingesting after close");

            Ok(())
        }
    }

    mod timeout_tests {
        use super::*;

        #[tokio::test]
        async fn test_ack_timeout() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_ack_timeout");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::CloseStream { delay_ms: 0 }],
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
                .arrow(schema.clone())
                .server_lack_of_ack_timeout_ms(500)
                .flush_timeout_ms(2000)
                .recovery(false)
                .build_arrow()
                .await?;

            let batch = create_test_record_batch(schema, vec![1], vec![Some("test")]);
            let offset = stream.ingest_batch(batch).await?;

            let result = stream.wait_for_offset(offset).await;
            assert!(result.is_err(), "Expected timeout error");

            Ok(())
        }

        #[tokio::test]
        async fn test_flush_timeout() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_flush_timeout");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::BatchAck {
                        ack_up_to_offset: 0,
                        delay_ms: 5000,
                        ack_up_to_records: 1,
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
                .arrow(schema.clone())
                .flush_timeout_ms(100)
                .build_arrow()
                .await?;

            let batch = create_test_record_batch(schema, vec![1], vec![Some("test")]);
            let _ack = stream.ingest_batch(batch).await?;

            let result = stream.flush().await;
            assert!(result.is_err(), "Expected flush timeout error");

            Ok(())
        }
    }

    mod lifecycle_tests {
        use super::*;

        #[tokio::test]
        async fn test_idempotent_close() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_idempotent_close");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server.inject_responses(TABLE_NAME, vec![]).await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let mut stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema)
                .build_arrow()
                .await?;

            stream.close().await?;
            stream.close().await?;

            assert!(stream.is_closed());

            Ok(())
        }

        #[tokio::test]
        async fn test_empty_flush() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_empty_flush");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server.inject_responses(TABLE_NAME, vec![]).await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema)
                .build_arrow()
                .await?;

            let start_time = std::time::Instant::now();
            stream.flush().await?;
            let duration = start_time.elapsed();

            assert!(
                duration.as_millis() <= 1000,
                "Empty flush should complete quickly, took {:?}",
                duration
            );

            Ok(())
        }
    }

    mod concurrency_tests {
        use super::*;

        #[tokio::test]
        async fn test_concurrent_batch_ingestion() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_concurrent_batch_ingestion");

            const NUM_TASKS: usize = 5;
            const BATCHES_PER_TASK: usize = 2;
            const TOTAL_BATCHES: usize = NUM_TASKS * BATCHES_PER_TASK;

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            let mut responses = Vec::new();
            for i in 0..TOTAL_BATCHES {
                responses.push(MockFlightResponse::BatchAck {
                    ack_up_to_offset: i as i64,
                    delay_ms: 10,
                    ack_up_to_records: (i + 1) as u64,
                });
            }
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
                .arrow(schema.clone())
                .max_inflight_batches(TOTAL_BATCHES + 10)
                .build_arrow()
                .await?;
            let stream = Arc::new(stream);

            let mut tasks = Vec::new();
            for task_id in 0..NUM_TASKS {
                let stream_clone = Arc::clone(&stream);
                let schema_clone = schema.clone();
                let task = tokio::spawn(async move {
                    let mut offsets = Vec::new();
                    for batch_id in 0..BATCHES_PER_TASK {
                        let batch = create_test_record_batch(
                            schema_clone.clone(),
                            vec![(task_id * 100 + batch_id) as i64],
                            vec![Some(&format!("task-{}-batch-{}", task_id, batch_id))],
                        );
                        match stream_clone.ingest_batch(batch).await {
                            Ok(offset) => offsets.push(offset),
                            Err(e) => return Err(e),
                        }
                    }
                    Ok(offsets)
                });
                tasks.push(task);
            }

            let mut all_offsets = Vec::new();
            for task in tasks {
                let offsets = task.await??;
                all_offsets.extend(offsets);
            }

            assert_eq!(all_offsets.len(), TOTAL_BATCHES);

            for offset in &all_offsets {
                stream.wait_for_offset(*offset).await?;
            }
            all_offsets.sort();

            let expected: Vec<i64> = (0..TOTAL_BATCHES as i64).collect();
            assert_eq!(all_offsets, expected);

            Ok(())
        }
    }

    mod unacked_tests {
        use super::*;

        #[tokio::test]
        async fn test_get_unacked_batches_empty_when_all_acked(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_get_unacked_batches_empty_when_all_acked");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 1,
                        },
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 1,
                            delay_ms: 0,
                            ack_up_to_records: 2,
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
                .arrow(schema.clone())
                .build_arrow()
                .await?;

            let batch1 = create_test_record_batch(schema.clone(), vec![1], vec![Some("test1")]);
            let offset1 = stream.ingest_batch(batch1).await?;
            stream.wait_for_offset(offset1).await?;

            let batch2 = create_test_record_batch(schema.clone(), vec![2], vec![Some("test2")]);
            let offset2 = stream.ingest_batch(batch2).await?;
            stream.wait_for_offset(offset2).await?;

            stream.close().await?;

            let unacked = stream.get_unacked_batches().await?;
            assert!(
                unacked.is_empty(),
                "All batches were acked, should be empty"
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_get_unacked_batches_after_failure() -> Result<(), Box<dyn std::error::Error>>
        {
            setup_tracing();
            info!("Starting test_get_unacked_batches_after_failure");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 1,
                        },
                        MockFlightResponse::Error {
                            status: tonic::Status::invalid_argument("Permanent failure"),
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
                .arrow(schema.clone())
                .recovery(false)
                .build_arrow()
                .await?;

            let batch1 = create_test_record_batch(schema.clone(), vec![1], vec![Some("acked")]);
            let offset1 = stream.ingest_batch(batch1).await?;
            assert!(stream.wait_for_offset(offset1).await.is_ok());

            let batch2 = create_test_record_batch(schema.clone(), vec![2], vec![Some("unacked")]);
            let offset2 = stream.ingest_batch(batch2).await?;
            assert!(stream.wait_for_offset(offset2).await.is_err());

            let _ = stream.close().await;

            let unacked = stream.get_unacked_batches().await?;
            assert_eq!(unacked.len(), 1, "Should have 1 unacked batch");

            Ok(())
        }
    }

    mod recovery_tests {
        use super::*;

        #[tokio::test]
        async fn test_supervisor_recovery_after_retriable_error(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_supervisor_recovery_after_retriable_error");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 1,
                        },
                        MockFlightResponse::Error {
                            status: tonic::Status::unavailable("Temporary network issue"),
                            delay_ms: 0,
                        },
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 1,
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
                .arrow(schema.clone())
                .recovery(true)
                .recovery_timeout_ms(5000)
                .recovery_backoff_ms(100)
                .recovery_retries(3)
                .build_arrow()
                .await?;

            let batch1 = create_test_record_batch(schema.clone(), vec![1], vec![Some("first")]);
            let offset1 = stream.ingest_batch(batch1).await?;
            assert_eq!(offset1, 0);
            stream.wait_for_offset(offset1).await?;

            let batch2 = create_test_record_batch(schema.clone(), vec![2], vec![Some("second")]);
            let offset2 = stream.ingest_batch(batch2).await?;

            let result = stream.wait_for_offset(offset2).await;
            assert!(result.is_ok(), "Expected recovery to succeed: {:?}", result);

            Ok(())
        }

        #[tokio::test]
        async fn test_recreate_arrow_stream() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_recreate_arrow_stream");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 1,
                        },
                        MockFlightResponse::Error {
                            status: tonic::Status::invalid_argument("Schema changed"),
                            delay_ms: 0,
                        },
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 1,
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
                .arrow(schema.clone())
                .recovery(false)
                .build_arrow()
                .await?;

            let batch1 = create_test_record_batch(schema.clone(), vec![1], vec![Some("acked")]);
            let offset1 = stream.ingest_batch(batch1).await?;
            stream.wait_for_offset(offset1).await?;

            let batch2 = create_test_record_batch(schema.clone(), vec![2], vec![Some("will-fail")]);
            let offset2 = stream.ingest_batch(batch2).await?;
            let _ = stream.wait_for_offset(offset2).await;

            let _ = stream.close().await;

            let new_stream = sdk.recreate_arrow_stream(&stream).await?;

            new_stream.flush().await?;

            assert!(!new_stream.is_closed());
            assert_eq!(new_stream.table_name(), TABLE_NAME);

            Ok(())
        }

        #[tokio::test]
        async fn test_record_based_acknowledgment() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_record_based_acknowledgment");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 3,
                        },
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 1,
                            delay_ms: 0,
                            ack_up_to_records: 5,
                        },
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 2,
                            delay_ms: 0,
                            ack_up_to_records: 9,
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
                .arrow(schema.clone())
                .build_arrow()
                .await?;

            let batch1 = create_test_record_batch(
                schema.clone(),
                vec![1, 2, 3],
                vec![Some("a"), Some("b"), Some("c")],
            );
            let offset1 = stream.ingest_batch(batch1).await?;
            stream.wait_for_offset(offset1).await?;

            let batch2 =
                create_test_record_batch(schema.clone(), vec![4, 5], vec![Some("d"), Some("e")]);
            let offset2 = stream.ingest_batch(batch2).await?;
            stream.wait_for_offset(offset2).await?;

            let batch3 = create_test_record_batch(
                schema.clone(),
                vec![6, 7, 8, 9],
                vec![Some("f"), Some("g"), Some("h"), Some("i")],
            );
            let offset3 = stream.ingest_batch(batch3).await?;
            stream.wait_for_offset(offset3).await?;

            stream.close().await?;

            Ok(())
        }

        #[tokio::test]
        async fn test_partial_batch_recovery() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_partial_batch_recovery");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 5,
                        },
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 1,
                            delay_ms: 0,
                            ack_up_to_records: 12,
                        },
                        MockFlightResponse::Error {
                            status: tonic::Status::unavailable("Connection lost"),
                            delay_ms: 0,
                        },
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 3,
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
                .arrow(schema.clone())
                .recovery(true)
                .recovery_timeout_ms(5000)
                .recovery_backoff_ms(100)
                .recovery_retries(3)
                .build_arrow()
                .await?;

            let batch1 = create_test_record_batch(
                schema.clone(),
                vec![1, 2, 3, 4, 5],
                vec![Some("a"), Some("b"), Some("c"), Some("d"), Some("e")],
            );
            let offset1 = stream.ingest_batch(batch1).await?;
            stream.wait_for_offset(offset1).await?;

            let batch2 = create_test_record_batch(
                schema.clone(),
                vec![6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                vec![
                    Some("f"),
                    Some("g"),
                    Some("h"),
                    Some("i"),
                    Some("j"),
                    Some("k"),
                    Some("l"),
                    Some("m"),
                    Some("n"),
                    Some("o"),
                ],
            );
            let offset2 = stream.ingest_batch(batch2).await?;

            let result = stream.wait_for_offset(offset2).await;
            assert!(
                result.is_ok(),
                "Expected partial batch recovery to succeed: {:?}",
                result
            );

            stream.close().await?;

            Ok(())
        }

        /// Regression test: after a non-close-signal recovery (server Error, ack timeout, etc.)
        /// the supervisor must set `is_paused = true` before reconnecting so that concurrent
        /// `ingest_batch` calls cannot send to the new connection with a stale wire offset.
        ///
        /// Setup: advance the wire offset counter to N by acking N batches on connection 1,
        /// then trigger a server Error while one more batch is pending.  Ingest a further batch
        /// immediately (simulating a concurrent caller) before recovery completes.  The mock
        /// validates strictly-sequential offsets on every new DoPut connection (resets to 0).
        /// If the pause gate is missing, the concurrent batch may be sent with offset N on the
        /// new connection → mock rejects with `NonIncrementalOffset` → recovery fails.
        #[tokio::test]
        async fn test_no_stale_offset_after_non_close_signal_recovery(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_no_stale_offset_after_non_close_signal_recovery");

            const INITIAL_BATCHES: usize = 5;

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // Ack each of the initial batches individually, then Error on the next one,
            // then ack the replay (which starts at offset 0 on the new connection).
            let mut responses: Vec<MockFlightResponse> = (0..INITIAL_BATCHES)
                .map(|i| MockFlightResponse::BatchAck {
                    ack_up_to_offset: i as i64,
                    delay_ms: 0,
                    ack_up_to_records: (i + 1) as u64,
                })
                .collect();
            responses.push(MockFlightResponse::Error {
                status: tonic::Status::unavailable("Simulated disconnect"),
                delay_ms: 0,
            });
            // On the new connection offsets restart from 0.  Ack covers all replayed rows.
            responses.push(MockFlightResponse::BatchAck {
                ack_up_to_offset: 0,
                delay_ms: 0,
                ack_up_to_records: (INITIAL_BATCHES + 2) as u64,
            });

            mock_server.inject_responses(TABLE_NAME, responses).await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url)
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema.clone())
                .recovery(true)
                .recovery_retries(3)
                .recovery_backoff_ms(100)
                .recovery_timeout_ms(10_000)
                .flush_timeout_ms(15_000)
                .build_arrow()
                .await?;

            // Ingest INITIAL_BATCHES batches and wait for each ack, advancing
            // the wire offset counter to INITIAL_BATCHES on connection 1.
            for i in 0..INITIAL_BATCHES {
                let batch =
                    create_test_record_batch(schema.clone(), vec![i as i64], vec![Some("init")]);
                let offset = stream.ingest_batch(batch).await?;
                stream.wait_for_offset(offset).await?;
            }

            // This batch lands in flight when the server fires the Error.
            let pending_batch = create_test_record_batch(
                schema.clone(),
                vec![INITIAL_BATCHES as i64],
                vec![Some("pending")],
            );
            let pending_offset = stream.ingest_batch(pending_batch).await?;

            let concurrent_batch = create_test_record_batch(
                schema.clone(),
                vec![(INITIAL_BATCHES + 1) as i64],
                vec![Some("concurrent")],
            );
            let concurrent_offset = stream.ingest_batch(concurrent_batch).await?;

            let r1 = stream.wait_for_offset(pending_offset).await;
            let r2 = stream.wait_for_offset(concurrent_offset).await;

            assert!(
                r1.is_ok(),
                "Pending batch recovery failed (stale offset on new connection?): {r1:?}",
            );
            assert!(
                r2.is_ok(),
                "Concurrent batch recovery failed (stale offset on new connection?): {r2:?}",
            );

            // All INITIAL_BATCHES + 2 rows must have physically reached the server.
            // This catches silent data loss: if a batch is falsely acked without being
            // sent, the server row count will fall short.
            let total_rows = mock_server.get_total_records_received().await;
            assert!(
                total_rows >= (INITIAL_BATCHES + 2) as u64,
                "Server received {total_rows} rows, expected at least {}",
                INITIAL_BATCHES + 2,
            );

            Ok(())
        }

        /// Verifies no records are silently dropped when many concurrent `ingest_batch`
        /// calls are in flight during a server-error recovery.
        ///
        /// The `is_paused` gate + `ingest_mutex` ordering must ensure that every batch
        /// pushed to `pending_batches` is either sent on the current connection or
        /// replayed on the new one — never buffered-but-unsent and then falsely acked.
        ///
        /// Uses a multi-threaded runtime so the ingest_mutex-release / is_paused-clear
        /// race that this PR fixes can actually manifest if the gate is broken.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn test_no_lost_records_during_non_close_signal_recovery(
        ) -> Result<(), Box<dyn std::error::Error>> {
            use futures::future::join_all;

            setup_tracing();
            info!("Starting test_no_lost_records_during_non_close_signal_recovery");

            const CONCURRENT_TASKS: usize = 10;
            const ROWS_PER_BATCH: usize = 3;
            const TOTAL_UNIQUE_ROWS: u64 = (CONCURRENT_TASKS * ROWS_PER_BATCH) as u64;

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // Fire Error before any ack → all CONCURRENT_TASKS batches are pending at recovery.
            // On the new connection, ack everything in one shot.
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::Error {
                            status: tonic::Status::unavailable("Simulated disconnect"),
                            delay_ms: 0,
                        },
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: CONCURRENT_TASKS as i64 - 1,
                            delay_ms: 0,
                            ack_up_to_records: TOTAL_UNIQUE_ROWS,
                        },
                    ],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url)
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = Arc::new(
                sdk.stream_builder()
                    .table(TABLE_NAME)
                    .headers_provider(Arc::new(TestHeadersProvider::default()))
                    .arrow(schema.clone())
                    .recovery(true)
                    .recovery_retries(3)
                    .recovery_backoff_ms(50)
                    .recovery_timeout_ms(10_000)
                    .flush_timeout_ms(15_000)
                    .build_arrow()
                    .await?,
            );

            // Kick off CONCURRENT_TASKS ingest_batch calls simultaneously.
            let handles: Vec<_> = (0..CONCURRENT_TASKS)
                .map(|i| {
                    let stream = Arc::clone(&stream);
                    let schema = schema.clone();
                    tokio::spawn(async move {
                        let ids: Vec<i64> = (0..ROWS_PER_BATCH as i64)
                            .map(|r| i as i64 * 100 + r)
                            .collect();
                        let strings: Vec<Option<&str>> = vec![Some("row"); ROWS_PER_BATCH];
                        let batch = create_test_record_batch(schema, ids, strings);
                        stream.ingest_batch(batch).await
                    })
                })
                .collect();

            let offsets: Vec<_> = join_all(handles)
                .await
                .into_iter()
                .enumerate()
                .map(|(i, r)| {
                    r.unwrap_or_else(|e| panic!("task {i} panicked: {e:?}"))
                        .unwrap_or_else(|e| panic!("task {i} ingest_batch failed: {e}"))
                })
                .collect();

            // Every batch must be acknowledged — a falsely-acked batch would cause
            // wait_for_offset to succeed even though the server never received it,
            // but the row-count assertion below would catch the discrepancy.
            for offset in &offsets {
                stream.wait_for_offset(*offset).await?;
            }

            // The server must have physically received at least TOTAL_UNIQUE_ROWS rows.
            // Replay means the actual count may be higher, but never lower — a shortfall
            // indicates a batch was falsely acked without being sent to the server.
            let total_rows = mock_server.get_total_records_received().await;
            assert!(
                total_rows >= TOTAL_UNIQUE_ROWS,
                "Server received {total_rows} rows; expected at least {TOTAL_UNIQUE_ROWS} \
                 (possible silent data loss from pause-gate race)",
            );

            Ok(())
        }

        /// Same as `test_no_lost_records_during_non_close_signal_recovery` but recovery
        /// is triggered by a server-sent graceful-close signal rather than an error.
        ///
        /// The graceful-close path sets `is_paused = true` inside `process_acks` and
        /// then waits for the grace period to expire before triggering reconnect.
        /// Concurrent `ingest_batch` callers that arrive after the close signal must
        /// buffer into `pending_batches` and be replayed — not silently dropped.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn test_no_lost_records_during_close_signal_recovery(
        ) -> Result<(), Box<dyn std::error::Error>> {
            use futures::future::join_all;

            setup_tracing();
            info!("Starting test_no_lost_records_during_close_signal_recovery");

            const CONCURRENT_TASKS: usize = 10;
            const ROWS_PER_BATCH: usize = 3;
            const TOTAL_UNIQUE_ROWS: u64 = (CONCURRENT_TASKS * ROWS_PER_BATCH) as u64;

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // GracefulClose with a short grace period, then ack everything on the new connection.
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::GracefulClose {
                            duration_ms: 100,
                            delay_ms: 0,
                            ack_up_to_offset: None,
                            ack_up_to_records: None,
                        },
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: CONCURRENT_TASKS as i64 - 1,
                            delay_ms: 0,
                            ack_up_to_records: TOTAL_UNIQUE_ROWS,
                        },
                    ],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url)
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = Arc::new(
                sdk.stream_builder()
                    .table(TABLE_NAME)
                    .headers_provider(Arc::new(TestHeadersProvider::default()))
                    .arrow(schema.clone())
                    .recovery(true)
                    .recovery_retries(3)
                    .recovery_backoff_ms(50)
                    .recovery_timeout_ms(10_000)
                    .flush_timeout_ms(15_000)
                    .build_arrow()
                    .await?,
            );

            let handles: Vec<_> = (0..CONCURRENT_TASKS)
                .map(|i| {
                    let stream = Arc::clone(&stream);
                    let schema = schema.clone();
                    tokio::spawn(async move {
                        let ids: Vec<i64> = (0..ROWS_PER_BATCH as i64)
                            .map(|r| i as i64 * 100 + r)
                            .collect();
                        let strings: Vec<Option<&str>> = vec![Some("row"); ROWS_PER_BATCH];
                        let batch = create_test_record_batch(schema, ids, strings);
                        stream.ingest_batch(batch).await
                    })
                })
                .collect();

            let offsets: Vec<_> = join_all(handles)
                .await
                .into_iter()
                .enumerate()
                .map(|(i, r)| {
                    r.unwrap_or_else(|e| panic!("task {i} panicked: {e:?}"))
                        .unwrap_or_else(|e| panic!("task {i} ingest_batch failed: {e}"))
                })
                .collect();

            for offset in &offsets {
                stream.wait_for_offset(*offset).await?;
            }

            let total_rows = mock_server.get_total_records_received().await;
            assert!(
                total_rows >= TOTAL_UNIQUE_ROWS,
                "Server received {total_rows} rows; expected at least {TOTAL_UNIQUE_ROWS} \
                 (possible silent data loss from pause-gate race)",
            );

            Ok(())
        }
    }

    mod ipc_ingestion_tests {
        use super::*;
        use crate::utils::{ipc_bytes_to_record_batch, record_batch_to_ipc_bytes};

        #[tokio::test]
        async fn test_
        _basic() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_ingest_ipc_batch_basic");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::BatchAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                        ack_up_to_records: 3,
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
                .arrow(schema.clone())
                .build_arrow()
                .await?;

            let batch = create_test_record_batch(
                schema,
                vec![1, 2, 3],
                vec![Some("hello"), Some("world"), None],
            );
            let ipc_bytes = record_batch_to_ipc_bytes(&batch);

            let offset = stream.ingest_ipc_batch(ipc_bytes).await?;

            assert_eq!(offset, 0, "Expected offset 0 for first batch");
            stream.wait_for_offset(offset).await?;
            assert_eq!(mock_server.get_batch_count().await, 1);

            Ok(())
        }

        #[tokio::test]
        async fn test_ingest_ipc_batch_multiple() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_ingest_ipc_batch_multiple");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 2,
                        },
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 1,
                            delay_ms: 0,
                            ack_up_to_records: 4,
                        },
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 2,
                            delay_ms: 0,
                            ack_up_to_records: 6,
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
                .arrow(schema.clone())
                .build_arrow()
                .await?;

            let mut offsets = Vec::new();
            for i in 0..3i64 {
                let batch = create_test_record_batch(
                    schema.clone(),
                    vec![i * 10, i * 10 + 1],
                    vec![Some(&format!("batch-{}", i)), None],
                );
                let ipc_bytes = record_batch_to_ipc_bytes(&batch);
                let offset = stream.ingest_ipc_batch(ipc_bytes).await?;
                assert_eq!(offset, i);
                offsets.push(offset);
            }

            for offset in offsets {
                stream.wait_for_offset(offset).await?;
            }

            assert_eq!(mock_server.get_batch_count().await, 3);
            assert_eq!(mock_server.get_max_offset_received().await, 2);

            Ok(())
        }

        #[tokio::test]
        async fn test_ingest_ipc_batch_invalid_bytes() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_ingest_ipc_batch_invalid_bytes");

            let (_mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema)
                .build_arrow()
                .await?;

            let result = stream
                .ingest_ipc_batch(bytes::Bytes::from_static(b"not valid arrow ipc"))
                .await;

            assert!(
                result.is_err(),
                "Expected InvalidArgument for garbage bytes"
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_ingest_ipc_batch_after_close_rejected(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_ingest_ipc_batch_after_close_rejected");

            let (_mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let mut stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema.clone())
                .build_arrow()
                .await?;

            stream.close().await?;

            let batch = create_test_record_batch(schema, vec![1], vec![Some("test")]);
            let ipc_bytes = record_batch_to_ipc_bytes(&batch);
            let result = stream.ingest_ipc_batch(ipc_bytes).await;
            assert!(result.is_err(), "Expected error when ingesting after close");

            Ok(())
        }

        #[tokio::test]
        async fn test_ingest_ipc_batch_recovery() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_ingest_ipc_batch_recovery");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // First batch acked, then server drops. On reconnect the IPC batch is replayed.
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 1,
                        },
                        MockFlightResponse::Error {
                            status: tonic::Status::unavailable("Temporary network issue"),
                            delay_ms: 0,
                        },
                        // Replayed batch lands at offset 0 on the new connection
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 1,
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
                .arrow(schema.clone())
                .recovery(true)
                .recovery_timeout_ms(5000)
                .recovery_backoff_ms(100)
                .recovery_retries(3)
                .build_arrow()
                .await?;

            let batch1 = create_test_record_batch(schema.clone(), vec![1], vec![Some("first")]);
            let offset1 = stream
                .ingest_ipc_batch(record_batch_to_ipc_bytes(&batch1))
                .await?;
            assert_eq!(offset1, 0);
            stream.wait_for_offset(offset1).await?;

            // This batch triggers the error; supervisor reconnects and replays it
            let batch2 = create_test_record_batch(schema.clone(), vec![2], vec![Some("second")]);
            let offset2 = stream
                .ingest_ipc_batch(record_batch_to_ipc_bytes(&batch2))
                .await?;

            let result = stream.wait_for_offset(offset2).await;
            assert!(
                result.is_ok(),
                "Expected IPC batch recovery to succeed: {:?}",
                result
            );

            Ok(())
        }

        /// After a failure, `get_unacked_batches` should return the IPC payload as a
        /// deserialized `RecordBatch` with the original data intact.
        #[tokio::test]
        async fn test_ingest_ipc_batch_unacked_returns_record_batch(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_ingest_ipc_batch_unacked_returns_record_batch");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::Error {
                        status: tonic::Status::invalid_argument("Permanent failure"),
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
                .arrow(schema.clone())
                .recovery(false)
                .build_arrow()
                .await?;

            let original_batch =
                create_test_record_batch(schema, vec![42, 43], vec![Some("x"), Some("y")]);
            let ipc_bytes = record_batch_to_ipc_bytes(&original_batch);

            let offset = stream.ingest_ipc_batch(ipc_bytes).await?;
            let _ = stream.wait_for_offset(offset).await;
            let _ = stream.close().await;

            let unacked = stream.get_unacked_batches().await?;
            assert_eq!(unacked.len(), 1, "Should have 1 unacked batch");

            // Verify the deserialized batch matches the original
            let recovered = &unacked[0];
            assert_eq!(recovered.num_rows(), original_batch.num_rows());
            assert_eq!(recovered.schema(), original_batch.schema());

            // Round-trip the recovered batch through IPC to compare column data
            let recovered_ipc = record_batch_to_ipc_bytes(recovered);
            let recovered_rt = ipc_bytes_to_record_batch(&recovered_ipc);
            assert_eq!(recovered_rt.column(0), original_batch.column(0));
            assert_eq!(recovered_rt.column(1), original_batch.column(1));

            Ok(())
        }

        #[tokio::test]
        async fn test_ingest_ipc_batch_schema_mismatch() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_ingest_ipc_batch_schema_mismatch");

            let (_mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema)
                .build_arrow()
                .await?;

            // Create IPC bytes with a different schema.
            use arrow_array::Int32Array;
            use arrow_schema::{DataType, Field, Schema};
            let wrong_schema = Arc::new(Schema::new(vec![Field::new(
                "different_field",
                DataType::Int32,
                false,
            )]));
            let wrong_batch = arrow_array::RecordBatch::try_new(
                wrong_schema,
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )?;
            let ipc_bytes = record_batch_to_ipc_bytes(&wrong_batch);

            let result = stream.ingest_ipc_batch(ipc_bytes).await;
            assert!(result.is_err(), "Expected schema mismatch error");
            let err_msg = format!("{}", result.unwrap_err());
            assert!(
                err_msg.contains("schema does not match"),
                "Error should mention schema mismatch: {err_msg}"
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_ingest_ipc_batch_with_compression() -> Result<(), Box<dyn std::error::Error>>
        {
            setup_tracing();
            info!("Starting test_ingest_ipc_batch_with_compression");

            let (_mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema.clone())
                .ipc_compression(Some(arrow_ipc::CompressionType::ZSTD))
                .build_arrow()
                .await?;

            // ingest_ipc_batch now materialises the IPC bytes and re-encodes with the
            // stream compression setting, so this should succeed.
            let batch = create_test_record_batch(schema, vec![1], vec![Some("test")]);
            let ipc_bytes = record_batch_to_ipc_bytes(&batch);
            let result = stream.ingest_ipc_batch(ipc_bytes).await;
            assert!(
                result.is_ok(),
                "ingest_ipc_batch should succeed when compression is enabled: {:?}",
                result.err()
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_mixed_ingest_batch_and_ipc() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_mixed_ingest_batch_and_ipc");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 2,
                        },
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 1,
                            delay_ms: 0,
                            ack_up_to_records: 5,
                        },
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 2,
                            delay_ms: 0,
                            ack_up_to_records: 7,
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
                .arrow(schema.clone())
                .build_arrow()
                .await?;

            // Interleave RecordBatch and IPC ingestion.
            let batch1 = create_test_record_batch(
                schema.clone(),
                vec![1, 2],
                vec![Some("native1"), Some("native2")],
            );
            let offset1 = stream.ingest_batch(batch1).await?;

            let batch2 = create_test_record_batch(
                schema.clone(),
                vec![3, 4, 5],
                vec![Some("ipc1"), Some("ipc2"), Some("ipc3")],
            );
            let ipc_bytes = record_batch_to_ipc_bytes(&batch2);
            let offset2 = stream.ingest_ipc_batch(ipc_bytes).await?;

            let batch3 = create_test_record_batch(
                schema,
                vec![6, 7],
                vec![Some("native3"), Some("native4")],
            );
            let offset3 = stream.ingest_batch(batch3).await?;

            stream.wait_for_offset(offset1).await?;
            stream.wait_for_offset(offset2).await?;
            stream.wait_for_offset(offset3).await?;

            assert_eq!(mock_server.get_batch_count().await, 3);

            Ok(())
        }
    }

    mod chunking_tests {
        use super::*;
        use crate::utils::create_large_test_record_batch;

        // 10 500 rows × 200-byte payload ≈ 2.12 MiB IPC-encoded — safely above the
        // 2 MiB chunking threshold and below tonic's 4 MiB default decode limit.
        // At 2 MiB/chunk this batch requires at least 2 physical Flight messages.
        const LARGE_BATCH_ROWS: usize = 10_500;
        const PAYLOAD_BYTES_PER_ROW: usize = 200;

        /// A large RecordBatch must be split into multiple physical Flight messages
        /// (`ingest_batch` path). Fails before the fix (batch_count == 1) and
        /// passes after (batch_count >= 2, max_offset >= 1).
        #[tokio::test]
        async fn test_large_batch_is_split_into_chunks() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_large_batch_is_split_into_chunks");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // Ack fires on the first chunk (offset 0) and covers all rows, so the
            // logical batch is fully acknowledged regardless of how many physical
            // chunks follow. Subsequent chunks hit the auto-ack path, but
            // pending_batches is already empty so no re-ack occurs.
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::BatchAck {
                        // Fire ack only when offset 1 arrives (second chunk).
                        // This guarantees both chunks are counted before wait_for_offset returns.
                        ack_up_to_offset: 1,
                        delay_ms: 0,
                        ack_up_to_records: LARGE_BATCH_ROWS as u64,
                    }],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url)
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema.clone())
                .flush_timeout_ms(10_000)
                .build_arrow()
                .await?;

            let batch =
                create_large_test_record_batch(schema, LARGE_BATCH_ROWS, PAYLOAD_BYTES_PER_ROW);
            let offset = stream.ingest_batch(batch).await?;
            assert_eq!(offset, 0);

            stream.wait_for_offset(offset).await?;

            // Without chunking the whole batch lands in one Flight message
            // (batch_count == 1, max_offset == 0).  With chunking in place each
            // chunk gets its own physical offset, so both counters grow.
            let batch_count = mock_server.get_batch_count().await;
            assert!(
                batch_count >= 2,
                "Expected large batch to be split into ≥2 Flight messages (chunks), got {batch_count}",
            );

            let max_offset = mock_server.get_max_offset_received().await;
            assert!(
                max_offset >= 1,
                "Expected max wire offset ≥1 (multiple chunks), got {max_offset}",
            );

            Ok(())
        }

        /// A large IPC batch must be split into multiple physical Flight messages
        /// (`ingest_ipc_batch` path). Same failure/pass criterion as the Batch test.
        #[tokio::test]
        async fn test_large_ipc_batch_is_split_into_chunks(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_large_ipc_batch_is_split_into_chunks");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::BatchAck {
                        // Fire ack only when offset 1 arrives (second chunk).
                        ack_up_to_offset: 1,
                        delay_ms: 0,
                        ack_up_to_records: LARGE_BATCH_ROWS as u64,
                    }],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url)
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema.clone())
                .flush_timeout_ms(10_000)
                .build_arrow()
                .await?;

            let batch =
                create_large_test_record_batch(schema, LARGE_BATCH_ROWS, PAYLOAD_BYTES_PER_ROW);
            let ipc_bytes = record_batch_to_ipc_bytes(&batch);

            let offset = stream.ingest_ipc_batch(ipc_bytes).await?;
            assert_eq!(offset, 0);

            stream.wait_for_offset(offset).await?;

            let batch_count = mock_server.get_batch_count().await;
            assert!(
                batch_count >= 2,
                "Expected large IPC batch to be split into ≥2 Flight messages (chunks), got {batch_count}",
            );

            let max_offset = mock_server.get_max_offset_received().await;
            assert!(
                max_offset >= 1,
                "Expected max wire offset ≥1 (multiple chunks), got {max_offset}",
            );

            Ok(())
        }

        /// When a large pending batch is replayed after a disconnect, the replay path
        /// must also chunk it — not blast a single oversized Flight message.
        ///
        /// Failure criterion (before fix):  batch_count == 2  (1 failed + 1 replayed, no chunking)
        /// Pass criterion   (after  fix):   batch_count >= 3  (1 failed + ≥2 replayed chunks)
        #[tokio::test]
        async fn test_large_batch_chunking_preserved_on_recovery(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_large_batch_chunking_preserved_on_recovery");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        // Drop the connection when the first (or only) chunk arrives,
                        // before sending any ack.
                        MockFlightResponse::Error {
                            status: tonic::Status::unavailable("Simulated disconnect"),
                            delay_ms: 0,
                        },
                        // On reconnect the batch is replayed. Ack fires on the second
                        // replayed chunk (offset 1 on the new connection), guaranteeing both
                        // chunks are counted before wait_for_offset returns.
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 1,
                            delay_ms: 0,
                            ack_up_to_records: LARGE_BATCH_ROWS as u64,
                        },
                    ],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url)
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema.clone())
                .recovery(true)
                .recovery_retries(3)
                .recovery_backoff_ms(100)
                .recovery_timeout_ms(10_000)
                .flush_timeout_ms(15_000)
                .build_arrow()
                .await?;

            let batch =
                create_large_test_record_batch(schema, LARGE_BATCH_ROWS, PAYLOAD_BYTES_PER_ROW);
            let offset = stream.ingest_batch(batch).await?;

            let result = stream.wait_for_offset(offset).await;
            assert!(
                result.is_ok(),
                "Expected recovery replay of large batch to succeed: {result:?}",
            );

            // batch_count accumulates across connections.
            // Before fix: ack_up_to_offset=1 never fires (only 1 chunk per connection),
            //             so wait_for_offset times out and result.is_ok() fails above.
            // After  fix: 1 (failed first chunk) + 2 (both replay chunks) = 3 ≥ 3.
            let batch_count = mock_server.get_batch_count().await;
            assert!(
                batch_count >= 3,
                "Expected ≥3 total Flight messages (1 failed + ≥2 replayed chunks), got {batch_count}",
            );

            Ok(())
        }
    }

    mod dictionary_tests {
        use super::*;

        #[tokio::test]
        async fn test_ingest_dict_batch() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_ingest_dict_batch");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_dict_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::BatchAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                        ack_up_to_records: 3,
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
                .arrow(schema.clone())
                .build_arrow()
                .await?;

            let batch = create_test_dict_record_batch(
                schema,
                vec![1, 2, 3],
                vec![Some("cat_a"), Some("cat_b"), Some("cat_a")],
            );

            let offset = stream.ingest_batch(batch).await?;
            stream.wait_for_offset(offset).await?;

            // The mock server should have received dictionary + batch messages.
            // get_batch_count counts FlightData messages after schema, so dictionaries count.
            assert!(mock_server.get_batch_count().await >= 1);

            Ok(())
        }

        #[tokio::test]
        async fn test_ingest_dict_ipc_batch() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_ingest_dict_ipc_batch");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_dict_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::BatchAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                        ack_up_to_records: 3,
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
                .arrow(schema.clone())
                .build_arrow()
                .await?;

            let batch = create_test_dict_record_batch(
                schema,
                vec![1, 2, 3],
                vec![Some("cat_a"), Some("cat_b"), Some("cat_a")],
            );
            let ipc_bytes = record_batch_to_ipc_bytes(&batch);

            let offset = stream.ingest_ipc_batch(ipc_bytes).await?;
            stream.wait_for_offset(offset).await?;

            assert!(mock_server.get_batch_count().await >= 1);

            Ok(())
        }
    }

    mod graceful_close_tests {
        use super::*;
        use std::time::Instant;

        #[tokio::test]
        async fn test_default_graceful_close_waits_for_full_server_duration(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_default_graceful_close_waits_for_full_server_duration (arrow)");

            const SERVER_DURATION_MS: u64 = 1000;

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // Ack first 2 batches, then send graceful close, then ack batch 2 on reconnect.
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 3,
                        },
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 1,
                            delay_ms: 0,
                            ack_up_to_records: 6,
                        },
                        // Send graceful close signal after batch 2 arrives.
                        MockFlightResponse::GracefulClose {
                            duration_ms: SERVER_DURATION_MS,
                            delay_ms: 0,
                            ack_up_to_offset: None,
                            ack_up_to_records: None,
                        },
                        // After recovery, the client replays batch 2. Ack it.
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 3,
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
                .arrow(schema.clone())
                .recovery(true)
                .recovery_backoff_ms(100)
                .recovery_retries(3)
                .build_arrow()
                .await?;

            // Ingest batch 0 and 1 - these will be acked.
            let batch0 = create_test_record_batch(
                schema.clone(),
                vec![1, 2, 3],
                vec![Some("a"), Some("b"), Some("c")],
            );
            let offset0 = stream.ingest_batch(batch0).await?;
            stream.wait_for_offset(offset0).await?;

            let batch1 = create_test_record_batch(
                schema.clone(),
                vec![4, 5, 6],
                vec![Some("d"), Some("e"), Some("f")],
            );
            let offset1 = stream.ingest_batch(batch1).await?;
            stream.wait_for_offset(offset1).await?;

            // Ingest batch 2 - triggers graceful close signal. The client should wait
            // for the full server duration before triggering recovery.
            let batch2 = create_test_record_batch(
                schema.clone(),
                vec![7, 8, 9],
                vec![Some("g"), Some("h"), Some("i")],
            );
            let start = Instant::now();
            let offset2 = stream.ingest_batch(batch2).await?;

            // Wait for batch 2 to be acked (after recovery replays it).
            stream.wait_for_offset(offset2).await?;
            let elapsed = start.elapsed();

            // Should have waited roughly the server duration before recovery.
            assert!(
                elapsed.as_millis() >= (SERVER_DURATION_MS as u128 - 200),
                "Expected to wait at least ~{}ms for graceful close, but only waited {}ms",
                SERVER_DURATION_MS,
                elapsed.as_millis()
            );

            stream.close().await?;
            Ok(())
        }

        #[tokio::test]
        async fn test_immediate_recovery_on_close_signal() -> Result<(), Box<dyn std::error::Error>>
        {
            setup_tracing();
            info!("Starting test_immediate_recovery_on_close_signal (arrow)");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 3,
                        },
                        // Send graceful close with long duration after batch 1.
                        MockFlightResponse::GracefulClose {
                            duration_ms: 10_000,
                            delay_ms: 0,
                            ack_up_to_offset: None,
                            ack_up_to_records: None,
                        },
                        // After immediate recovery, ack batch 1.
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 3,
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
                .arrow(schema.clone())
                .recovery(true)
                .recovery_backoff_ms(100)
                .recovery_retries(3)
                .stream_paused_max_wait_time_ms(Some(0))
                .build_arrow()
                .await?;

            let batch0 = create_test_record_batch(
                schema.clone(),
                vec![1, 2, 3],
                vec![Some("a"), Some("b"), Some("c")],
            );
            let offset0 = stream.ingest_batch(batch0).await?;
            stream.wait_for_offset(offset0).await?;

            // Batch 1 triggers graceful close. With stream_paused_max_wait_time_ms=0,
            // recovery should be triggered immediately.
            let batch1 = create_test_record_batch(
                schema.clone(),
                vec![4, 5, 6],
                vec![Some("d"), Some("e"), Some("f")],
            );
            let start = Instant::now();
            let offset1 = stream.ingest_batch(batch1).await?;

            stream.wait_for_offset(offset1).await?;
            let elapsed = start.elapsed();

            // Should recover quickly, not wait 10 seconds.
            assert!(
                elapsed.as_millis() < 5000,
                "Expected immediate recovery, but waited {}ms",
                elapsed.as_millis()
            );

            stream.close().await?;
            Ok(())
        }

        #[tokio::test]
        async fn test_client_max_less_than_server() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_client_max_less_than_server (arrow)");

            const SERVER_DURATION_MS: u64 = 5000;
            const CLIENT_MAX_WAIT_MS: u64 = 500;

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        // Send graceful close after batch 0.
                        MockFlightResponse::GracefulClose {
                            duration_ms: SERVER_DURATION_MS,
                            delay_ms: 0,
                            ack_up_to_offset: None,
                            ack_up_to_records: None,
                        },
                        // After recovery, ack batch 0.
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 3,
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
                .arrow(schema.clone())
                .recovery(true)
                .recovery_backoff_ms(100)
                .recovery_retries(3)
                .stream_paused_max_wait_time_ms(Some(CLIENT_MAX_WAIT_MS))
                .build_arrow()
                .await?;

            let batch0 = create_test_record_batch(
                schema.clone(),
                vec![1, 2, 3],
                vec![Some("a"), Some("b"), Some("c")],
            );
            let start = Instant::now();
            let offset0 = stream.ingest_batch(batch0).await?;

            stream.wait_for_offset(offset0).await?;
            let elapsed = start.elapsed();

            // Should wait roughly CLIENT_MAX_WAIT_MS (not SERVER_DURATION_MS).
            assert!(
                elapsed.as_millis() >= (CLIENT_MAX_WAIT_MS as u128 - 200),
                "Expected to wait at least ~{}ms, but only waited {}ms",
                CLIENT_MAX_WAIT_MS,
                elapsed.as_millis()
            );
            assert!(
                elapsed.as_millis() < (SERVER_DURATION_MS as u128 - 500),
                "Expected to wait less than server duration {}ms, but waited {}ms",
                SERVER_DURATION_MS,
                elapsed.as_millis()
            );

            stream.close().await?;
            Ok(())
        }

        #[tokio::test]
        async fn test_all_acked_during_graceful_close_exits_early(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_all_acked_during_graceful_close_exits_early (arrow)");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        // Ack batch 0 first.
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 3,
                        },
                        // Send graceful close with long duration after batch 1.
                        // The close signal also carries ack data for batch 1,
                        // so all in-flight batches are acked at the same time.
                        MockFlightResponse::GracefulClose {
                            duration_ms: 10_000,
                            delay_ms: 0,
                            ack_up_to_offset: Some(1),
                            ack_up_to_records: Some(6),
                        },
                        // After early recovery, no batches need replay since all were acked.
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
                .arrow(schema.clone())
                .recovery(true)
                .recovery_backoff_ms(100)
                .recovery_retries(3)
                .stream_paused_max_wait_time_ms(None)
                .build_arrow()
                .await?;

            let batch0 = create_test_record_batch(
                schema.clone(),
                vec![1, 2, 3],
                vec![Some("a"), Some("b"), Some("c")],
            );
            let offset0 = stream.ingest_batch(batch0).await?;
            stream.wait_for_offset(offset0).await?;

            let batch1 = create_test_record_batch(
                schema.clone(),
                vec![4, 5, 6],
                vec![Some("d"), Some("e"), Some("f")],
            );
            let start = Instant::now();
            let offset1 = stream.ingest_batch(batch1).await?;

            // Batch 1 should be acked during the grace period (after 100ms delay).
            // The graceful close should exit early since all batches are acked.
            stream.wait_for_offset(offset1).await?;
            let elapsed = start.elapsed();

            // Should exit well before the 10s grace period.
            assert!(
                elapsed.as_millis() < 3000,
                "Expected early exit from graceful close, but waited {}ms",
                elapsed.as_millis()
            );

            stream.close().await?;
            Ok(())
        }

        #[tokio::test]
        async fn test_ingest_accepted_during_graceful_close(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_ingest_accepted_during_graceful_close (arrow)");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        // Send graceful close after batch 0.
                        MockFlightResponse::GracefulClose {
                            duration_ms: 5_000,
                            delay_ms: 0,
                            ack_up_to_offset: None,
                            ack_up_to_records: None,
                        },
                        // After recovery, ack both batches (batch 0 + batch 1 = 6 records).
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 1,
                            delay_ms: 0,
                            ack_up_to_records: 6,
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
                .arrow(schema.clone())
                .recovery(true)
                .recovery_backoff_ms(100)
                .recovery_retries(3)
                .stream_paused_max_wait_time_ms(None)
                .build_arrow()
                .await?;

            let batch0 = create_test_record_batch(
                schema.clone(),
                vec![1, 2, 3],
                vec![Some("a"), Some("b"), Some("c")],
            );
            let _offset0 = stream.ingest_batch(batch0).await?;

            // Wait a bit for the graceful close signal to be processed.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

            // Ingestion during the grace period should be accepted and buffered.
            // The batch will be replayed after recovery.
            let batch1 = create_test_record_batch(
                schema.clone(),
                vec![4, 5, 6],
                vec![Some("d"), Some("e"), Some("f")],
            );
            let offset1 = stream.ingest_batch(batch1).await?;
            assert_eq!(offset1, 1, "Expected offset 1 for second batch");

            // After recovery, both batches should be acked.
            stream.wait_for_offset(offset1).await?;

            stream.close().await?;
            Ok(())
        }
    }
}
