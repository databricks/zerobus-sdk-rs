mod mock_arrow_flight;
mod utils;

mod arrow_flight_tests {
    use std::sync::Arc;

    use databricks_zerobus_ingest_sdk::{
        ArrowStreamConfigurationOptions, ArrowTableProperties, NoTlsConfig, ZerobusSdk,
    };
    use tracing::info;

    use crate::mock_arrow_flight::{start_mock_flight_server, MockFlightResponse};
    use crate::utils::{
        create_test_arrow_schema, create_test_record_batch, setup_tracing, TestHeadersProvider,
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema,
            };

            let result = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    None,
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    None,
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    None,
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    Some(ArrowStreamConfigurationOptions {
                        flush_timeout_ms: 5000,
                        ..Default::default()
                    }),
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let mut stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    None,
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    Some(ArrowStreamConfigurationOptions {
                        server_lack_of_ack_timeout_ms: 1000,
                        ..Default::default()
                    }),
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    None,
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let mut stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    None,
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    Some(ArrowStreamConfigurationOptions {
                        server_lack_of_ack_timeout_ms: 500,
                        flush_timeout_ms: 2000,
                        recovery: false,
                        ..Default::default()
                    }),
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    Some(ArrowStreamConfigurationOptions {
                        flush_timeout_ms: 100,
                        ..Default::default()
                    }),
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema,
            };

            let mut stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    None,
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema,
            };

            let stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    None,
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    Some(ArrowStreamConfigurationOptions {
                        max_inflight_batches: TOTAL_BATCHES + 10,
                        ..Default::default()
                    }),
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let mut stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    None,
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let mut stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    Some(ArrowStreamConfigurationOptions {
                        recovery: false,
                        ..Default::default()
                    }),
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    Some(ArrowStreamConfigurationOptions {
                        recovery: true,
                        recovery_timeout_ms: 5000,
                        recovery_backoff_ms: 100,
                        recovery_retries: 3,
                        ..Default::default()
                    }),
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let mut stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    Some(ArrowStreamConfigurationOptions {
                        recovery: false,
                        ..Default::default()
                    }),
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let mut stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    None,
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let mut stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    Some(ArrowStreamConfigurationOptions {
                        recovery: true,
                        recovery_timeout_ms: 5000,
                        recovery_backoff_ms: 100,
                        recovery_retries: 3,
                        ..Default::default()
                    }),
                )
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
    }

    mod ipc_ingestion_tests {
        use super::*;
        use crate::utils::{ipc_bytes_to_record_batch, record_batch_to_ipc_bytes};

        #[tokio::test]
        async fn test_ingest_ipc_batch_basic() -> Result<(), Box<dyn std::error::Error>> {
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    None,
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    None,
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema,
            };

            let stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    None,
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let mut stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    None,
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    Some(ArrowStreamConfigurationOptions {
                        recovery: true,
                        recovery_timeout_ms: 5000,
                        recovery_backoff_ms: 100,
                        recovery_retries: 3,
                        ..Default::default()
                    }),
                )
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

            let table_properties = ArrowTableProperties {
                table_name: TABLE_NAME.to_string(),
                schema: schema.clone(),
            };

            let mut stream = sdk
                .create_arrow_stream_with_headers_provider(
                    table_properties,
                    Arc::new(TestHeadersProvider::default()),
                    Some(ArrowStreamConfigurationOptions {
                        recovery: false,
                        ..Default::default()
                    }),
                )
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
    }
}
