mod mock_arrow_flight;
mod utils;

mod arrow_flight_tests {
    use std::sync::Arc;

    use databricks_zerobus_ingest_sdk::{NoTlsConfig, ZerobusError, ZerobusSdk};
    use tracing::info;

    use crate::mock_arrow_flight::{start_mock_flight_server, MockFlightResponse};
    use crate::utils::{
        advance_tokio_time_near_instant_limit, create_test_arrow_schema,
        create_test_dict_record_batch, create_test_dict_schema, create_test_record_batch,
        record_batch_to_ipc_bytes, setup_tracing, CountingHeadersProvider,
        HangingInvalidationHeadersProvider, TestHeadersProvider,
    };

    const TABLE_NAME: &str = "test_catalog.test_schema.test_table";

    /// Extracts the `id` (Int64) column values from a test batch, for asserting that a
    /// recovered/sliced batch contains the expected rows (not just the right row count).
    fn batch_ids(batch: &arrow_array::RecordBatch) -> Vec<i64> {
        use arrow_array::{Array, Int64Array};
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64")
            .values()
            .to_vec()
    }

    mod stream_creation_tests {
        use super::*;
        use tonic::Status;

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

        #[tokio::test]
        async fn test_initial_auth_rejection_refreshes_once_then_succeeds(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::FailSetup {
                        status: Status::unauthenticated("stale credential"),
                    }],
                )
                .await;

            let get_headers_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let invalidations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let provider = Arc::new(CountingHeadersProvider {
                get_headers_calls: Arc::clone(&get_headers_calls),
                invalidations: Arc::clone(&invalidations),
            });
            let sdk = ZerobusSdk::builder()
                .endpoint(server_url)
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(provider)
                .arrow(schema)
                .recovery(true)
                .recovery_backoff_ms(0)
                .recovery_retries(1)
                .build_arrow()
                .await?;

            assert_eq!(
                get_headers_calls.load(std::sync::atomic::Ordering::SeqCst),
                2,
                "the retry must fetch headers again after invalidation"
            );
            assert_eq!(
                invalidations.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "the rejected initial credential must be invalidated exactly once"
            );
            assert!(!stream.is_closed());

            Ok(())
        }

        #[tokio::test(start_paused = true)]
        async fn test_unrepresentable_recovery_timeout_is_rejected(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();

            let (_mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();
            let sdk = ZerobusSdk::builder()
                .endpoint(server_url)
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            advance_tokio_time_near_instant_limit().await;

            let stream_result = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema)
                .recovery_timeout_ms(u64::MAX)
                .build_arrow()
                .await;

            match stream_result {
                Err(ZerobusError::InvalidArgument(message)) => {
                    assert!(message.contains("recovery_timeout_ms"));
                }
                Err(error) => panic!("unexpected stream creation error: {error}"),
                Ok(_) => panic!("unrepresentable recovery timeout was accepted"),
            }

            Ok(())
        }

        #[tokio::test(start_paused = true)]
        async fn test_unrepresentable_ack_timeout_is_rejected(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();

            let (_mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();
            let sdk = ZerobusSdk::builder()
                .endpoint(server_url)
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            advance_tokio_time_near_instant_limit().await;

            let stream_result = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema)
                .server_lack_of_ack_timeout_ms(u64::MAX)
                .build_arrow()
                .await;

            match stream_result {
                Err(ZerobusError::InvalidArgument(message)) => {
                    assert!(message.contains("server_lack_of_ack_timeout_ms"));
                }
                Err(error) => panic!("unexpected stream creation error: {error}"),
                Ok(_) => panic!("unrepresentable ACK timeout was accepted"),
            }

            Ok(())
        }

        #[tokio::test]
        async fn test_repeated_initial_auth_rejection_stops_after_one_refresh(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::FailSetup {
                            status: Status::unauthenticated("stale credential"),
                        },
                        MockFlightResponse::FailSetup {
                            status: Status::permission_denied("refreshed credential rejected"),
                        },
                        // A regression that retries auth failures repeatedly would reach this
                        // third setup attempt instead of surfacing the second rejection.
                        MockFlightResponse::FailSetup {
                            status: Status::unauthenticated("unexpected third setup attempt"),
                        },
                    ],
                )
                .await;

            let get_headers_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let invalidations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let provider = Arc::new(CountingHeadersProvider {
                get_headers_calls: Arc::clone(&get_headers_calls),
                invalidations: Arc::clone(&invalidations),
            });
            let sdk = ZerobusSdk::builder()
                .endpoint(server_url)
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let result = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(provider)
                .arrow(schema)
                .recovery(true)
                .recovery_backoff_ms(0)
                .recovery_retries(5)
                .build_arrow()
                .await;
            let error = match result {
                Ok(_) => panic!("the second auth rejection must terminate initial setup"),
                Err(error) => error,
            };

            assert!(
                error.to_string().contains("refreshed credential rejected"),
                "the second auth rejection must surface unchanged, got: {error}"
            );
            assert!(!error.is_retryable());
            assert_eq!(
                get_headers_calls.load(std::sync::atomic::Ordering::SeqCst),
                2,
                "only the initial attempt and one refreshed attempt may fetch headers"
            );
            assert_eq!(
                invalidations.load(std::sync::atomic::Ordering::SeqCst),
                2,
                "each rejected credential is invalidated, but only the first gets a retry"
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_initial_non_auth_permanent_error_is_not_retried(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::FailSetup {
                        status: Status::invalid_argument("invalid initial schema"),
                    }],
                )
                .await;

            let get_headers_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let invalidations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let provider = Arc::new(CountingHeadersProvider {
                get_headers_calls: Arc::clone(&get_headers_calls),
                invalidations: Arc::clone(&invalidations),
            });
            let sdk = ZerobusSdk::builder()
                .endpoint(server_url)
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let result = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(provider)
                .arrow(schema)
                .recovery(true)
                .recovery_backoff_ms(0)
                .recovery_retries(5)
                .build_arrow()
                .await;
            let error = match result {
                Ok(_) => panic!("a permanent non-auth setup error must not be retried"),
                Err(error) => error,
            };

            assert!(
                error.to_string().contains("invalid initial schema"),
                "the original setup rejection must surface, got: {error}"
            );
            assert!(!error.is_retryable());
            assert_eq!(
                get_headers_calls.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "a permanent non-auth setup error must stop after the first attempt"
            );
            assert_eq!(
                invalidations.load(std::sync::atomic::Ordering::SeqCst),
                0,
                "non-auth setup failures must not invalidate credentials"
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_initial_auth_rejection_requires_retry_budget(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::FailSetup {
                        status: Status::unauthenticated("stale credential without retry budget"),
                    }],
                )
                .await;

            let get_headers_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let invalidations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let provider = Arc::new(CountingHeadersProvider {
                get_headers_calls: Arc::clone(&get_headers_calls),
                invalidations: Arc::clone(&invalidations),
            });
            let sdk = ZerobusSdk::builder()
                .endpoint(server_url)
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let result = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(provider)
                .arrow(schema)
                .recovery(true)
                .recovery_backoff_ms(0)
                .recovery_retries(0)
                .build_arrow()
                .await;
            let error = match result {
                Ok(_) => panic!("an auth rejection must not retry without recovery budget"),
                Err(error) => error,
            };

            assert!(error
                .to_string()
                .contains("stale credential without retry budget"));
            assert_eq!(
                get_headers_calls.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "zero retry budget must allow only the initial header fetch"
            );
            assert_eq!(
                invalidations.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "the rejected credential is invalidated even when no retry remains"
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_initial_auth_rejection_does_not_get_separate_retry_budget(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::FailSetup {
                            status: Status::unavailable("transient setup failure"),
                        },
                        MockFlightResponse::FailSetup {
                            status: Status::unauthenticated("auth rejected after budget spent"),
                        },
                    ],
                )
                .await;

            let get_headers_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let invalidations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let provider = Arc::new(CountingHeadersProvider {
                get_headers_calls: Arc::clone(&get_headers_calls),
                invalidations: Arc::clone(&invalidations),
            });
            let sdk = ZerobusSdk::builder()
                .endpoint(server_url)
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let result = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(provider)
                .arrow(schema)
                .recovery(true)
                .recovery_backoff_ms(0)
                .recovery_retries(1)
                .build_arrow()
                .await;
            let error = match result {
                Ok(_) => panic!("auth refresh must use the shared recovery budget"),
                Err(error) => error,
            };

            assert!(error
                .to_string()
                .contains("auth rejected after budget spent"));
            assert_eq!(
                get_headers_calls.load(std::sync::atomic::Ordering::SeqCst),
                2,
                "the exhausted shared budget must prevent a third attempt"
            );
            assert_eq!(
                invalidations.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "the auth rejection is invalidated but cannot receive an extra retry"
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_initial_auth_invalidation_timeout_preserves_one_retry_limit(
        ) -> Result<(), Box<dyn std::error::Error>> {
            const ATTEMPT_TIMEOUT_MS: u64 = 600;
            const SETUP_REJECTION_DELAY_MS: u64 = 300;
            const MAX_TWO_ATTEMPT_DURATION_MS: u64 = 1_500;

            setup_tracing();

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::FailSetupAfter {
                            status: Status::unauthenticated("first rejected credential"),
                            delay_ms: SETUP_REJECTION_DELAY_MS,
                        },
                        MockFlightResponse::FailSetupAfter {
                            status: Status::permission_denied("second rejected credential"),
                            delay_ms: SETUP_REJECTION_DELAY_MS,
                        },
                        // A stalled invalidation must not turn the auth path into generic
                        // timeout retries that consume the rest of the recovery budget.
                        MockFlightResponse::FailSetup {
                            status: Status::unauthenticated("unexpected third setup attempt"),
                        },
                    ],
                )
                .await;

            let invalidations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let cancellations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let provider = Arc::new(HangingInvalidationHeadersProvider {
                invalidations: Arc::clone(&invalidations),
                cancellations: Arc::clone(&cancellations),
            });
            let sdk = ZerobusSdk::builder()
                .endpoint(server_url)
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let started = std::time::Instant::now();
            let result = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(provider)
                .arrow(schema)
                .recovery(true)
                .recovery_backoff_ms(0)
                .recovery_timeout_ms(ATTEMPT_TIMEOUT_MS)
                .recovery_retries(5)
                .build_arrow()
                .await;
            let elapsed = started.elapsed();
            let error = match result {
                Ok(_) => panic!("stalled invalidation must not bypass the one-auth-retry limit"),
                Err(error) => error,
            };

            assert!(
                error.to_string().contains("second rejected credential"),
                "the final auth rejection must be preserved, got: {error}"
            );
            assert!(!error.is_retryable());
            assert!(
                elapsed < std::time::Duration::from_millis(MAX_TWO_ATTEMPT_DURATION_MS),
                "setup and invalidation must share each attempt timeout; elapsed: {elapsed:?}"
            );
            assert_eq!(
                invalidations.load(std::sync::atomic::Ordering::SeqCst),
                2,
                "only the initial attempt and one auth retry may invalidate credentials"
            );
            assert_eq!(
                cancellations.load(std::sync::atomic::Ordering::SeqCst),
                2,
                "each stalled invalidation must be cancelled at the setup deadline"
            );

            Ok(())
        }
    }

    mod ingestion_tests {
        use super::*;

        /// A zero-row batch must be rejected with InvalidArgument rather than accepted
        /// (the Flight encoder emits no data message for it, so it would never be acked
        /// and flush() would hang).
        #[tokio::test]
        async fn test_ingest_empty_batch_rejected() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_ingest_empty_batch_rejected");

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
                .arrow(schema.clone())
                .build_arrow()
                .await?;

            let empty = arrow_array::RecordBatch::new_empty(schema.clone());
            let err = stream
                .ingest_batch(empty)
                .await
                .expect_err("an empty batch must be rejected");
            assert!(
                err.to_string().to_lowercase().contains("empty"),
                "expected an empty-batch InvalidArgument, got: {}",
                err
            );
            assert!(
                !err.is_retryable(),
                "empty-batch rejection is non-retryable, got: {}",
                err
            );

            Ok(())
        }

        /// The client-side ingest gate compares batch and stream schemas with
        /// metadata included, so a stream created from a variant-annotated schema
        /// rejects a batch whose (physically identical) schema lacks the
        /// `arrow.parquet.variant` marker. This pins the marker as load-bearing:
        /// a caller who annotates the stream schema must also annotate its
        /// batches.
        #[tokio::test]
        async fn test_ingest_rejects_variant_marker_schema_mismatch(
        ) -> Result<(), Box<dyn std::error::Error>> {
            use std::sync::Arc;

            use arrow_array::{LargeBinaryArray, RecordBatch, StructArray};
            use arrow_schema::{DataType, Schema as ArrowSchema};
            use databricks_zerobus_ingest_sdk::schema::{
                arrow_schema_from_uc_columns, arrow_schema_from_uc_columns_with_options,
                ArrowSchemaOptions, UcColumn,
            };

            setup_tracing();
            info!("Starting test_ingest_rejects_variant_marker_schema_mismatch");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            mock_server.inject_responses(TABLE_NAME, vec![]).await;

            let cols = vec![UcColumn {
                name: "attrs".into(),
                type_name: "VARIANT".into(),
                type_text: "variant".into(),
                type_json: String::new(),
                nullable: false,
                position: 0,
            }];
            // Stream schema is annotated; the batch schema is not (same physical
            // shape). The marker is the only difference between them.
            let mut annotate = ArrowSchemaOptions::default();
            annotate.annotate_variant_extension = true;
            let marked_schema =
                Arc::new(arrow_schema_from_uc_columns_with_options(&cols, &annotate)?);
            let unmarked_schema = Arc::new(arrow_schema_from_uc_columns(&cols)?);

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;
            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(Arc::clone(&marked_schema))
                .build_arrow()
                .await?;

            // Build a variant batch against the unmarked schema.
            let DataType::Struct(child_fields) = unmarked_schema.field(0).data_type() else {
                panic!("expected VARIANT Struct");
            };
            let attrs = StructArray::new(
                child_fields.clone(),
                vec![
                    Arc::new(LargeBinaryArray::from(vec![b"m".as_slice()])),
                    Arc::new(LargeBinaryArray::from(vec![b"v".as_slice()])),
                ],
                None,
            );
            let batch = RecordBatch::try_new(Arc::clone(&unmarked_schema), vec![Arc::new(attrs)])?;
            // Sanity: the two schemas differ only by the marker metadata.
            assert_ne!(
                batch.schema().as_ref(),
                marked_schema.as_ref() as &ArrowSchema,
                "test setup: marked and unmarked schemas must differ"
            );

            let err = stream
                .ingest_batch(batch)
                .await
                .expect_err("a schema differing only by the variant marker must be rejected");
            assert!(
                err.to_string()
                    .to_lowercase()
                    .contains("schema does not match"),
                "expected a schema-mismatch InvalidArgument, got: {}",
                err
            );
            assert!(
                !err.is_retryable(),
                "schema-mismatch rejection is non-retryable, got: {}",
                err
            );

            Ok(())
        }

        /// A forward acknowledgement is a protocol violation, not a transient connection
        /// failure. Recovery must not hide it from a waiter or replay the pending batch.
        #[tokio::test]
        async fn test_forward_acknowledgement_is_terminal_with_recovery_enabled(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();

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
                .recovery_backoff_ms(0)
                .flush_timeout_ms(1_000)
                .build_arrow()
                .await?;

            let batch =
                create_test_record_batch(schema, vec![1, 2], vec![Some("first"), Some("second")]);
            let offset = stream.ingest_batch(batch).await?;
            let error = stream
                .wait_for_offset(offset)
                .await
                .expect_err("the invalid acknowledgement must reach the waiter");

            assert!(!error.is_retryable());
            match error {
                ZerobusError::InvalidStateError(message) => {
                    assert!(message.contains("3 records"));
                    assert!(message.contains("2 records were submitted"));
                }
                other => panic!("expected an invalid-state error, got {other:?}"),
            }
            assert_eq!(
                mock_server.get_batch_count().await,
                1,
                "a terminal protocol violation must not replay the batch"
            );

            Ok(())
        }

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

        /// An ack that lands before the stream closes must resolve wait_for_offset() as
        /// Ok(()), not a spurious closed error — otherwise a caller could retry a batch
        /// the server already acknowledged. The waiter checks the ack watermark before
        /// is_closed, so a durable target wins over closure.
        #[tokio::test]
        async fn test_wait_for_offset_ok_when_acked_before_close(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_wait_for_offset_ok_when_acked_before_close");

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

            let batch = create_test_record_batch(schema, vec![1], vec![Some("a")]);
            let offset = stream.ingest_batch(batch).await?;

            // close() flushes (the batch is acked), so is_closed is set with the ack
            // watermark already covering the target offset.
            stream.close().await?;

            // The target was acknowledged before closure -> Ok, not a closed error.
            stream.wait_for_offset(offset).await?;

            Ok(())
        }

        /// After the stream terminally closes, an already-acknowledged offset must still
        /// resolve as Ok (the ack wins over closure — the waiter re-reads the watermark
        /// after observing closure), while an un-acked offset returns the real terminal
        /// error.
        #[tokio::test]
        async fn test_wait_for_offset_acked_wins_over_terminal_close(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_wait_for_offset_acked_wins_over_terminal_close");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // A is acked (1 record); B then triggers a terminal error, closing the stream.
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
                            status: tonic::Status::invalid_argument("boom"),
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
                .arrow(schema.clone())
                .recovery(false)
                .build_arrow()
                .await?;

            let batch_a = create_test_record_batch(schema.clone(), vec![1], vec![Some("a")]);
            let a_offset = stream.ingest_batch(batch_a).await?;
            let batch_b = create_test_record_batch(schema.clone(), vec![2], vec![Some("b")]);
            let b_offset = stream.ingest_batch(batch_b).await?;

            // B drives the stream to a terminal close and surfaces the real error.
            let b_err = stream
                .wait_for_offset(b_offset)
                .await
                .expect_err("un-acked B must fail with the terminal error");
            assert!(
                b_err.to_string().contains("boom"),
                "un-acked offset must return the real terminal error, got: {}",
                b_err
            );

            // A was acknowledged before the close -> Ok, not the terminal error.
            stream.wait_for_offset(a_offset).await?;

            Ok(())
        }

        /// close() must surface a background terminal failure rather than returning Ok(()),
        /// so the ingest-then-close() pattern doesn't hide failed batches.
        #[tokio::test]
        async fn test_close_returns_terminal_error_after_background_failure(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_close_returns_terminal_error_after_background_failure");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // A is acked; B then triggers a terminal error that the supervisor closes on.
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
                            status: tonic::Status::invalid_argument("boom"),
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

            let batch_a = create_test_record_batch(schema.clone(), vec![1], vec![Some("a")]);
            stream.ingest_batch(batch_a).await?;
            let batch_b = create_test_record_batch(schema.clone(), vec![2], vec![Some("b")]);
            let b_offset = stream.ingest_batch(batch_b).await?;

            // Drive the supervisor to its terminal failure (B is never acked).
            let _ = stream.wait_for_offset(b_offset).await;

            // close() now runs against an already-closed stream and must surface the real
            // terminal error instead of Ok(()).
            let err = stream
                .close()
                .await
                .expect_err("close() must surface the background terminal failure");
            assert!(
                err.to_string().contains("boom"),
                "close() must return the real terminal error, got: {}",
                err
            );

            Ok(())
        }

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

        /// Cancelling close after supervisor/sender teardown must leave the stream in a
        /// resumable Closing state: new ingests are rejected, retrieval remains disabled,
        /// and resumed/repeated close calls return the original flush error without waiting
        /// for flush_timeout again.
        #[tokio::test]
        async fn test_cancelled_close_rejects_ingest_and_resumes_teardown(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_cancelled_close_rejects_ingest_and_resumes_teardown");

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

            let mut stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema.clone())
                .flush_timeout_ms(100)
                .build_arrow()
                .await?;

            let pending_batch =
                create_test_record_batch(schema.clone(), vec![1], vec![Some("pending")]);
            stream.ingest_batch(pending_batch).await?;

            let (reached, _proceed) = stream.arm_close_finalize_barrier().await;
            let mut close_future = Box::pin(stream.close());
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                tokio::select! {
                    _ = reached.notified() => {}
                    result = &mut close_future => {
                        panic!("close completed before the finalization barrier: {result:?}")
                    }
                }
            })
            .await
            .expect("close must reach the finalization barrier");
            drop(close_future);

            assert!(
                !stream.is_closed(),
                "cancelled teardown is not finalized yet"
            );
            let batch = create_test_record_batch(schema, vec![2], vec![Some("late")]);
            assert!(
                stream.ingest_batch(batch).await.is_err(),
                "Closing must reject new ingests"
            );
            assert!(
                stream.get_unacked_batches().await.is_err(),
                "unacked retrieval is allowed only after Closed"
            );

            let resumed = tokio::time::timeout(std::time::Duration::from_secs(1), stream.close())
                .await
                .expect("resumed close must skip flush and finish promptly")
                .expect_err("resumed close must return the stored flush error");
            assert!(
                resumed.to_string().contains("Flush timed out"),
                "expected stored flush timeout, got: {}",
                resumed
            );
            assert!(stream.is_closed());

            let repeated = stream
                .close()
                .await
                .expect_err("repeated close must return the same stored flush error");
            assert!(
                repeated.to_string().contains("Flush timed out"),
                "expected idempotent flush timeout, got: {}",
                repeated
            );
            assert_eq!(stream.get_unacked_batches().await?.len(), 1);

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

        /// A terminal server error must reach a blocked wait_for_offset()/flush() as the
        /// real error, not a generic close/timeout. process_acks publishes the error while
        /// is_closed is still false (the waiter keeps waiting), so the supervisor
        /// re-publishes it after finalization to wake the waiter with the real error.
        #[tokio::test]
        async fn test_wait_for_offset_returns_real_error_on_terminal_failure(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_wait_for_offset_returns_real_error_on_terminal_failure");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::Error {
                        status: Status::invalid_argument("Permanent failure"),
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
                .recovery(false)
                // Long enough that a regressed waiter would surface the flush deadline
                // instead of the real error, so this assertion catches the regression.
                .flush_timeout_ms(3000)
                .build_arrow()
                .await?;

            let batch = create_test_record_batch(schema, vec![1], vec![Some("test")]);
            let offset = stream.ingest_batch(batch).await?;

            let err = stream
                .wait_for_offset(offset)
                .await
                .expect_err("terminal failure must surface as an error");
            let msg = err.to_string();
            assert!(
                msg.contains("Permanent failure"),
                "waiter must return the real server error, got: {}",
                msg
            );

            Ok(())
        }

        /// Stream-end (server closes without ack -> `Ok(None)`) is a terminal arm that does
        /// not pre-populate the error watch from `process_acks`, unlike a mid-stream error.
        /// A blocked waiter must still get the real stream-end error, not a generic
        /// close/flush deadline.
        #[tokio::test]
        async fn test_wait_for_offset_returns_real_error_on_stream_end(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_wait_for_offset_returns_real_error_on_stream_end");

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
                .recovery(false)
                // Long enough that a regressed waiter would surface the flush deadline
                // instead of the real error, so this assertion catches the regression.
                .flush_timeout_ms(3000)
                .build_arrow()
                .await?;

            let batch = create_test_record_batch(schema, vec![1], vec![Some("test")]);
            let offset = stream.ingest_batch(batch).await?;

            let err = stream
                .wait_for_offset(offset)
                .await
                .expect_err("stream end must surface as an error");
            let msg = err.to_string();
            assert!(
                msg.contains("Server closed the stream"),
                "waiter must return the real stream-end error, got: {}",
                msg
            );

            Ok(())
        }

        /// A non-retryable, non-auth reconnect failure must terminate the stream with the
        /// real error, not a synthetic "Reconnection failed" retried to exhaustion.
        #[tokio::test]
        async fn test_non_auth_non_retryable_reconnect_failure_terminates(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_non_auth_non_retryable_reconnect_failure_terminates");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // conn1: a retriable error triggers recovery. conn2 (reconnect): setup rejected
            // with a non-retryable, non-auth error, which must terminate the stream.
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::Error {
                            status: Status::unavailable("Connection lost"),
                            delay_ms: 0,
                        },
                        MockFlightResponse::FailSetup {
                            status: Status::invalid_argument("Reconnect rejected"),
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
                .recovery_backoff_ms(0)
                // Even with retries available, a non-retryable reconnect failure must
                // terminate immediately rather than retry to exhaustion.
                .recovery_retries(5)
                .flush_timeout_ms(5000)
                .build_arrow()
                .await?;

            let batch = create_test_record_batch(schema, vec![1], vec![Some("a")]);
            let offset = stream.ingest_batch(batch).await?;

            let err = stream
                .wait_for_offset(offset)
                .await
                .expect_err("non-retryable reconnect failure must surface as an error");
            assert!(
                err.to_string().contains("Reconnect rejected"),
                "waiter must surface the real reconnect error, got: {}",
                err
            );
            // The original error's classification is preserved, not flattened into a
            // synthetic retryable "Reconnection failed".
            assert!(
                !err.is_retryable(),
                "the surfaced error must keep its non-retryable classification, got: {}",
                err
            );

            Ok(())
        }

        /// A retryable, non-auth reconnect failure is carried through `pending_error` and
        /// retried until the budget is exhausted, then surfaces the real (retryable) error
        /// rather than a synthetic one.
        #[tokio::test]
        async fn test_retryable_reconnect_failure_exhausted_surfaces_real_error(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_retryable_reconnect_failure_exhausted_surfaces_real_error");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // conn1: retriable error -> recovery. conn2 & conn3: setup rejected with a
            // retryable error. With 2 retries, both reconnect attempts fail and the stream
            // terminates with that error carried through pending_error.
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::Error {
                            status: Status::unavailable("Connection lost"),
                            delay_ms: 0,
                        },
                        MockFlightResponse::FailSetup {
                            status: Status::unavailable("Reconnect unavailable"),
                        },
                        MockFlightResponse::FailSetup {
                            status: Status::unavailable("Reconnect unavailable"),
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
                .recovery_backoff_ms(0)
                .recovery_retries(2)
                .flush_timeout_ms(5000)
                .build_arrow()
                .await?;

            let batch = create_test_record_batch(schema, vec![1], vec![Some("a")]);
            let offset = stream.ingest_batch(batch).await?;

            let err = stream
                .wait_for_offset(offset)
                .await
                .expect_err("exhausted retries must surface an error");
            assert!(
                err.to_string().contains("Reconnect unavailable"),
                "must surface the real reconnect error, got: {}",
                err
            );
            assert!(
                err.is_retryable(),
                "the surfaced error must keep its retryable classification, got: {}",
                err
            );

            Ok(())
        }

        /// A reconnect auth rejection must invalidate cached credentials and retry (so the
        /// next attempt can mint a fresh token) rather than terminating the stream.
        #[tokio::test]
        async fn test_reconnect_auth_rejection_invalidates_and_retries(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_reconnect_auth_rejection_invalidates_and_retries");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // conn1: retriable error -> recovery. conn2: auth-rejected setup -> invalidate +
            // retry. conn3: no scripted response -> setup succeeds -> the stream recovers.
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::Error {
                            status: Status::unavailable("Connection lost"),
                            delay_ms: 0,
                        },
                        MockFlightResponse::FailSetup {
                            status: Status::unauthenticated("Token expired"),
                        },
                    ],
                )
                .await;

            let invalidations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let provider = Arc::new(CountingHeadersProvider {
                invalidations: Arc::clone(&invalidations),
                ..Default::default()
            });

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(provider)
                .arrow(schema.clone())
                .recovery(true)
                .recovery_backoff_ms(0)
                .recovery_retries(5)
                .flush_timeout_ms(5000)
                .build_arrow()
                .await?;

            let batch = create_test_record_batch(schema, vec![1], vec![Some("a")]);
            let offset = stream.ingest_batch(batch).await?;

            // The auth rejection must not terminate the stream: recovery re-mints and the
            // batch is eventually acknowledged.
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                stream.wait_for_offset(offset),
            )
            .await
            .expect("recovery after auth re-mint should complete")?;

            // Exactly one invalidation: the single auth rejection on conn2 (conn1's
            // retriable error and conn3's successful setup must not invalidate).
            assert_eq!(
                invalidations.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "auth rejection must invalidate cached credentials exactly once"
            );

            Ok(())
        }

        /// A custom headers provider whose invalidation never completes must not stall
        /// recovery indefinitely. The invalidation is bounded by recovery_timeout_ms; on
        /// timeout the stream closes and surfaces the original auth rejection.
        #[tokio::test]
        async fn test_reconnect_auth_invalidation_timeout_surfaces_original_error(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_reconnect_auth_invalidation_timeout_surfaces_original_error");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::Error {
                            status: Status::unavailable("Connection lost"),
                            delay_ms: 0,
                        },
                        MockFlightResponse::FailSetup {
                            status: Status::unauthenticated("Token invalidation stalled"),
                        },
                    ],
                )
                .await;

            let invalidations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let cancellations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let provider = Arc::new(HangingInvalidationHeadersProvider {
                invalidations: Arc::clone(&invalidations),
                cancellations: Arc::clone(&cancellations),
            });

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(provider)
                .arrow(schema.clone())
                .recovery(true)
                .recovery_backoff_ms(0)
                .recovery_timeout_ms(1000)
                .recovery_retries(5)
                .flush_timeout_ms(5000)
                .build_arrow()
                .await?;

            let batch = create_test_record_batch(schema, vec![1], vec![Some("a")]);
            let offset = stream.ingest_batch(batch).await?;

            let err = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                stream.wait_for_offset(offset),
            )
            .await
            .expect("stalled invalidation must be bounded")
            .expect_err("invalidation timeout must terminate recovery");
            assert!(
                err.to_string().contains("Token invalidation stalled"),
                "must surface the original auth rejection, got: {}",
                err
            );
            assert!(
                stream.is_closed(),
                "stream must close after invalidation timeout"
            );
            assert_eq!(
                invalidations.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "the stalled invalidation must not be retried after terminal closure"
            );
            assert_eq!(
                cancellations.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "the timed-out invalidation future must be cancelled"
            );

            Ok(())
        }

        /// Terminal auth cleanup runs after the stream closes, but it must still be
        /// bounded so a custom provider cannot leave the supervisor task alive forever.
        #[tokio::test]
        async fn test_terminal_auth_invalidation_is_bounded(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_terminal_auth_invalidation_is_bounded");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::Error {
                        status: Status::unauthenticated("Terminal auth rejection"),
                        delay_ms: 0,
                    }],
                )
                .await;

            let invalidations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let cancellations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let provider = Arc::new(HangingInvalidationHeadersProvider {
                invalidations: Arc::clone(&invalidations),
                cancellations: Arc::clone(&cancellations),
            });

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(provider)
                .arrow(schema.clone())
                .recovery(false)
                .recovery_timeout_ms(1000)
                .flush_timeout_ms(5000)
                .build_arrow()
                .await?;

            let batch = create_test_record_batch(schema, vec![1], vec![Some("a")]);
            let offset = stream.ingest_batch(batch).await?;

            let err = stream
                .wait_for_offset(offset)
                .await
                .expect_err("terminal auth rejection must fail the waiter");
            assert!(
                err.to_string().contains("Terminal auth rejection"),
                "must surface the original terminal auth error, got: {}",
                err
            );

            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while cancellations.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("terminal invalidation must be cancelled at its timeout");
            assert_eq!(
                invalidations.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "terminal auth cleanup should be attempted once"
            );
            assert!(
                stream.is_closed(),
                "terminal auth failure must close the stream"
            );

            Ok(())
        }

        /// If reconnect auth rejections persist past the retry budget, the stream must
        /// terminate with the original auth error rather than a synthetic one.
        #[tokio::test]
        async fn test_reconnect_auth_rejection_exhausted_surfaces_original(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_reconnect_auth_rejection_exhausted_surfaces_original");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // conn1: retriable error -> recovery. conn2 & conn3: auth-rejected setup. With
            // 2 retries, both reconnect attempts fail auth and the stream terminates.
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::Error {
                            status: Status::unavailable("Connection lost"),
                            delay_ms: 0,
                        },
                        MockFlightResponse::FailSetup {
                            status: Status::unauthenticated("Token expired"),
                        },
                        MockFlightResponse::FailSetup {
                            status: Status::unauthenticated("Token expired"),
                        },
                    ],
                )
                .await;

            let invalidations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let provider = Arc::new(CountingHeadersProvider {
                invalidations: Arc::clone(&invalidations),
                ..Default::default()
            });

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(provider)
                .arrow(schema.clone())
                .recovery(true)
                .recovery_backoff_ms(0)
                .recovery_retries(2)
                .flush_timeout_ms(5000)
                .build_arrow()
                .await?;

            let batch = create_test_record_batch(schema, vec![1], vec![Some("a")]);
            let offset = stream.ingest_batch(batch).await?;

            let err = stream
                .wait_for_offset(offset)
                .await
                .expect_err("exhausted auth retries must surface an error");
            assert!(
                err.to_string().contains("Token expired"),
                "must surface the original auth error, got: {}",
                err
            );
            assert!(
                !err.is_retryable(),
                "the surfaced auth error must keep its non-retryable classification, got: {}",
                err
            );
            // Each of the two auth-rejected reconnect attempts invalidates once.
            assert_eq!(
                invalidations.load(std::sync::atomic::Ordering::SeqCst),
                2,
                "each auth-rejected reconnect attempt must invalidate credentials"
            );

            Ok(())
        }

        /// The auth-retry override must not leak: an auth rejection (which invalidates and
        /// retries) followed by a non-auth, non-retryable reconnect failure must terminate
        /// immediately on the second failure rather than being retried.
        #[tokio::test]
        async fn test_reconnect_auth_retry_flag_does_not_leak(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_reconnect_auth_retry_flag_does_not_leak");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // conn1: retriable error -> recovery. conn2: auth rejection (invalidate + retry).
            // conn3: non-auth, non-retryable rejection -> must terminate immediately.
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::Error {
                            status: Status::unavailable("Connection lost"),
                            delay_ms: 0,
                        },
                        MockFlightResponse::FailSetup {
                            status: Status::unauthenticated("Token expired"),
                        },
                        MockFlightResponse::FailSetup {
                            status: Status::invalid_argument("Reconnect rejected"),
                        },
                    ],
                )
                .await;

            let invalidations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let provider = Arc::new(CountingHeadersProvider {
                invalidations: Arc::clone(&invalidations),
                ..Default::default()
            });

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(provider)
                .arrow(schema.clone())
                .recovery(true)
                .recovery_backoff_ms(0)
                // Generous budget: termination must come from the non-retryable
                // classification, not from exhausting retries.
                .recovery_retries(5)
                .flush_timeout_ms(5000)
                .build_arrow()
                .await?;

            let batch = create_test_record_batch(schema, vec![1], vec![Some("a")]);
            let offset = stream.ingest_batch(batch).await?;

            let err = stream
                .wait_for_offset(offset)
                .await
                .expect_err("non-retryable reconnect failure after an auth retry must terminate");
            assert!(
                err.to_string().contains("Reconnect rejected"),
                "must surface the non-auth reconnect error, got: {}",
                err
            );
            assert!(
                !err.is_retryable(),
                "the non-auth error must terminate (retry override did not leak), got: {}",
                err
            );
            // Only the auth rejection invalidated; the non-auth failure did not.
            assert_eq!(
                invalidations.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "only the auth-rejected attempt should invalidate credentials"
            );

            Ok(())
        }

        /// flush() on a closed stream with nothing ingested returns a closed-stream error
        /// rather than panicking or hanging.
        #[tokio::test]
        async fn test_flush_on_closed_empty_stream_errors() -> Result<(), Box<dyn std::error::Error>>
        {
            setup_tracing();
            info!("Starting test_flush_on_closed_empty_stream_errors");

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
                .arrow(schema.clone())
                .build_arrow()
                .await?;

            // Nothing ingested; close, then flush must report the closed stream.
            stream.close().await?;
            let err = stream
                .flush()
                .await
                .expect_err("flush on a closed stream must error");
            assert!(
                err.to_string().contains("closed"),
                "expected a closed-stream error, got: {}",
                err
            );

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

        /// End-to-end: when the server rejects the stream *during setup* with the
        /// structured schema-validation `ErrorInfo`, `build_arrow()` surfaces it
        /// as `ZerobusError::InvalidSchema` carrying the decoded causes — not a
        /// generic `CreateStreamError`. This exercises the full production path
        /// (poll → `FlightError` → `from_setup_status`) that the errors.rs unit
        /// tests can't reach, since they build a `tonic::Status` directly.
        #[tokio::test]
        async fn test_setup_schema_validation_error_surfaces_invalid_schema(
        ) -> Result<(), Box<dyn std::error::Error>> {
            use std::collections::HashMap;

            use databricks_zerobus_ingest_sdk::{SchemaValidationCause, ZerobusError};
            use tonic_types::{ErrorDetails, StatusExt};

            setup_tracing();
            info!("Starting test_setup_schema_validation_error_surfaces_invalid_schema");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // Mirror what Shinkansen attaches on a schema mismatch.
            let mut metadata = HashMap::new();
            metadata.insert("error_code".to_string(), "8001".to_string());
            metadata.insert(
                "causes".to_string(),
                "FIELD_NOT_IN_TABLE,MISSING_REQUIRED_COLUMN".to_string(),
            );
            let status = Status::with_error_details(
                tonic::Code::InvalidArgument,
                "Arrow Flight schema validation failed: ...",
                ErrorDetails::with_error_info(
                    "SCHEMA_VALIDATION_FAILED",
                    "zerobus.databricks.com",
                    metadata,
                ),
            );

            mock_server
                .inject_responses(TABLE_NAME, vec![MockFlightResponse::FailSetup { status }])
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
                .arrow(schema)
                .build_arrow()
                .await;

            match result {
                Err(ZerobusError::InvalidSchema { causes, .. }) => {
                    assert_eq!(
                        causes,
                        vec![
                            SchemaValidationCause::FieldNotInTable,
                            SchemaValidationCause::MissingRequiredColumn,
                        ]
                    );
                }
                Err(other) => panic!("expected InvalidSchema, got {other:?}"),
                Ok(_) => panic!("expected InvalidSchema, but stream setup succeeded"),
            }

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

        /// Idle time before ingestion must not consume a future batch's ACK deadline.
        #[tokio::test]
        async fn test_ack_timeout_starts_when_batch_becomes_pending(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();

            const ACK_TIMEOUT_MS: u64 = 1_000;
            const IDLE_MS: u64 = 800;
            const ACK_DELAY_MS: u64 = 400;
            const PAST_OLD_DEADLINE_MS: u64 = ACK_TIMEOUT_MS - IDLE_MS + 1;

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::BatchAck {
                        ack_up_to_offset: 0,
                        delay_ms: ACK_DELAY_MS,
                        ack_up_to_records: 1,
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
                .arrow(schema.clone())
                .server_lack_of_ack_timeout_ms(ACK_TIMEOUT_MS)
                .flush_timeout_ms(ACK_TIMEOUT_MS * 2)
                .recovery(false)
                .build_arrow()
                .await?;

            let ack_idle = stream.arm_ack_idle_notify().await;
            tokio::time::timeout(std::time::Duration::from_secs(5), ack_idle.notified())
                .await
                .expect("ACK processor should enter its no-pending wait");

            tokio::time::pause();
            tokio::time::advance(std::time::Duration::from_millis(IDLE_MS)).await;

            // Clone before ingestion so the one-shot notification cannot be missed.
            let delayed_ack_armed = mock_server.delayed_ack_armed();
            let batch = create_test_record_batch(schema, vec![2], vec![Some("target")]);
            let offset = stream.ingest_batch(batch).await?;

            // Keep this current-thread runtime runnable while the batch crosses the real
            // loopback socket, preventing paused time from auto-advancing to a timer.
            let delayed_ack_armed = delayed_ack_armed.notified();
            tokio::pin!(delayed_ack_armed);
            let watchdog_started = std::time::Instant::now();
            while futures::poll!(delayed_ack_armed.as_mut()).is_pending() {
                assert!(
                    watchdog_started.elapsed() < std::time::Duration::from_secs(5),
                    "mock server did not arm the delayed ACK"
                );
                tokio::task::yield_now().await;
            }

            // This crosses the old response-relative deadline, but remains well before
            // the target batch's pending-relative deadline.
            tokio::time::advance(std::time::Duration::from_millis(PAST_OLD_DEADLINE_MS)).await;
            tokio::task::yield_now().await;
            assert!(
                !stream.is_closed(),
                "idle time must not consume the target batch's ACK deadline"
            );

            tokio::time::advance(std::time::Duration::from_millis(
                ACK_DELAY_MS - PAST_OLD_DEADLINE_MS,
            ))
            .await;
            tokio::task::yield_now().await;
            stream.wait_for_offset(offset).await?;
            assert!(!stream.is_closed());

            tokio::time::resume();
            stream.close().await?;
            Ok(())
        }

        #[tokio::test]
        async fn test_ack_timeout() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_ack_timeout");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // Delay the ack far past the ack timeout so the client's ack-timeout branch
            // fires (rather than scripting an EOF/close, which is a different terminal arm).
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::BatchAck {
                        ack_up_to_offset: 0,
                        delay_ms: 10_000,
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
                .server_lack_of_ack_timeout_ms(500)
                .flush_timeout_ms(2000)
                .recovery(false)
                .build_arrow()
                .await?;

            let batch = create_test_record_batch(schema, vec![1], vec![Some("test")]);
            let offset = stream.ingest_batch(batch).await?;

            let err = stream
                .wait_for_offset(offset)
                .await
                .expect_err("Expected ack-timeout error");
            assert!(
                err.to_string().contains("Server ack timeout"),
                "expected the ack-timeout error, got: {}",
                err
            );

            Ok(())
        }

        /// A batch accepted after the local request sender closes is buffered but not
        /// submitted, so it has no ACK deadline on the failed connection. The send
        /// failure itself must wake the supervisor and start recovery.
        #[tokio::test]
        async fn test_closed_request_sender_starts_recovery(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::HoldResponseAfterRequestEof],
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
                .arrow(schema.clone())
                .server_lack_of_ack_timeout_ms(30_000)
                .flush_timeout_ms(60_000)
                .recovery(true)
                .recovery_backoff_ms(0)
                .recovery_retries(1)
                .build_arrow()
                .await?;

            let (reconnect_reached, reconnect_proceed) =
                stream.arm_reconnect_rebuild_barrier().await;
            stream.replace_batch_sender_with_closed_channel().await;

            let batch = create_test_record_batch(schema, vec![1], vec![Some("replay")]);
            let offset = stream.ingest_batch(batch).await?;

            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                reconnect_reached.notified(),
            )
            .await
            .expect("request send failure must start recovery without an ACK deadline");
            reconnect_proceed.notify_one();

            stream.wait_for_offset(offset).await?;
            assert_eq!(mock_server.get_batch_count().await, 1);
            stream.close().await?;
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
        async fn test_close_propagates_flush_error() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_close_propagates_flush_error");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // Ack is delayed well past the flush timeout, so the flush performed by
            // close() times out while the stream is still open (no server error, so the
            // idempotent close guard does not short-circuit).
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

            let mut stream = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema.clone())
                .flush_timeout_ms(100)
                .build_arrow()
                .await?;

            let batch = create_test_record_batch(schema, vec![1], vec![Some("unacked")]);
            let _offset = stream.ingest_batch(batch).await?;

            let close_result = stream.close().await;
            assert!(
                close_result.is_err(),
                "close() must propagate the error when its flush fails"
            );

            // The stream is still torn down, and the unacked batch is recoverable.
            assert!(stream.is_closed());
            let unacked = stream.get_unacked_batches().await?;
            assert_eq!(unacked.len(), 1, "Should have 1 unacked batch");

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

    mod backpressure_tests {
        use super::*;

        /// `max_inflight_batches` bounds batches awaiting ACK, not just batches
        /// buffered before the encoder drains. With capacity 1 the second ingest must
        /// block until the first is acked.
        #[tokio::test]
        async fn test_max_inflight_batches_blocks_until_ack(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_max_inflight_batches_blocks_until_ack");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // Ack for offset 0 is delayed; until it arrives the single permit stays held.
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::BatchAck {
                        ack_up_to_offset: 0,
                        delay_ms: 500,
                        ack_up_to_records: 1,
                    }],
                )
                .await;

            let sdk = ZerobusSdk::builder()
                .endpoint(server_url.clone())
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            let stream = Arc::new(
                sdk.stream_builder()
                    .table(TABLE_NAME)
                    .headers_provider(Arc::new(TestHeadersProvider::default()))
                    .arrow(schema.clone())
                    .max_inflight_batches(1)
                    .recovery(false)
                    .build_arrow()
                    .await?,
            );

            // First batch takes the only permit; held until the delayed ack.
            let batch1 = create_test_record_batch(schema.clone(), vec![1], vec![Some("a")]);
            stream.ingest_batch(batch1).await?;

            // Wait until the mock received batch 1 (encoder freed the channel slot), so
            // a later block is attributable to the permit, not channel capacity.
            let mut waited_ms = 0;
            while mock_server.get_batch_count().await < 1 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                waited_ms += 5;
                assert!(waited_ms < 2000, "mock never received the first batch");
            }

            // Second ingest must block on the permit (not just the encoder drain).
            let stream2 = Arc::clone(&stream);
            let schema2 = schema.clone();
            let mut handle = tokio::spawn(async move {
                let batch2 = create_test_record_batch(schema2, vec![2], vec![Some("b")]);
                stream2.ingest_batch(batch2).await
            });

            // Well under the 500ms ack delay it should still be blocked.
            tokio::select! {
                res = &mut handle => {
                    panic!("2nd ingest_batch should block until the 1st is acked, returned: {:?}", res)
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(150)) => {}
            }

            // Once the ack frees the permit, the second ingest completes.
            let joined = tokio::time::timeout(std::time::Duration::from_secs(3), handle)
                .await
                .expect("2nd ingest_batch should unblock after the ack frees a permit")?;
            assert!(
                joined.is_ok(),
                "2nd ingest_batch failed: {:?}",
                joined.err()
            );

            Ok(())
        }

        /// `max_inflight_batches == 0` must be rejected by `build_arrow` (a zero bound
        /// would deadlock every ingest / panic the zero-capacity channel). This is
        /// Arrow-only validation; JSON/proto `build()` is unaffected.
        #[tokio::test]
        async fn test_max_inflight_batches_zero_rejected() -> Result<(), Box<dyn std::error::Error>>
        {
            setup_tracing();
            info!("Starting test_max_inflight_batches_zero_rejected");

            let schema = create_test_arrow_schema();
            let sdk = ZerobusSdk::builder()
                .endpoint("http://127.0.0.1:1")
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;

            // Rejected before any connection attempt.
            let result = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema)
                .max_inflight_batches(0)
                .build_arrow()
                .await;

            assert!(
                matches!(
                    result,
                    Err(databricks_zerobus_ingest_sdk::ZerobusError::InvalidArgument(_))
                ),
                "expected InvalidArgument for max_inflight_batches(0), got {:?}",
                result.err()
            );

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

            // The permanent error already closed the stream, so close() short-circuits
            // to Ok via its idempotent guard; the unacked batch was moved to failed by
            // the error path, not by this close().
            let _ = stream.close().await;

            let unacked = stream.get_unacked_batches().await?;
            assert_eq!(unacked.len(), 1, "Should have 1 unacked batch");

            Ok(())
        }

        /// A partially-acked (auto-chunked) batch must be returned as only its un-acked
        /// suffix on terminal failure, not the whole batch — otherwise manual retry
        /// re-sends the already-durable prefix.
        #[tokio::test]
        async fn test_get_unacked_batches_slices_partially_acked(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_get_unacked_batches_slices_partially_acked");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // Batch A (offset 0, 3 rows): only 1 record acked. Batch B (offset 1)
            // arrival triggers a permanent error, closing the stream (recovery off).
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

            let batch_a = create_test_record_batch(
                schema.clone(),
                vec![1, 2, 3],
                vec![Some("a"), Some("b"), Some("c")],
            );
            stream.ingest_batch(batch_a).await?;
            let batch_b = create_test_record_batch(schema.clone(), vec![4], vec![Some("d")]);
            let offset_b = stream.ingest_batch(batch_b).await?;

            // Wait until the terminal error closes the stream, then retrieve unacked.
            let _ = stream.wait_for_offset(offset_b).await;
            let _ = stream.close().await;

            let unacked = stream.get_unacked_batches().await?;
            // Batch A ([1,2,3]) had 1 record acked -> un-acked suffix is ids [2, 3].
            // Batch B is fully un-acked -> id [4].
            assert_eq!(unacked.len(), 2, "expected sliced A suffix + full B");
            assert_eq!(
                batch_ids(&unacked[0]),
                vec![2, 3],
                "batch A must be sliced to its un-acked suffix (ids [2, 3])"
            );
            assert_eq!(
                batch_ids(&unacked[1]),
                vec![4],
                "batch B must be fully retained"
            );

            Ok(())
        }

        /// Retrieval must be idempotent: because `get_unacked_batches` drains still-pending
        /// batches into the failed set, calling it repeatedly must return the same sliced
        /// snapshot rather than dropping batches on the second call.
        #[tokio::test]
        async fn test_get_unacked_batches_idempotent() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_get_unacked_batches_idempotent");

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

            let batch_a = create_test_record_batch(
                schema.clone(),
                vec![1, 2, 3],
                vec![Some("a"), Some("b"), Some("c")],
            );
            stream.ingest_batch(batch_a).await?;
            let batch_b = create_test_record_batch(schema.clone(), vec![4], vec![Some("d")]);
            let offset_b = stream.ingest_batch(batch_b).await?;
            let _ = stream.wait_for_offset(offset_b).await;
            let _ = stream.close().await;

            let first: Vec<Vec<i64>> = stream
                .get_unacked_batches()
                .await?
                .iter()
                .map(batch_ids)
                .collect();
            let second: Vec<Vec<i64>> = stream
                .get_unacked_batches()
                .await?
                .iter()
                .map(batch_ids)
                .collect();

            assert_eq!(
                first,
                vec![vec![2, 3], vec![4]],
                "first snapshot should be sliced A suffix (ids [2,3]) + full B (id [4])"
            );
            assert_eq!(
                first, second,
                "repeated get_unacked_batches must be idempotent"
            );

            Ok(())
        }
    }

    mod recovery_tests {
        use super::*;

        /// The supervisor must surface a non-retryable configuration error if a timeout
        /// that was representable at construction can no longer form a runtime deadline.
        #[tokio::test(start_paused = true)]
        async fn test_unrepresentable_runtime_recovery_deadline_is_rejected(
        ) -> Result<(), Box<dyn std::error::Error>> {
            const RECOVERY_TIMEOUT_MS: u64 = 32 * 365 * 24 * 60 * 60 * 1_000;

            setup_tracing();

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::Error {
                        status: tonic::Status::unavailable("Connection lost"),
                        delay_ms: 0,
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
                .recovery(true)
                .recovery_backoff_ms(0)
                .recovery_timeout_ms(RECOVERY_TIMEOUT_MS)
                .recovery_retries(1)
                .build_arrow()
                .await?;

            advance_tokio_time_near_instant_limit().await;

            let batch = create_test_record_batch(schema, vec![1], vec![Some("replay")]);
            stream.ingest_batch(batch).await?;
            let watchdog_started = std::time::Instant::now();
            while !stream.is_closed() {
                assert!(
                    watchdog_started.elapsed() < std::time::Duration::from_secs(5),
                    "near-limit recovery did not terminate"
                );
                tokio::task::yield_now().await;
            }
            match stream.flush().await {
                Err(ZerobusError::InvalidArgument(message)) => {
                    assert!(message.contains("recovery_timeout_ms"));
                }
                Err(error) => panic!("unexpected recovery error: {error}"),
                Ok(()) => panic!("unrepresentable recovery deadline was accepted"),
            }
            assert_eq!(stream.get_unacked_batches().await?.len(), 1);
            Ok(())
        }

        /// close() during the reconnect rebuild window must slice with the pre-reconnect
        /// watermark. A barrier parks reconnect after the new connection is established but
        /// before pending ranges/watermark are rebuilt; close() then reaps the parked
        /// supervisor and drains with the pre-reconnect watermark/ranges, so A is sliced to
        /// its un-acked suffix (ids [2, 3]) and B is retained whole.
        #[tokio::test]
        async fn test_close_during_reconnect_rebuild_window_slices_correctly(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_close_during_reconnect_rebuild_window_slices_correctly");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // Partially ack A (1 of 3 records), then a retriable error on B triggers the
            // reconnect we park at the rebuild barrier.
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
                            status: tonic::Status::unavailable("Connection lost"),
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
                .recovery(true)
                .recovery_backoff_ms(0)
                .recovery_retries(5)
                // Short flush timeout so close()'s flush returns quickly while parked.
                .flush_timeout_ms(200)
                .build_arrow()
                .await?;

            // Arm two seams: one to confirm A's partial ack has been applied, one to park
            // the next reconnect right before it rebuilds pending ranges.
            let ack_applied = stream.arm_ack_applied_notify().await;
            let (reached, _proceed) = stream.arm_reconnect_rebuild_barrier().await;

            let batch_a = create_test_record_batch(
                schema.clone(),
                vec![1, 2, 3],
                vec![Some("a"), Some("b"), Some("c")],
            );
            stream.ingest_batch(batch_a).await?;

            // Wait until A's partial ack (1 of 3 records) is applied, so
            // last_acked_records == 1 before the error triggers reconnect. Otherwise the
            // ack and error can arrive back-to-back and the drain would see watermark 0.
            tokio::time::timeout(std::time::Duration::from_secs(5), ack_applied.notified())
                .await
                .expect("A's partial ack should be applied");

            let batch_b = create_test_record_batch(schema.clone(), vec![4], vec![Some("d")]);
            stream.ingest_batch(batch_b).await?;

            // Wait until reconnect is parked at the rebuild barrier: connection is up, but
            // ranges/watermark are not yet rebuilt (watermark still the pre-reconnect 1).
            tokio::time::timeout(std::time::Duration::from_secs(5), reached.notified())
                .await
                .expect("reconnect should reach the rebuild barrier");

            // close() reaps the parked supervisor and drains with the pre-rebuild state.
            let _ = stream.close().await;

            let unacked = stream.get_unacked_batches().await?;
            assert_eq!(unacked.len(), 2, "expected sliced A suffix + full B");
            assert_eq!(
                batch_ids(&unacked[0]),
                vec![2, 3],
                "A must be sliced with the pre-reconnect watermark (ids [2, 3])"
            );
            assert_eq!(batch_ids(&unacked[1]), vec![4], "B fully retained");

            Ok(())
        }

        /// Regression test for the mock contract: `ack_up_to_records` must be
        /// connection-relative. A retriable error forces a second DoPut connection
        /// for the same table; the auto-ack on that connection must count only the
        /// rows replayed on it, NOT the cumulative rows across both connections.
        /// Under the old global row counter the auto-ack would be 6 (3 + 3) and this
        /// test would fail, masking slicing/replay bugs.
        #[tokio::test]
        async fn test_auto_ack_is_connection_relative() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_auto_ack_is_connection_relative");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // First connection errors (retriable) after the batch is decoded; the
            // batch stays unacked and is replayed on the recovered connection, where
            // responses are exhausted so it hits the auto-ack path.
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::Error {
                        status: tonic::Status::unavailable("Temporary network issue"),
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
                .recovery(true)
                .recovery_timeout_ms(5000)
                .recovery_backoff_ms(100)
                .recovery_retries(3)
                .build_arrow()
                .await?;

            // One 3-row batch. It is decoded on connection 1 (errors), then replayed
            // and decoded again on connection 2 (auto-acked).
            let batch = create_test_record_batch(
                schema.clone(),
                vec![1, 2, 3],
                vec![Some("a"), Some("b"), Some("c")],
            );
            let offset = stream.ingest_batch(batch).await?;
            stream.wait_for_offset(offset).await?;

            // Both connections decoded the 3-row batch, so the GLOBAL observation is 6.
            assert_eq!(
                mock_server.get_total_records_received().await,
                6,
                "both connections should have decoded the batch (global observation)"
            );

            // But every auto-ack must be connection-relative: exactly the 3 rows
            // replayed on the recovered connection, never the cumulative 6.
            let acks = mock_server.get_auto_ack_records().await;
            assert!(
                !acks.is_empty(),
                "expected an auto-ack on the recovered connection"
            );
            assert!(
                acks.iter().all(|&r| r == 3),
                "auto-ack must exclude rows from the first connection (expected all == 3), got {:?}",
                acks
            );

            Ok(())
        }

        /// A record ingested during the reconnect rebuild window must be replayed, not
        /// silently dropped. A partial ack (1 of 3 records) lands, a retriable error
        /// triggers recovery, and a barrier parks reconnect after the new connection is up
        /// but before it takes ingest_mutex and rebuilds ranges. While parked
        /// (is_paused == true) a record is ingested and buffered; after the barrier is
        /// released it must be replayed and acknowledged on the recovered connection.
        #[tokio::test]
        async fn test_ingest_during_reconnect_window_is_not_dropped(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_ingest_during_reconnect_window_is_not_dropped");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // Connection 1: partial-ack A (1 of 3 records) so last_acked_records == 1, then
            // a retriable error on B triggers the reconnect we park at the barrier. B (a
            // second batch) makes the error fire promptly on connection 1 — relying on A
            // alone would instead reconnect via the slow ack timeout and let the scripted
            // error fire on a later connection.
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
                            status: tonic::Status::unavailable("Connection lost"),
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
                .arrow(schema.clone())
                .recovery(true)
                .recovery_backoff_ms(0)
                .recovery_retries(5)
                .build_arrow()
                .await?;

            let ack_applied = stream.arm_ack_applied_notify().await;
            let (reached, proceed) = stream.arm_reconnect_rebuild_barrier().await;

            let batch_a = create_test_record_batch(
                schema.clone(),
                vec![1, 2, 3],
                vec![Some("a"), Some("b"), Some("c")],
            );
            stream.ingest_batch(batch_a).await?;

            // Wait until A's partial ack (1 of 3) is applied (last_acked_records == 1),
            // otherwise the ack and error can arrive back-to-back and the watermark is 0.
            tokio::time::timeout(std::time::Duration::from_secs(5), ack_applied.notified())
                .await
                .expect("A's partial ack should be applied");

            // B triggers the retriable error on connection 1, kicking off recovery.
            let batch_b = create_test_record_batch(schema.clone(), vec![50], vec![Some("y")]);
            stream.ingest_batch(batch_b).await?;

            // Wait until reconnect is parked: connection up, ingest_mutex free, counters
            // and ranges not yet rebuilt.
            tokio::time::timeout(std::time::Duration::from_secs(5), reached.notified())
                .await
                .expect("reconnect should reach the rebuild barrier");

            // Ingest while parked. is_paused is set, so the record buffers (Ok) after
            // computing its range against the still-pre-reconnect counter.
            let batch_c = create_test_record_batch(schema.clone(), vec![99], vec![Some("z")]);
            let c_offset = stream.ingest_batch(batch_c).await?;

            // Release reconnect: it takes ingest_mutex and rebuilds/replays, rebasing the
            // buffered record consistently and sending it on the recovered connection.
            proceed.notify_one();

            // The buffered record must be replayed and acknowledged, not silently dropped.
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                stream.wait_for_offset(c_offset),
            )
            .await
            .expect("record ingested during the reconnect window must not be dropped")?;

            // Global observation: conn1 decodes A (3) + B (1) = 4; the recovered connection
            // replays A's un-acked suffix (2) + B (1) + C (1) = 4, so 8 total.
            assert_eq!(
                mock_server.get_total_records_received().await,
                8,
                "recovered connection must replay A's un-acked suffix, B, and the windowed record"
            );

            Ok(())
        }

        /// close() while a reconnect is parked at the rebuild barrier must tear the stream
        /// down and move pending batches to the failed set, without panicking or hanging
        /// beyond the flush timeout.
        #[tokio::test]
        async fn test_close_during_reconnect_window_moves_pending_to_failed(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_close_during_reconnect_window_moves_pending_to_failed");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            // A retriable error triggers the reconnect we park at the rebuild barrier.
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::Error {
                        status: tonic::Status::unavailable("Connection lost"),
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
                .recovery(true)
                .recovery_backoff_ms(0)
                .recovery_retries(5)
                .flush_timeout_ms(200)
                .build_arrow()
                .await?;

            // Park the reconnect; do not release it — close() reaps it instead.
            let (reached, _proceed) = stream.arm_reconnect_rebuild_barrier().await;

            let batch = create_test_record_batch(schema.clone(), vec![1], vec![Some("a")]);
            stream.ingest_batch(batch).await?;

            tokio::time::timeout(std::time::Duration::from_secs(5), reached.notified())
                .await
                .expect("reconnect should reach the rebuild barrier");

            // close() must return (its flush times out at 200ms) without hanging or
            // panicking, reaping the parked supervisor along the way.
            let close_result =
                tokio::time::timeout(std::time::Duration::from_secs(5), stream.close())
                    .await
                    .expect("close() must not hang while a reconnect is parked");
            assert!(
                close_result.is_err(),
                "close() should surface the flush timeout"
            );

            // The un-acked batch was moved to the failed set and is retrievable.
            let unacked = stream.get_unacked_batches().await?;
            assert_eq!(
                unacked.len(),
                1,
                "pending batch must be moved to the failed set"
            );

            Ok(())
        }

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

        /// End-to-end: when the table schema changes mid-stream, the *reconnect*
        /// is rejected with the structured schema-validation `ErrorInfo`. Because
        /// `InvalidSchema` is non-retriable (the SDK holds a fixed schema and
        /// would just re-send the rejected one), the supervisor must surface it to
        /// a blocked `wait_for_offset` immediately rather than burning the recovery
        /// budget and reporting a generic failure. This lets callers (e.g. Vector)
        /// re-resolve their schema and rebuild the stream for zero-downtime.
        ///
        /// Sequence on the one table: ack batch 0 → drop the connection (retriable,
        /// triggers recovery) → reject the reconnect with the schema `ErrorInfo`.
        #[tokio::test]
        async fn test_reconnect_schema_validation_error_surfaces_invalid_schema(
        ) -> Result<(), Box<dyn std::error::Error>> {
            use std::collections::HashMap;

            use databricks_zerobus_ingest_sdk::{SchemaValidationCause, ZerobusError};
            use tonic_types::{ErrorDetails, StatusExt};

            setup_tracing();
            info!("Starting test_reconnect_schema_validation_error_surfaces_invalid_schema");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            let mut metadata = HashMap::new();
            metadata.insert("error_code".to_string(), "8001".to_string());
            metadata.insert("causes".to_string(), "MISSING_REQUIRED_COLUMN".to_string());
            let schema_status = tonic::Status::with_error_details(
                tonic::Code::InvalidArgument,
                "Arrow Flight schema validation failed: ...",
                ErrorDetails::with_error_info(
                    "SCHEMA_VALIDATION_FAILED",
                    "zerobus.databricks.com",
                    metadata,
                ),
            );

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        // First connection: ack batch 0, then drop the connection
                        // (retriable) to trigger recovery.
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 1,
                        },
                        MockFlightResponse::CloseStream { delay_ms: 0 },
                        // Second connection (the reconnect): reject during setup
                        // with the schema-validation ErrorInfo.
                        MockFlightResponse::FailSetup {
                            status: schema_status,
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
                .recovery_backoff_ms(50)
                .recovery_retries(3)
                .build_arrow()
                .await?;

            // Batch 0 is acked on the first connection.
            let batch1 = create_test_record_batch(schema.clone(), vec![1], vec![Some("first")]);
            let offset1 = stream.ingest_batch(batch1).await?;
            stream.wait_for_offset(offset1).await?;

            // Batch 1 is in flight when the connection drops; recovery reconnects
            // and is rejected with the schema error.
            let batch2 = create_test_record_batch(schema.clone(), vec![2], vec![Some("second")]);
            let offset2 = stream.ingest_batch(batch2).await?;

            match stream.wait_for_offset(offset2).await {
                Err(ZerobusError::InvalidSchema { causes, .. }) => {
                    assert_eq!(causes, vec![SchemaValidationCause::MissingRequiredColumn]);
                }
                other => panic!("expected Err(InvalidSchema) after reconnect, got {other:?}"),
            }

            Ok(())
        }

        /// Regression: a `flush()` that starts *after* the schema-reconnect has
        /// already closed the stream must still observe the typed `InvalidSchema`
        /// (with its causes), not a generic "stream is closed" error. The sibling
        /// test above covers a waiter already parked in the supervisor's
        /// `select!` before close; this one covers the late-caller path through
        /// the terminal-error snapshot, proving the typed error survives the
        /// close path (the whole point of the schema-error work).
        #[tokio::test]
        async fn test_flush_after_schema_reconnect_close_surfaces_invalid_schema(
        ) -> Result<(), Box<dyn std::error::Error>> {
            use std::collections::HashMap;
            use std::time::Duration;

            use databricks_zerobus_ingest_sdk::{SchemaValidationCause, ZerobusError};
            use tonic_types::{ErrorDetails, StatusExt};

            setup_tracing();
            info!("Starting test_flush_after_schema_reconnect_close_surfaces_invalid_schema");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();

            let mut metadata = HashMap::new();
            metadata.insert("error_code".to_string(), "8001".to_string());
            metadata.insert(
                "causes".to_string(),
                "FIELD_NOT_IN_TABLE,MISSING_REQUIRED_COLUMN".to_string(),
            );
            let schema_status = tonic::Status::with_error_details(
                tonic::Code::InvalidArgument,
                "Arrow Flight schema validation failed: ...",
                ErrorDetails::with_error_info(
                    "SCHEMA_VALIDATION_FAILED",
                    "zerobus.databricks.com",
                    metadata,
                ),
            );

            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        // First connection: ack batch 0, then drop the connection
                        // (retriable) to trigger recovery.
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 1,
                        },
                        MockFlightResponse::CloseStream { delay_ms: 0 },
                        // The reconnect is rejected during setup with the schema error,
                        // which terminates the stream (InvalidSchema is non-retriable).
                        MockFlightResponse::FailSetup {
                            status: schema_status,
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
                .recovery_backoff_ms(50)
                .recovery_retries(3)
                .build_arrow()
                .await?;

            let batch1 = create_test_record_batch(schema.clone(), vec![1], vec![Some("first")]);
            let offset1 = stream.ingest_batch(batch1).await?;
            stream.wait_for_offset(offset1).await?;

            // Batch 1 is in flight when the connection drops; the reconnect is
            // rejected and the supervisor closes the stream.
            let batch2 = create_test_record_batch(schema.clone(), vec![2], vec![Some("second")]);
            let _offset2 = stream.ingest_batch(batch2).await?;

            // Wait until the supervisor has closed the stream, so flush() below is
            // a *late* caller reading the terminal snapshot — not a waiter parked
            // before close.
            let mut waited = Duration::ZERO;
            while !stream.is_closed() && waited < Duration::from_secs(5) {
                tokio::time::sleep(Duration::from_millis(20)).await;
                waited += Duration::from_millis(20);
            }
            assert!(
                stream.is_closed(),
                "stream should be closed by the schema reconnect failure"
            );

            // A flush() starting after close must still surface the typed error.
            match stream.flush().await {
                Err(ZerobusError::InvalidSchema { causes, .. }) => {
                    assert_eq!(
                        causes,
                        vec![
                            SchemaValidationCause::FieldNotInTable,
                            SchemaValidationCause::MissingRequiredColumn,
                        ]
                    );
                }
                other => panic!("expected Err(InvalidSchema) from post-close flush, got {other:?}"),
            }

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
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 1,
                            delay_ms: 0,
                            ack_up_to_records: 4,
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

            // A third batch advances the mock script to the disconnect after the
            // partial ACK for batch2. Recovery must replay only batch2's final three
            // rows, followed by this batch.
            let batch3 = create_test_record_batch(schema, vec![16], vec![Some("p")]);
            let offset3 = stream.ingest_batch(batch3).await?;

            let result = stream.wait_for_offset(offset2).await;
            assert!(
                result.is_ok(),
                "Expected partial batch recovery to succeed: {:?}",
                result
            );
            stream.wait_for_offset(offset3).await?;
            assert_eq!(
                mock_server.get_total_records_received().await,
                20,
                "recovery must replay only the three-record unacknowledged suffix"
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
            // On the new connection offsets and record counts restart from 0. Ack both
            // replayed rows after the second batch arrives.
            responses.push(MockFlightResponse::BatchAck {
                ack_up_to_offset: 1,
                delay_ms: 0,
                ack_up_to_records: 2,
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
        use arrow_array::RecordBatch;
        use arrow_schema::Schema as ArrowSchema;
        use databricks_zerobus_ingest_sdk::ZerobusArrowStream;
        use std::time::{Duration, Instant};

        async fn build_rotation_stream(
            server_url: String,
            schema: Arc<ArrowSchema>,
            ack_wait_ms: Option<u64>,
        ) -> Result<ZerobusArrowStream, Box<dyn std::error::Error>> {
            let sdk = ZerobusSdk::builder()
                .endpoint(server_url)
                .unity_catalog_url("https://mock-uc.com")
                .tls_config(Arc::new(NoTlsConfig))
                .build()?;
            Ok(sdk
                .stream_builder()
                .table(TABLE_NAME)
                .headers_provider(Arc::new(TestHeadersProvider::default()))
                .arrow(schema)
                .recovery(true)
                .recovery_backoff_ms(10)
                .recovery_retries(3)
                .flush_timeout_ms(3_000)
                .stream_paused_max_wait_time_ms(ack_wait_ms)
                .build_arrow()
                .await?)
        }

        fn one_batch(schema: Arc<ArrowSchema>) -> RecordBatch {
            create_test_record_batch(schema, vec![1, 2, 3], vec![Some("a"), Some("b"), Some("c")])
        }

        #[tokio::test]
        async fn test_server_rotation_half_closes_before_reconnect(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_server_rotation_half_closes_before_reconnect (arrow)");

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

            // Batch 2 triggers rotation. It is replayed only after the active request
            // reaches EOF and the server response is drained.
            let batch2 = create_test_record_batch(
                schema.clone(),
                vec![7, 8, 9],
                vec![Some("g"), Some("h"), Some("i")],
            );
            let offset2 = stream.ingest_batch(batch2).await?;

            stream.wait_for_offset(offset2).await?;
            assert_eq!(mock_server.get_request_half_close_count(), 1);
            assert_eq!(mock_server.get_request_reset_count(), 0);

            stream.close().await?;
            Ok(())
        }

        #[tokio::test]
        async fn test_server_rotation_without_recovery_still_half_closes(
        ) -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_server_rotation_without_recovery_still_half_closes (arrow)");

            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![MockFlightResponse::GracefulClose {
                        duration_ms: 1_000,
                        delay_ms: 0,
                        ack_up_to_offset: None,
                        ack_up_to_records: None,
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
                .recovery(false)
                .stream_paused_max_wait_time_ms(Some(0))
                .build_arrow()
                .await?;

            let offset = stream.ingest_batch(one_batch(schema)).await?;
            let error = stream
                .wait_for_offset(offset)
                .await
                .expect_err("rotation must terminate when recovery is disabled");
            match error {
                ZerobusError::StreamClosedError(status) => {
                    assert_eq!(status.code(), tonic::Code::Unavailable);
                    assert_eq!(
                        status.message(),
                        "Server requested graceful stream rotation"
                    );
                }
                other => panic!("expected rotation error, got {other:?}"),
            }

            tokio::time::timeout(Duration::from_secs(2), async {
                while !stream.is_closed() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("rotation must finalize the stream without reconnecting");

            assert_eq!(mock_server.get_batch_count().await, 1, "must not reconnect");
            assert_eq!(mock_server.get_request_half_close_count(), 1);
            assert_eq!(mock_server.get_request_reset_count(), 0);
            let unacked = stream.get_unacked_batches().await?;
            assert_eq!(unacked.len(), 1);
            assert_eq!(unacked[0].num_rows(), 3);
            Ok(())
        }

        #[tokio::test]
        async fn test_zero_ack_wait_still_half_closes() -> Result<(), Box<dyn std::error::Error>> {
            setup_tracing();
            info!("Starting test_zero_ack_wait_still_half_closes (arrow)");

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
                        // After bounded transport cleanup and recovery, ack batch 1.
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

            // A zero configured wait skips only the ACK wait. The request must still
            // half-close and the response receives a bounded drain opportunity.
            let batch1 = create_test_record_batch(
                schema.clone(),
                vec![4, 5, 6],
                vec![Some("d"), Some("e"), Some("f")],
            );
            let start = Instant::now();
            let offset1 = stream.ingest_batch(batch1).await?;

            stream.wait_for_offset(offset1).await?;
            let elapsed = start.elapsed();

            // Cleanup should be bounded well below the advertised 10-second grace.
            assert!(
                elapsed.as_millis() < 2000,
                "Expected bounded transport cleanup, but waited {}ms",
                elapsed.as_millis()
            );
            assert_eq!(mock_server.get_request_half_close_count(), 1);
            assert_eq!(mock_server.get_request_reset_count(), 0);

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
            assert_eq!(mock_server.get_request_half_close_count(), 1);
            assert_eq!(mock_server.get_request_reset_count(), 0);

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
            tokio::time::timeout(Duration::from_secs(2), async {
                while mock_server.get_request_half_close_count() == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the active request should reach EOF after the close-signal ACK");
            assert_eq!(mock_server.get_request_half_close_count(), 1);
            assert_eq!(mock_server.get_request_reset_count(), 0);

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
                        // Acknowledge batch 0 with the close signal. Waiting for offset 0
                        // then proves the pause and submitted-record snapshot are active.
                        MockFlightResponse::GracefulClose {
                            duration_ms: 5_000,
                            delay_ms: 0,
                            ack_up_to_offset: Some(0),
                            ack_up_to_records: Some(3),
                        },
                        // Keep the active response open long enough to ingest while paused.
                        MockFlightResponse::HoldResponseAfterRequestEof,
                        // Only batch 1 is pending on the replacement connection.
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
            assert_eq!(
                mock_server.get_batch_count().await,
                2,
                "the post-signal batch must be sent only on the replacement connection"
            );
            assert_eq!(mock_server.get_request_half_close_count(), 1);
            assert_eq!(mock_server.get_request_reset_count(), 0);

            stream.close().await?;
            Ok(())
        }

        #[tokio::test]
        async fn test_post_eof_ack_is_applied_before_replay(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::GracefulClose {
                            duration_ms: 1_000,
                            delay_ms: 0,
                            ack_up_to_offset: None,
                            ack_up_to_records: None,
                        },
                        MockFlightResponse::BatchAckAfterRequestEof {
                            ack_up_to_offset: 0,
                            ack_up_to_records: 3,
                        },
                    ],
                )
                .await;

            let mut stream = build_rotation_stream(server_url, schema.clone(), Some(0)).await?;
            let offset = stream.ingest_batch(one_batch(schema)).await?;
            stream.wait_for_offset(offset).await?;

            assert_eq!(
                mock_server.get_batch_count().await,
                1,
                "late ACK avoids replay"
            );
            assert_eq!(mock_server.get_request_half_close_count(), 1);
            assert_eq!(mock_server.get_request_reset_count(), 0);
            stream.close().await?;
            Ok(())
        }

        #[tokio::test]
        async fn test_post_eof_permanent_error_is_preserved(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::GracefulClose {
                            duration_ms: 1_000,
                            delay_ms: 0,
                            ack_up_to_offset: None,
                            ack_up_to_records: None,
                        },
                        MockFlightResponse::ErrorAfterRequestEof {
                            status: tonic::Status::invalid_argument("post-EOF rejection"),
                        },
                    ],
                )
                .await;

            let stream = build_rotation_stream(server_url, schema.clone(), Some(0)).await?;
            let offset = stream.ingest_batch(one_batch(schema)).await?;
            let error = stream
                .wait_for_offset(offset)
                .await
                .expect_err("peer error must win");
            match error {
                ZerobusError::StreamClosedError(status) => {
                    assert_eq!(status.code(), tonic::Code::InvalidArgument);
                    assert_eq!(status.message(), "post-EOF rejection");
                }
                other => panic!("expected permanent peer status, got {other:?}"),
            }
            assert_eq!(mock_server.get_request_half_close_count(), 1);
            assert_eq!(mock_server.get_request_reset_count(), 0);
            Ok(())
        }

        #[tokio::test]
        async fn test_rotation_drain_timeout_falls_back_to_reconnect(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::GracefulClose {
                            duration_ms: 1_000,
                            delay_ms: 0,
                            ack_up_to_offset: None,
                            ack_up_to_records: None,
                        },
                        MockFlightResponse::HoldResponseAfterRequestEof,
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 3,
                        },
                    ],
                )
                .await;

            let mut stream = build_rotation_stream(server_url, schema.clone(), Some(0)).await?;
            let started = Instant::now();
            let offset = stream.ingest_batch(one_batch(schema)).await?;
            stream.wait_for_offset(offset).await?;

            assert!(started.elapsed() >= Duration::from_millis(450));
            assert_eq!(mock_server.get_request_half_close_count(), 1);
            assert_eq!(mock_server.get_request_reset_count(), 0);
            stream.close().await?;
            Ok(())
        }

        #[tokio::test]
        async fn test_invalid_close_ack_still_half_closes_request(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::GracefulClose {
                            duration_ms: 1_000,
                            delay_ms: 0,
                            ack_up_to_offset: Some(0),
                            ack_up_to_records: Some(4),
                        },
                        MockFlightResponse::ErrorAfterRequestEof {
                            status: tonic::Status::unavailable("rotation grace expired"),
                        },
                    ],
                )
                .await;

            let stream = build_rotation_stream(server_url, schema.clone(), Some(0)).await?;
            let offset = stream.ingest_batch(one_batch(schema)).await?;
            let error = stream
                .wait_for_offset(offset)
                .await
                .expect_err("invalid ACK must fail");
            assert!(matches!(error, ZerobusError::InvalidStateError(_)));
            assert_eq!(
                mock_server.get_batch_count().await,
                1,
                "a retryable post-EOF status must not trigger replay after a permanent error"
            );
            assert_eq!(mock_server.get_request_half_close_count(), 1);
            assert_eq!(mock_server.get_request_reset_count(), 0);
            Ok(())
        }

        #[tokio::test]
        async fn test_zero_server_grace_uses_bounded_local_drain(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let (mock_server, server_url) = start_mock_flight_server().await?;
            let schema = create_test_arrow_schema();
            mock_server
                .inject_responses(
                    TABLE_NAME,
                    vec![
                        MockFlightResponse::GracefulClose {
                            duration_ms: 0,
                            delay_ms: 0,
                            ack_up_to_offset: None,
                            ack_up_to_records: None,
                        },
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset: 0,
                            delay_ms: 0,
                            ack_up_to_records: 3,
                        },
                    ],
                )
                .await;

            let mut stream = build_rotation_stream(server_url, schema.clone(), None).await?;
            let offset = stream.ingest_batch(one_batch(schema)).await?;
            stream.wait_for_offset(offset).await?;
            assert_eq!(mock_server.get_request_half_close_count(), 1);
            assert_eq!(mock_server.get_request_reset_count(), 0);
            stream.close().await?;
            Ok(())
        }
    }
}
