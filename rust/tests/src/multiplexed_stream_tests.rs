mod mock_grpc;
mod utils;

use std::sync::Arc;

use databricks_zerobus_ingest_sdk::{
    MessageId, MultiplexedStream, NoTlsConfig, ZerobusError, ZerobusSdk, ZerobusStream,
};
use mock_grpc::{start_mock_server, MockResponse};
use tracing::info;
use utils::{create_test_descriptor_proto, setup_tracing, TestHeadersProvider};

#[derive(Clone)]
struct TestOpts {
    max_inflight_requests: usize,
    flush_timeout_ms: Option<u64>,
}

fn default_options() -> TestOpts {
    TestOpts {
        max_inflight_requests: 100,
        flush_timeout_ms: None,
    }
}

/// Helper: create an SDK pointed at a mock server.
async fn create_test_sdk(server_url: &str) -> Result<ZerobusSdk, Box<dyn std::error::Error>> {
    Ok(ZerobusSdk::builder()
        .endpoint(server_url)
        .unity_catalog_url("https://mock-uc.com")
        .tls_config(Arc::new(NoTlsConfig))
        .build()?)
}

/// Helper: create a ZerobusStream from the SDK.
async fn create_test_stream(
    sdk: &ZerobusSdk,
    table_name: &str,
    opts: TestOpts,
) -> Result<ZerobusStream, ZerobusError> {
    let mut builder = sdk
        .stream_builder()
        .table(table_name)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .compiled_proto(create_test_descriptor_proto().unwrap())
        .max_inflight_requests(opts.max_inflight_requests)
        .recovery(false);
    if let Some(ms) = opts.flush_timeout_ms {
        builder = builder.flush_timeout_ms(ms);
    }
    builder.build().await
}

mod construction_tests {
    use super::*;

    #[test]
    #[should_panic(expected = "MultiplexedStream requires at least one sub-stream")]
    fn test_empty_streams_panics() {
        MultiplexedStream::new(vec![]);
    }
}

mod single_stream_tests {
    use super::*;

    const TABLE: &str = "single.schema.table";

    #[tokio::test]
    async fn test_ingest_single_record() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_single_record");

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "s1".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let stream = create_test_stream(&sdk, TABLE, default_options()).await?;
        let mux = MultiplexedStream::new(vec![stream]);

        let msg_id = mux.ingest_record(b"hello".to_vec()).await?;
        assert_eq!(msg_id.stream_index(), 0);
        assert_eq!(msg_id.sub_offset(), 0);

        mux.wait_for_message_id(msg_id).await?;
        assert_eq!(mock_server.get_write_count().await, 1);
        assert!(!mux.is_closed());

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_multiple_records() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_multiple_records");

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "s1".to_string(),
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
                    MockResponse::RecordAck {
                        ack_up_to_offset: 3,
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 4,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let stream = create_test_stream(&sdk, TABLE, default_options()).await?;
        let mux = MultiplexedStream::new(vec![stream]);

        let mut msg_ids = Vec::new();
        for _ in 0..5 {
            let msg_id = mux.ingest_record(b"data".to_vec()).await?;
            msg_ids.push(msg_id);
        }

        // All go to stream 0 (single stream), sub-offsets should be sequential
        for (i, msg_id) in msg_ids.iter().enumerate() {
            assert_eq!(msg_id.stream_index(), 0);
            assert_eq!(msg_id.sub_offset(), i as i64);
        }

        mux.flush().await?;
        assert_eq!(mock_server.get_write_count().await, 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_batch_records() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_batch_records");

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "s1".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let stream = create_test_stream(&sdk, TABLE, default_options()).await?;
        let mux = MultiplexedStream::new(vec![stream]);

        let batch = vec![b"r1".to_vec(), b"r2".to_vec(), b"r3".to_vec()];
        let offset = mux.ingest_records(batch).await?;
        assert!(offset.is_some());

        mux.flush().await?;
        assert_eq!(mock_server.get_write_count().await, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_empty_batch_returns_none() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE,
                vec![MockResponse::CreateStream {
                    stream_id: "s1".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let stream = create_test_stream(&sdk, TABLE, default_options()).await?;
        let mux = MultiplexedStream::new(vec![stream]);

        let batch: Vec<Vec<u8>> = vec![];
        let offset = mux.ingest_records(batch).await?;
        assert!(offset.is_none());

        Ok(())
    }
}

mod multi_stream_tests {
    use super::*;
    use std::time::Duration;

    /// Use separate table names per stream so each gRPC connection gets its own response sequence.
    const TABLE_A: &str = "multi.schema.table_a";
    const TABLE_B: &str = "multi.schema.table_b";

    #[tokio::test]
    async fn test_round_robin_across_two_streams() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_round_robin_across_two_streams");

        let (mock_server, server_url) = start_mock_server().await?;

        // Stream A gets records at sub-offsets 0, 1, 2 (mux records 0, 2, 4)
        mock_server
            .inject_responses(
                TABLE_A,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "mux_a".to_string(),
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

        // Stream B gets records at sub-offsets 0, 1, 2 (mux records 1, 3, 5)
        mock_server
            .inject_responses(
                TABLE_B,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "mux_b".to_string(),
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

        let sdk = create_test_sdk(&server_url).await?;
        let s1 = create_test_stream(&sdk, TABLE_A, default_options()).await?;
        let s2 = create_test_stream(&sdk, TABLE_B, default_options()).await?;
        let mux = MultiplexedStream::new(vec![s1, s2]);

        let mut msg_ids = Vec::new();
        for _ in 0..6 {
            let msg_id = mux.ingest_record(b"data".to_vec()).await?;
            msg_ids.push(msg_id);
        }

        // Round-robin: even indices go to stream 0, odd to stream 1
        for (i, msg_id) in msg_ids.iter().enumerate() {
            assert_eq!(msg_id.stream_index(), i % 2);
        }

        mux.flush().await?;
        assert_eq!(mock_server.get_write_count().await, 6);

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_waits_on_chosen_stream_when_it_is_full(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_waits_on_chosen_stream_when_it_is_full");

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_A,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "slow_a".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 250,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 1,
                        delay_ms: 0,
                    },
                ],
            )
            .await;
        mock_server
            .inject_responses(
                TABLE_B,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "fast_b".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let opts = TestOpts {
            max_inflight_requests: 1,
            ..default_options()
        };
        let s1 = create_test_stream(&sdk, TABLE_A, opts.clone()).await?;
        let s2 = create_test_stream(&sdk, TABLE_B, opts).await?;
        let mux = Arc::new(MultiplexedStream::new(vec![s1, s2]));

        let first = mux.ingest_record(b"slow_a".to_vec()).await?;
        assert_eq!(first.stream_index(), 0);

        let second = mux.ingest_record(b"fast_b".to_vec()).await?;
        assert_eq!(second.stream_index(), 1);
        mux.wait_for_message_id(second).await?;

        let mux_for_task = Arc::clone(&mux);
        let third_task =
            tokio::spawn(async move { mux_for_task.ingest_record(b"waited".to_vec()).await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !third_task.is_finished(),
            "Expected ingest to wait on the chosen stream even when another sibling is free"
        );

        let third = tokio::time::timeout(Duration::from_millis(500), third_task)
            .await
            .expect("ingest should complete once the chosen lane drains")??;

        assert_eq!(third.stream_index(), 0);
        assert_eq!(third.sub_offset(), 1);
        mux.wait_for_message_id(first).await?;
        mux.wait_for_message_id(third).await?;
        assert_eq!(mock_server.get_write_count().await, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_batch_ingest_waits_on_chosen_stream_when_it_is_full(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_batch_ingest_waits_on_chosen_stream_when_it_is_full");

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_A,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "slow_a".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 250,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 1,
                        delay_ms: 0,
                    },
                ],
            )
            .await;
        mock_server
            .inject_responses(
                TABLE_B,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "fast_b".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let opts = TestOpts {
            max_inflight_requests: 1,
            ..default_options()
        };
        let s1 = create_test_stream(&sdk, TABLE_A, opts.clone()).await?;
        let s2 = create_test_stream(&sdk, TABLE_B, opts).await?;
        let mux = Arc::new(MultiplexedStream::new(vec![s1, s2]));

        let first = mux.ingest_record(b"slow_a".to_vec()).await?;
        assert_eq!(first.stream_index(), 0);

        let second = mux.ingest_record(b"fast_b".to_vec()).await?;
        assert_eq!(second.stream_index(), 1);
        mux.wait_for_message_id(second).await?;

        let mux_for_task = Arc::clone(&mux);
        let batch_task = tokio::spawn(async move {
            mux_for_task
                .ingest_records(vec![b"r1".to_vec(), b"r2".to_vec(), b"r3".to_vec()])
                .await
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !batch_task.is_finished(),
            "Expected batch ingest to wait on the chosen stream even when another sibling is free"
        );

        let batch_id = tokio::time::timeout(Duration::from_millis(500), batch_task)
            .await
            .expect("batch ingest should complete once the chosen lane drains")??
            .expect("non-empty batch should return a message id");

        assert_eq!(batch_id.stream_index(), 0);
        assert_eq!(batch_id.sub_offset(), 1);
        mux.wait_for_message_id(first).await?;
        mux.wait_for_message_id(batch_id).await?;
        assert_eq!(mock_server.get_write_count().await, 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_blocks_on_original_lane_when_all_streams_are_full(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_blocks_on_original_lane_when_all_streams_are_full");

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_A,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "slow_a".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 200,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 1,
                        delay_ms: 0,
                    },
                ],
            )
            .await;
        mock_server
            .inject_responses(
                TABLE_B,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "slow_b".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 200,
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let opts = TestOpts {
            max_inflight_requests: 1,
            ..default_options()
        };
        let s1 = create_test_stream(&sdk, TABLE_A, opts.clone()).await?;
        let s2 = create_test_stream(&sdk, TABLE_B, opts).await?;
        let mux = Arc::new(MultiplexedStream::new(vec![s1, s2]));

        let first = mux.ingest_record(b"slow_a".to_vec()).await?;
        let second = mux.ingest_record(b"slow_b".to_vec()).await?;

        let mux_for_task = Arc::clone(&mux);
        let third_task =
            tokio::spawn(async move { mux_for_task.ingest_record(b"blocked".to_vec()).await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !third_task.is_finished(),
            "Expected ingest to block while all sub-streams are full"
        );

        let third = tokio::time::timeout(Duration::from_millis(500), third_task)
            .await
            .expect("ingest should complete once the chosen lane drains")??;

        assert_eq!(
            third.stream_index(),
            0,
            "Expected fallback to the original chosen lane when all lanes are full"
        );
        assert_eq!(third.sub_offset(), 1);

        mux.wait_for_message_id(first).await?;
        mux.wait_for_message_id(second).await?;
        mux.wait_for_message_id(third).await?;
        assert_eq!(mock_server.get_write_count().await, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_close_with_multiple_streams() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_close_with_multiple_streams");

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_A,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "mux_a".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                ],
            )
            .await;
        mock_server
            .inject_responses(
                TABLE_B,
                vec![MockResponse::CreateStream {
                    stream_id: "mux_b".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let s1 = create_test_stream(&sdk, TABLE_A, default_options()).await?;
        let s2 = create_test_stream(&sdk, TABLE_B, default_options()).await?;
        let mut mux = MultiplexedStream::new(vec![s1, s2]);

        let offset = mux.ingest_record(b"data".to_vec()).await?;
        mux.wait_for_message_id(offset).await?;

        mux.close().await?;
        assert!(mux.is_closed());

        Ok(())
    }
}

mod failure_tests {
    use super::*;

    const TABLE_OK: &str = "fail.schema.ok";
    const TABLE_FAIL: &str = "fail.schema.fail";

    #[tokio::test]
    async fn test_wait_for_message_id_on_failed_stream_poisons_mux(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_wait_for_message_id_on_failed_stream_poisons_mux");

        let (mock_server, server_url) = start_mock_server().await?;

        // Stream OK: acks its record
        mock_server
            .inject_responses(
                TABLE_OK,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "stream_ok".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        // Stream FAIL: accepts ingest (into landing zone) but then the server
        // errors, so wait_for_message_id will surface the error.
        mock_server
            .inject_responses(
                TABLE_FAIL,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "stream_fail".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::Error {
                        status: tonic::Status::permission_denied("sub-stream failure"),
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let s1 = create_test_stream(&sdk, TABLE_OK, default_options()).await?;
        let s2 = create_test_stream(&sdk, TABLE_FAIL, default_options()).await?;
        let mux = MultiplexedStream::new(vec![s1, s2]);

        // First ingest (stream 0 = OK), succeeds fully
        let offset0 = mux.ingest_record(b"record1".to_vec()).await?;
        mux.wait_for_message_id(offset0).await?;

        // Second ingest (stream 1 = FAIL) — queuing succeeds
        let offset1 = mux.ingest_record(b"record2".to_vec()).await?;

        // But waiting for ack surfaces the sub-stream error
        let result = mux.wait_for_message_id(offset1).await;
        assert!(result.is_err(), "Expected error from failed sub-stream");

        // The sub-stream closed (non-retryable error), so the mux should be poisoned
        // and further ingest should fail with InvalidStateError.
        assert!(
            mux.is_closed(),
            "Expected mux to be poisoned after sub-stream close"
        );
        let ingest_after = mux.ingest_record(b"record3".to_vec()).await;
        assert!(
            matches!(ingest_after, Err(ZerobusError::InvalidStateError(_))),
            "Expected InvalidStateError on ingest after poison, got {:?}",
            ingest_after
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_error_on_closed_substream_poisons_mux(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_flush_error_on_closed_substream_poisons_mux");

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_FAIL,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "s1".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::Error {
                        status: tonic::Status::permission_denied("fail"),
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let s1 = create_test_stream(&sdk, TABLE_FAIL, default_options()).await?;
        let mux = MultiplexedStream::new(vec![s1]);

        let _ = mux.ingest_record(b"data".to_vec()).await?;

        // Non-retryable error → sub-stream closes → flush errors → mux poisoned.
        let flush_result = mux.flush().await;
        assert!(flush_result.is_err(), "Expected flush to fail");
        assert!(
            mux.is_closed(),
            "Expected mux poisoned after sub-stream close"
        );

        let ingest_after = mux.ingest_record(b"data".to_vec()).await;
        assert!(
            matches!(ingest_after, Err(ZerobusError::InvalidStateError(_))),
            "Expected InvalidStateError after poison, got {:?}",
            ingest_after
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_wait_timeout_does_not_poison_mux() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_wait_timeout_does_not_poison_mux");

        let (mock_server, server_url) = start_mock_server().await?;
        // Ack arrives well after our short flush_timeout_ms expires.
        mock_server
            .inject_responses(
                TABLE_OK,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "slow".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 2_000,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 1,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let opts = TestOpts {
            flush_timeout_ms: Some(100),
            ..default_options()
        };
        let s = create_test_stream(&sdk, TABLE_OK, opts).await?;
        let mux = MultiplexedStream::new(vec![s]);

        let offset = mux.ingest_record(b"slow".to_vec()).await?;

        // First wait times out before the ack arrives; sub-stream is still alive.
        let result = mux.wait_for_message_id(offset).await;
        assert!(result.is_err(), "Expected timeout error");
        assert!(!mux.is_closed(), "Mux should NOT be poisoned by timeout");

        // Further ingest should still succeed.
        let _ = mux.ingest_record(b"next".to_vec()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_get_unacked_records_auto_closes_mux() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_get_unacked_records_auto_closes_mux");

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_OK,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "s1".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let s = create_test_stream(&sdk, TABLE_OK, default_options()).await?;
        let mut mux = MultiplexedStream::new(vec![s]);

        let id = mux.ingest_record(b"hello".to_vec()).await?;
        mux.wait_for_message_id(id).await?;
        assert!(!mux.is_closed());

        let unacked: Vec<_> = mux.get_unacked_records().await?.collect();
        assert!(unacked.is_empty(), "All records were acked");
        assert!(
            mux.is_closed(),
            "get_unacked_records should have closed the mux"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_unacked_records_returns_failed_records(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_get_unacked_records_returns_failed_records");

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_FAIL,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "s1".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::Error {
                        status: tonic::Status::permission_denied("boom"),
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let s = create_test_stream(&sdk, TABLE_FAIL, default_options()).await?;
        let mut mux = MultiplexedStream::new(vec![s]);

        let _id = mux.ingest_record(b"will_fail".to_vec()).await?;
        // Trigger the failure and poisoning.
        let _ = mux.flush().await;
        assert!(mux.is_closed());

        let unacked: Vec<_> = mux.get_unacked_records().await?.collect();
        assert_eq!(unacked.len(), 1, "Expected the unacked record");
        assert!(
            matches!(&unacked[0], databricks_zerobus_ingest_sdk::EncodedRecord::Proto(b) if b == b"will_fail"),
            "Unexpected record payload: {:?}",
            unacked[0]
        );

        Ok(())
    }

    /// Poison must not lose records sitting unacked on a *healthy* sub-stream.
    ///
    /// The healthy stream's ack is delayed past the flush timeout, so the
    /// best-effort flush in the poison path times out and the record is still
    /// in the landing zone when the stream is torn down via signal_shutdown
    /// (no supervisor failure → `failed_records` never populated for it).
    /// `get_unacked_records` must report it anyway.
    #[tokio::test]
    async fn test_get_unacked_records_includes_stranded_records_on_healthy_streams(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_get_unacked_records_includes_stranded_records_on_healthy_streams");

        let (mock_server, server_url) = start_mock_server().await?;
        // Healthy stream: ack arrives long after the flush timeout.
        mock_server
            .inject_responses(
                TABLE_OK,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "stream_ok".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 5_000,
                    },
                ],
            )
            .await;
        // Failing stream: non-retryable error poisons the mux.
        mock_server
            .inject_responses(
                TABLE_FAIL,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "stream_fail".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::Error {
                        status: tonic::Status::permission_denied("boom"),
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let opts = TestOpts {
            flush_timeout_ms: Some(100),
            ..default_options()
        };
        let s1 = create_test_stream(&sdk, TABLE_OK, opts.clone()).await?;
        let s2 = create_test_stream(&sdk, TABLE_FAIL, opts).await?;
        let mut mux = MultiplexedStream::new(vec![s1, s2]);

        // Round-robin: first record lands on the healthy stream and stays
        // unacked, second lands on the failing stream.
        let _stranded = mux.ingest_record(b"stranded".to_vec()).await?;
        let failed = mux.ingest_record(b"failed".to_vec()).await?;

        let result = mux.wait_for_message_id(failed).await;
        assert!(result.is_err(), "Expected error from failed sub-stream");
        assert!(mux.is_closed(), "Expected mux to be poisoned");

        let mut unacked: Vec<_> = mux
            .get_unacked_records()
            .await?
            .map(|record| match record {
                databricks_zerobus_ingest_sdk::EncodedRecord::Proto(b) => b,
                other => panic!("Unexpected record type: {:?}", other),
            })
            .collect();
        unacked.sort();
        assert_eq!(
            unacked,
            vec![b"failed".to_vec(), b"stranded".to_vec()],
            "Expected both the failed and the stranded record to be recoverable"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_surfaces_sub_stream_error() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_FAIL,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "s1".to_string(),
                        delay_ms: 0,
                    },
                    // Accept ingest but then error — flush will fail
                    MockResponse::Error {
                        status: tonic::Status::permission_denied("fail"),
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let s1 = create_test_stream(&sdk, TABLE_FAIL, default_options()).await?;
        let mux = MultiplexedStream::new(vec![s1]);

        // Ingest queues successfully
        let _ = mux.ingest_record(b"data".to_vec()).await?;

        // Flush should fail because the sub-stream errors
        let result = mux.flush().await;
        assert!(result.is_err(), "Expected flush to fail");

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_ingest_waiting_for_capacity_fails_after_mux_poison(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_concurrent_ingest_waiting_for_capacity_fails_after_mux_poison");

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_FAIL,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "poisoned".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::Error {
                        status: tonic::Status::permission_denied("sub-stream failure"),
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let opts = TestOpts {
            max_inflight_requests: 1,
            ..default_options()
        };
        let stream = create_test_stream(&sdk, TABLE_FAIL, opts).await?;
        let mux = Arc::new(MultiplexedStream::new(vec![stream]));

        const TASKS: usize = 16;
        let barrier = Arc::new(tokio::sync::Barrier::new(TASKS));
        let mut handles = Vec::with_capacity(TASKS);

        for i in 0..TASKS {
            let mux = Arc::clone(&mux);
            let barrier = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                mux.ingest_record(format!("record-{i}").into_bytes()).await
            }));
        }

        let mut successes = Vec::new();
        let mut errors = 0;
        for handle in handles {
            match tokio::time::timeout(std::time::Duration::from_secs(2), handle)
                .await
                .expect("ingest task should finish after mux poison")?
            {
                Ok(message_id) => successes.push(message_id),
                Err(ZerobusError::InvalidStateError(_))
                | Err(ZerobusError::StreamClosedError(_)) => errors += 1,
                Err(e) => panic!("unexpected ingest error: {e:?}"),
            }
        }

        assert_eq!(
            successes.len(),
            1,
            "Only the first record should be admitted before poison"
        );
        assert_eq!(successes[0].stream_index(), 0);
        assert_eq!(successes[0].sub_offset(), 0);
        assert_eq!(errors, TASKS - 1);
        assert!(mux.is_closed(), "Mux should report the failed sub-stream");
        assert_eq!(mock_server.get_write_count().await, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_times_out_when_capacity_never_recovers(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_OK,
                vec![MockResponse::CreateStream {
                    stream_id: "stalled".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let stream = create_test_stream(
            &sdk,
            TABLE_OK,
            TestOpts {
                max_inflight_requests: 1,
                flush_timeout_ms: Some(100),
            },
        )
        .await?;
        let mux = MultiplexedStream::new(vec![stream]);

        mux.ingest_record(b"fills-capacity".to_vec()).await?;
        let result = mux.ingest_record(b"times-out".to_vec()).await;

        assert!(matches!(
            result,
            Err(ZerobusError::ConnectionTimeout(message))
                if message.contains("sub-stream 0")
        ));
        assert!(
            !mux.is_closed(),
            "capacity timeout should not poison the mux"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_public_is_closed_reports_closed_substream(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();

        // Public `is_closed()` should be an eager status check: even when the
        // mux has not lazily observed the failed lane through ingest/flush/wait,
        // it should report a sub-stream that has closed asynchronously.
        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_FAIL,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "s1".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::Error {
                        status: tonic::Status::permission_denied("async close"),
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let stream = create_test_stream(&sdk, TABLE_FAIL, default_options()).await?;

        let _ = stream.ingest_record_offset(b"data".to_vec()).await?;
        let mux = MultiplexedStream::new(vec![stream]);

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !mux.is_closed() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("mux public is_closed should report the closed inner stream");

        Ok(())
    }

    #[tokio::test]
    async fn test_is_closed_when_sub_stream_closes() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_FAIL,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "s1".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let s1 = create_test_stream(&sdk, TABLE_FAIL, default_options()).await?;
        let mut mux = MultiplexedStream::new(vec![s1]);

        assert!(!mux.is_closed());

        let offset = mux.ingest_record(b"data".to_vec()).await?;
        mux.wait_for_message_id(offset).await?;
        mux.close().await?;

        assert!(mux.is_closed());

        Ok(())
    }
}

mod offset_mapping_tests {
    use super::*;

    const TABLE: &str = "offset.schema.table";

    #[tokio::test]
    async fn test_wait_for_unknown_message_id_returns_error(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE,
                vec![MockResponse::CreateStream {
                    stream_id: "s1".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let stream = create_test_stream(&sdk, TABLE, default_options()).await?;
        let mux = MultiplexedStream::new(vec![stream]);

        // Construct a message id pointing to a non-existent stream (index 63)
        let bad_id = MessageId::from_raw(63i64 << (64 - 6));
        let result = mux.wait_for_message_id(bad_id).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ZerobusError::InvalidArgument(msg) if msg.contains("63")
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_wait_for_message_id_maps_correctly() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_wait_for_message_id_maps_correctly");

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "s1".to_string(),
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

        let sdk = create_test_sdk(&server_url).await?;
        let stream = create_test_stream(&sdk, TABLE, default_options()).await?;
        let mux = MultiplexedStream::new(vec![stream]);

        let o0 = mux.ingest_record(b"a".to_vec()).await?;
        let o1 = mux.ingest_record(b"b".to_vec()).await?;
        let o2 = mux.ingest_record(b"c".to_vec()).await?;

        mux.wait_for_message_id(o0).await?;
        mux.wait_for_message_id(o1).await?;
        mux.wait_for_message_id(o2).await?;

        assert_eq!(mock_server.get_write_count().await, 3);

        Ok(())
    }
}
