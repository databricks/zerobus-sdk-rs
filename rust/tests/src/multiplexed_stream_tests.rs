mod mock_grpc;
mod utils;

use std::future::Future;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use databricks_zerobus_ingest_sdk::databricks::zerobus::RecordType;
use databricks_zerobus_ingest_sdk::{
    AckCallback, HeadersProvider, MessageId, MultiplexedStream, NoTlsConfig, StreamBuilder,
    ZerobusError, ZerobusSdk, ZerobusStream,
};
use futures::poll;
use mock_grpc::{start_mock_server, MockResponse, MockResponseGate};
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

mod public_builder_tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use super::*;

    const TABLE: &str = "builder.schema.table";
    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    fn create_response(stream_id: &str) -> MockResponse {
        MockResponse::CreateStream {
            stream_id: stream_id.to_string(),
            delay_ms: 0,
        }
    }

    fn ack_response() -> MockResponse {
        MockResponse::RecordAck {
            ack_up_to_offset: 0,
            delay_ms: 0,
        }
    }

    async fn test_sdk(
        responses: Vec<MockResponse>,
    ) -> TestResult<(mock_grpc::MockZerobusServer, ZerobusSdk)> {
        let (server, url) = start_mock_server().await?;
        server.inject_responses(TABLE, responses).await;
        let sdk = create_test_sdk(&url).await?;
        Ok((server, sdk))
    }

    fn builder<'a>(
        sdk: &'a ZerobusSdk,
        headers_provider: Arc<dyn HeadersProvider>,
    ) -> StreamBuilder<'a> {
        sdk.stream_builder()
            .table(TABLE)
            .headers_provider(headers_provider)
            .recovery(false)
    }

    #[derive(Default)]
    struct RecordingCallback(Mutex<Vec<MessageId>>);

    impl AckCallback<MessageId> for RecordingCallback {
        fn on_ack(&self, message_id: MessageId) {
            self.0.lock().unwrap().push(message_id);
        }

        fn on_error(&self, _message_id: MessageId, _error_message: &str) {}
    }

    enum HeaderBehavior {
        FailFirst,
        BlockAfterFirst(tokio::sync::mpsc::UnboundedSender<()>),
    }

    struct ControlledHeadersProvider {
        calls: AtomicUsize,
        behavior: HeaderBehavior,
    }

    #[async_trait::async_trait]
    impl HeadersProvider for ControlledHeadersProvider {
        async fn get_headers(&self) -> Result<HashMap<&'static str, String>, ZerobusError> {
            let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
            match (&self.behavior, first) {
                (HeaderBehavior::FailFirst, true) => Err(ZerobusError::InvalidArgument(
                    "injected mux construction failure".to_string(),
                )),
                (HeaderBehavior::BlockAfterFirst(blocked), false) => {
                    let _ = blocked.send(());
                    std::future::pending().await
                }
                _ => Ok(HashMap::new()),
            }
        }
    }

    #[tokio::test]
    async fn builder_opens_all_streams_and_adapts_callbacks() -> TestResult {
        setup_tracing();
        // Each connection clones this script. Connections that start after the
        // first create response use the mock's fallback create and still begin
        // record responses at index 1.
        let mut responses = vec![create_response("mux-builder")];
        responses.extend(vec![ack_response(); 4]);
        let (_server, sdk) = test_sdk(responses).await?;
        let callback = Arc::new(RecordingCallback::default());
        let mut mux = builder(&sdk, Arc::new(TestHeadersProvider::default()))
            .compiled_proto(
                create_test_descriptor_proto().ok_or("failed to create test descriptor")?,
            )
            .multiplexed_ack_callback(callback.clone())
            .multiplexed(2)
            .build()
            .await?;

        let first = mux.ingest_record(b"first".to_vec()).await?;
        let second = mux.ingest_record(b"second".to_vec()).await?;
        assert_eq!(first.stream_index(), 0);
        assert_eq!(second.stream_index(), 1);
        assert_eq!(first.sub_offset(), 0);
        assert_eq!(second.sub_offset(), 0);
        mux.flush().await?;

        assert!(matches!(
            mux.new_record(),
            Err(ZerobusError::InvalidArgument(_))
        ));
        mux.close().await?;

        let mut callback_ids = callback.0.lock().unwrap().clone();
        callback_ids.sort_by_key(MessageId::stream_index);
        assert_eq!(callback_ids, vec![first, second]);
        Ok(())
    }

    #[tokio::test]
    async fn builder_partitions_inflight_capacity_across_streams() -> TestResult {
        let ack_gate = Arc::new(MockResponseGate::new());
        let responses = vec![
            create_response("mux-capacity"),
            MockResponse::GatedRecordAck {
                ack_up_to_offset: 0,
                gate: Arc::clone(&ack_gate),
            },
            MockResponse::RecordAck {
                ack_up_to_offset: 1,
                delay_ms: 0,
            },
        ];
        let (_server, sdk) = test_sdk(responses).await?;
        let mut mux = builder(&sdk, Arc::new(TestHeadersProvider::default()))
            .compiled_proto(
                create_test_descriptor_proto().ok_or("failed to create test descriptor")?,
            )
            .max_inflight_requests(2)
            .multiplexed(2)
            .build()
            .await?;

        let first = mux.ingest_record(b"lane-0".to_vec()).await?;
        let second = mux.ingest_record(b"lane-1".to_vec()).await?;
        assert_eq!(first.stream_index(), 0);
        assert_eq!(second.stream_index(), 1);

        let mut third = Box::pin(mux.ingest_record(b"lane-0-blocked".to_vec()));
        assert!(matches!(poll!(&mut third), Poll::Pending));
        tokio::task::yield_now().await;
        assert!(matches!(poll!(&mut third), Poll::Pending));

        ack_gate.release();
        ack_gate.release();
        let third = tokio::time::timeout(Duration::from_secs(3), third).await??;
        assert_eq!(third.stream_index(), 0);
        assert_eq!(third.sub_offset(), 1);
        tokio::time::timeout(Duration::from_secs(3), mux.close()).await??;
        Ok(())
    }

    #[tokio::test]
    async fn dynamic_proto_mux_creates_records() -> TestResult {
        let (_server, sdk) = test_sdk(vec![create_response("dynamic-mux")]).await?;
        let descriptor =
            create_test_descriptor_proto().ok_or("failed to create test descriptor")?;
        let message_descriptor = databricks_zerobus_ingest_sdk::message_descriptor(&descriptor)?;
        let mut mux = builder(&sdk, Arc::new(TestHeadersProvider::default()))
            .dynamic_proto(message_descriptor)
            .multiplexed(1)
            .build()
            .await?;

        mux.new_record()?;
        mux.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn json_mux_sends_json_record_type() -> TestResult {
        let (server, sdk) = test_sdk(vec![create_response("json-mux")]).await?;
        let mut mux = builder(&sdk, Arc::new(TestHeadersProvider::default()))
            .json()
            .multiplexed(1)
            .build()
            .await?;

        assert_eq!(
            server.get_create_record_types().await,
            vec![Some(RecordType::Json as i32)]
        );
        mux.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn partial_construction_failure_closes_successful_stream() -> TestResult {
        let (mock_server, sdk) = test_sdk(vec![create_response("partial-success")]).await?;
        let provider = Arc::new(ControlledHeadersProvider {
            calls: AtomicUsize::new(0),
            behavior: HeaderBehavior::FailFirst,
        });
        let result = builder(&sdk, provider.clone())
            .json()
            .multiplexed(2)
            .build()
            .await;

        assert!(matches!(
            result,
            Err(ZerobusError::InvalidArgument(message))
                if message == "injected mux construction failure"
        ));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(mock_server.get_stream_count().await, 1);
        assert_eq!(
            Arc::strong_count(&provider),
            1,
            "the successful stream and all active creation attempts must be dropped before build returns"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancelling_partial_construction_releases_all_lanes() -> TestResult {
        let (mock_server, sdk) = test_sdk(vec![create_response("opened-before-cancel")]).await?;
        let (blocked_tx, mut blocked_rx) = tokio::sync::mpsc::unbounded_channel();
        let provider = Arc::new(ControlledHeadersProvider {
            calls: AtomicUsize::new(0),
            behavior: HeaderBehavior::BlockAfterFirst(blocked_tx),
        });
        let task_provider = Arc::clone(&provider);
        let build_task = tokio::spawn(async move {
            builder(&sdk, task_provider)
                .json()
                .multiplexed(2)
                .build()
                .await
        });

        tokio::time::timeout(Duration::from_secs(3), blocked_rx.recv())
            .await?
            .ok_or("blocking creation attempt ended unexpectedly")?;
        tokio::time::timeout(Duration::from_secs(3), async {
            while mock_server.get_stream_count().await < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await?;

        build_task.abort();
        match build_task.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("aborted multiplexed build task unexpectedly completed"),
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            while Arc::strong_count(&provider) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
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
        MultiplexedStream::from_streams_for_testing(vec![]);
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
        let mux = MultiplexedStream::from_streams_for_testing(vec![stream]);

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
        let mux = MultiplexedStream::from_streams_for_testing(vec![stream]);

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
        let mux = MultiplexedStream::from_streams_for_testing(vec![stream]);

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
        let mux = MultiplexedStream::from_streams_for_testing(vec![stream]);

        let batch: Vec<Vec<u8>> = vec![];
        let offset = mux.ingest_records(batch).await?;
        assert!(offset.is_none());

        Ok(())
    }
}

mod multi_stream_tests {
    use super::*;

    const COMPLETION_TIMEOUT: Duration = Duration::from_secs(5);

    /// Use separate table names per stream so each gRPC connection gets its own response sequence.
    const TABLE_A: &str = "multi.schema.table_a";
    const TABLE_B: &str = "multi.schema.table_b";

    async fn expect_completion<F>(future: F, message: &str) -> F::Output
    where
        F: Future,
    {
        tokio::time::timeout(COMPLETION_TIMEOUT, future)
            .await
            .expect(message)
    }

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
        let mux = MultiplexedStream::from_streams_for_testing(vec![s1, s2]);

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
    async fn test_ingest_reroutes_when_preferred_stream_is_full(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_reroutes_when_preferred_stream_is_full");

        let (mock_server, server_url) = start_mock_server().await?;
        let slow_ack = Arc::new(MockResponseGate::new());
        mock_server
            .inject_responses(
                TABLE_A,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "slow_a".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::GatedRecordAck {
                        ack_up_to_offset: 0,
                        gate: Arc::clone(&slow_ack),
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
                    MockResponse::RecordAck {
                        ack_up_to_offset: 1,
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
        let mux = Arc::new(MultiplexedStream::from_streams_for_testing(vec![s1, s2]));

        let first = mux.ingest_record(b"slow_a".to_vec()).await?;
        assert_eq!(first.stream_index(), 0);

        let second = mux.ingest_record(b"fast_b".to_vec()).await?;
        assert_eq!(second.stream_index(), 1);
        mux.wait_for_message_id(second).await?;

        let third = mux.ingest_record(b"rerouted".to_vec()).await?;
        assert_eq!(third.stream_index(), 1);
        assert_eq!(third.sub_offset(), 1);
        expect_completion(
            mux.wait_for_message_id(third),
            "rerouted record should be acknowledged on the available lane",
        )
        .await?;

        slow_ack.release();
        expect_completion(
            mux.wait_for_message_id(first),
            "first record should be acknowledged after releasing its gate",
        )
        .await?;
        assert_eq!(mock_server.get_write_count().await, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_batch_ingest_reroutes_when_preferred_stream_is_full(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_batch_ingest_reroutes_when_preferred_stream_is_full");

        let (mock_server, server_url) = start_mock_server().await?;
        let slow_ack = Arc::new(MockResponseGate::new());
        mock_server
            .inject_responses(
                TABLE_A,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "slow_a".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::GatedRecordAck {
                        ack_up_to_offset: 0,
                        gate: Arc::clone(&slow_ack),
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
                    MockResponse::RecordAck {
                        ack_up_to_offset: 1,
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
        let mux = Arc::new(MultiplexedStream::from_streams_for_testing(vec![s1, s2]));

        let first = mux.ingest_record(b"slow_a".to_vec()).await?;
        assert_eq!(first.stream_index(), 0);

        let second = mux.ingest_record(b"fast_b".to_vec()).await?;
        assert_eq!(second.stream_index(), 1);
        mux.wait_for_message_id(second).await?;

        let batch_id = mux
            .ingest_records(vec![b"r1".to_vec(), b"r2".to_vec(), b"r3".to_vec()])
            .await?
            .expect("non-empty batch should return a message id");

        assert_eq!(batch_id.stream_index(), 1);
        assert_eq!(batch_id.sub_offset(), 1);
        expect_completion(
            mux.wait_for_message_id(batch_id),
            "rerouted batch should be acknowledged on the available lane",
        )
        .await?;

        slow_ack.release();
        expect_completion(
            mux.wait_for_message_id(first),
            "first record should be acknowledged after releasing its gate",
        )
        .await?;
        assert_eq!(mock_server.get_write_count().await, 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_uses_first_lane_to_drain_when_all_streams_are_full(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_ingest_uses_first_lane_to_drain_when_all_streams_are_full");

        let (mock_server, server_url) = start_mock_server().await?;
        let lane_a_ack = Arc::new(MockResponseGate::new());
        let lane_b_ack = Arc::new(MockResponseGate::new());
        mock_server
            .inject_responses(
                TABLE_A,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "slow_a".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::GatedRecordAck {
                        ack_up_to_offset: 0,
                        gate: Arc::clone(&lane_a_ack),
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
                    MockResponse::GatedRecordAck {
                        ack_up_to_offset: 0,
                        gate: Arc::clone(&lane_b_ack),
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
        let mux = Arc::new(MultiplexedStream::from_streams_for_testing(vec![s1, s2]));

        let first = mux.ingest_record(b"slow_a".to_vec()).await?;
        let second = mux.ingest_record(b"slow_b".to_vec()).await?;

        let mut third_future = Box::pin(mux.ingest_record(b"blocked".to_vec()));
        assert!(
            matches!(poll!(&mut third_future), Poll::Pending),
            "Expected ingest to block while all sub-streams are full"
        );
        tokio::task::yield_now().await;
        assert!(
            matches!(poll!(&mut third_future), Poll::Pending),
            "Expected ingest to remain blocked after yielding"
        );

        lane_a_ack.release();
        let third =
            expect_completion(third_future, "ingest should complete once any lane drains").await?;

        assert_eq!(
            third.stream_index(),
            0,
            "Expected the first lane that drained"
        );
        assert_eq!(third.sub_offset(), 1);

        lane_b_ack.release();
        expect_completion(
            mux.wait_for_message_id(first),
            "first record should be acknowledged after releasing lane A",
        )
        .await?;
        expect_completion(
            mux.wait_for_message_id(second),
            "second record should be acknowledged after releasing lane B",
        )
        .await?;
        expect_completion(
            mux.wait_for_message_id(third),
            "third record should be acknowledged after its lane drains",
        )
        .await?;
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
        let mut mux = MultiplexedStream::from_streams_for_testing(vec![s1, s2]);

        let offset = mux.ingest_record(b"data".to_vec()).await?;
        mux.wait_for_message_id(offset).await?;

        mux.close().await?;
        assert!(mux.is_closed());

        Ok(())
    }

    #[tokio::test]
    async fn test_close_times_out_all_streams_concurrently(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();

        let (mock_server, server_url) = start_mock_server().await?;
        let lane_a_ack = Arc::new(MockResponseGate::new());
        let lane_b_ack = Arc::new(MockResponseGate::new());
        mock_server
            .inject_responses(
                TABLE_A,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "close_a".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::GatedRecordAck {
                        ack_up_to_offset: 0,
                        gate: Arc::clone(&lane_a_ack),
                    },
                ],
            )
            .await;
        mock_server
            .inject_responses(
                TABLE_B,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "close_b".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::GatedRecordAck {
                        ack_up_to_offset: 0,
                        gate: Arc::clone(&lane_b_ack),
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let opts = TestOpts {
            flush_timeout_ms: Some(5_000),
            ..default_options()
        };
        let s1 = create_test_stream(&sdk, TABLE_A, opts.clone()).await?;
        let s2 = create_test_stream(&sdk, TABLE_B, opts).await?;
        let mut mux = MultiplexedStream::from_streams_for_testing(vec![s1, s2]);

        mux.ingest_record(b"lane-a".to_vec()).await?;
        mux.ingest_record(b"lane-b".to_vec()).await?;
        expect_completion(
            async {
                while mock_server.get_write_count().await < 2 {
                    tokio::task::yield_now().await;
                }
            },
            "both records should reach the server",
        )
        .await;

        tokio::time::pause();
        let mut close = Box::pin(mux.close());
        assert!(matches!(poll!(&mut close), Poll::Pending));
        tokio::time::advance(Duration::from_millis(5_001)).await;
        assert!(matches!(poll!(&mut close), Poll::Pending));
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(1_001)).await;

        let mut close_result = None;
        for _ in 0..20 {
            match poll!(&mut close) {
                Poll::Ready(result) => {
                    close_result = Some(result);
                    break;
                }
                Poll::Pending => tokio::task::yield_now().await,
            }
        }
        tokio::time::resume();
        lane_a_ack.release();
        lane_b_ack.release();

        close_result
            .expect("all lane flush timeouts should elapse concurrently")
            .expect_err("unacknowledged lanes should time out during close");
        Ok(())
    }
}

mod failure_tests {
    use std::sync::Mutex;

    use super::*;
    use databricks_zerobus_ingest_sdk::OffsetId;

    const TABLE_OK: &str = "fail.schema.ok";
    const TABLE_FAIL: &str = "fail.schema.fail";

    #[derive(Default)]
    struct RecordingOffsetCallback {
        errors: Mutex<Vec<(OffsetId, String)>>,
    }

    impl AckCallback for RecordingOffsetCallback {
        fn on_ack(&self, _offset_id: OffsetId) {}

        fn on_error(&self, offset_id: OffsetId, error_message: &str) {
            self.errors
                .lock()
                .unwrap()
                .push((offset_id, error_message.to_string()));
        }
    }

    async fn create_callback_stream(
        sdk: &ZerobusSdk,
        table_name: &str,
        callback: Arc<dyn AckCallback>,
    ) -> Result<ZerobusStream, ZerobusError> {
        sdk.stream_builder()
            .table(table_name)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap())
            .ack_callback(callback)
            .callback_max_wait_time_ms(Some(1_000))
            .recovery(false)
            .build()
            .await
    }

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
        let mux = MultiplexedStream::from_streams_for_testing(vec![s1, s2]);

        // First ingest (stream 0 = OK), succeeds fully
        let offset0 = mux.ingest_record(b"record1".to_vec()).await?;
        mux.wait_for_message_id(offset0).await?;

        // Second ingest (stream 1 = FAIL) — queuing succeeds
        let offset1 = mux.ingest_record(b"record2".to_vec()).await?;

        // But waiting for ack surfaces the sub-stream error
        let result = mux.wait_for_message_id(offset1).await;
        assert!(
            matches!(&result, Err(ZerobusError::StreamClosedError(status)) if status.code() == tonic::Code::PermissionDenied),
            "Expected permission-denied error from failed sub-stream: {result:?}"
        );

        // The sub-stream closed (non-retryable error), so the mux should be poisoned
        // and further ingest should preserve the same terminal error.
        assert!(
            mux.is_closed(),
            "Expected mux to be poisoned after sub-stream close"
        );
        let ingest_after = mux.ingest_record(b"record3".to_vec()).await;
        assert!(
            matches!(&ingest_after, Err(ZerobusError::StreamClosedError(status)) if status.code() == tonic::Code::PermissionDenied),
            "Expected preserved terminal error on ingest after poison: {ingest_after:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_poison_fails_sibling_callbacks_and_close_preserves_error(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();

        let (mock_server, server_url) = start_mock_server().await?;
        let healthy_ack = Arc::new(MockResponseGate::new());
        mock_server
            .inject_responses(
                TABLE_OK,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "callback_healthy".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::GatedRecordAck {
                        ack_up_to_offset: 0,
                        gate: Arc::clone(&healthy_ack),
                    },
                ],
            )
            .await;
        mock_server
            .inject_responses(
                TABLE_FAIL,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "callback_failed".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::Error {
                        status: tonic::Status::permission_denied("callback failure"),
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let healthy_callback = Arc::new(RecordingOffsetCallback::default());
        let failed_callback = Arc::new(RecordingOffsetCallback::default());
        let healthy = create_callback_stream(&sdk, TABLE_OK, healthy_callback.clone()).await?;
        let failed = create_callback_stream(&sdk, TABLE_FAIL, failed_callback.clone()).await?;
        let mut mux = MultiplexedStream::from_streams_for_testing(vec![healthy, failed]);

        let _healthy_id = mux.ingest_record(b"healthy-pending".to_vec()).await?;
        let failed_id = mux.ingest_record(b"failed-pending".to_vec()).await?;
        let wait_error = mux
            .wait_for_message_id(failed_id)
            .await
            .expect_err("the failed lane should poison the mux");
        assert!(
            matches!(&wait_error, ZerobusError::StreamClosedError(status) if status.code() == tonic::Code::PermissionDenied)
        );

        let close_error = mux
            .close()
            .await
            .expect_err("poisoned close must return the terminal error");
        healthy_ack.release();
        assert!(
            matches!(&close_error, ZerobusError::StreamClosedError(status) if status.code() == tonic::Code::PermissionDenied)
        );

        for callback in [&healthy_callback, &failed_callback] {
            let errors = callback.errors.lock().unwrap();
            assert_eq!(errors.len(), 1, "each accepted record needs one callback");
            assert_eq!(errors[0].0, 0);
            assert!(errors[0].1.contains("callback failure"), "{errors:?}");
        }

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
        let mux = MultiplexedStream::from_streams_for_testing(vec![s1]);

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
            matches!(&ingest_after, Err(ZerobusError::StreamClosedError(status)) if status.code() == tonic::Code::PermissionDenied),
            "Expected preserved terminal error after poison: {ingest_after:?}"
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
        let mux = MultiplexedStream::from_streams_for_testing(vec![s]);

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
        let mut mux = MultiplexedStream::from_streams_for_testing(vec![s]);

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
        let mut mux = MultiplexedStream::from_streams_for_testing(vec![s]);

        let _id = mux.ingest_record(b"will_fail".to_vec()).await?;
        // Trigger the failure and poisoning.
        let flush_error = mux.flush().await.expect_err("flush should fail");
        assert!(
            matches!(&flush_error, ZerobusError::StreamClosedError(status) if status.code() == tonic::Code::PermissionDenied)
        );
        assert!(mux.is_closed());

        let close_error = mux
            .close()
            .await
            .expect_err("close must not report durable success after lane failure");
        assert!(
            matches!(&close_error, ZerobusError::StreamClosedError(status) if status.code() == tonic::Code::PermissionDenied),
            "close should preserve the terminal server error: {close_error:?}"
        );

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
    /// Terminal cleanup moves records from every lane into recoverable storage
    /// before shutting the lanes down.
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
        let mut mux = MultiplexedStream::from_streams_for_testing(vec![s1, s2]);

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
        let mux = MultiplexedStream::from_streams_for_testing(vec![s1]);

        // Ingest queues successfully
        let _ = mux.ingest_record(b"data".to_vec()).await?;

        // Flush should fail because the sub-stream errors
        let result = mux.flush().await;
        assert!(result.is_err(), "Expected flush to fail");

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_uses_one_timeout_window_and_prefers_terminal_error(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();

        let (mock_server, server_url) = start_mock_server().await?;
        let healthy_ack = Arc::new(MockResponseGate::new());
        mock_server
            .inject_responses(
                TABLE_OK,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "flush_healthy".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::GatedRecordAck {
                        ack_up_to_offset: 0,
                        gate: Arc::clone(&healthy_ack),
                    },
                ],
            )
            .await;
        mock_server
            .inject_responses(
                TABLE_FAIL,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "flush_failed".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::Error {
                        status: tonic::Status::permission_denied("flush terminal failure"),
                        delay_ms: 1_000,
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let opts = TestOpts {
            flush_timeout_ms: Some(5_000),
            ..default_options()
        };
        let healthy = create_test_stream(&sdk, TABLE_OK, opts.clone()).await?;
        let failed = create_test_stream(&sdk, TABLE_FAIL, opts).await?;
        let mut mux = MultiplexedStream::from_streams_for_testing(vec![healthy, failed]);

        tokio::time::pause();
        mux.ingest_record(b"healthy-pending".to_vec()).await?;
        mux.ingest_record(b"failed-pending".to_vec()).await?;
        let mut flush = Box::pin(mux.flush());
        assert!(matches!(poll!(&mut flush), Poll::Pending));

        for _ in 0..100 {
            if mock_server.get_write_count().await == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(mock_server.get_write_count().await, 2);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(1_001)).await;
        for _ in 0..20 {
            if mux.is_closed() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            mux.is_closed(),
            "the terminal lane should fail during flush"
        );
        assert!(matches!(poll!(&mut flush), Poll::Pending));
        tokio::time::advance(Duration::from_millis(4_000)).await;

        let mut flush_result = None;
        for _ in 0..20 {
            match poll!(&mut flush) {
                Poll::Ready(result) => {
                    flush_result = Some(result);
                    break;
                }
                Poll::Pending => tokio::task::yield_now().await,
            }
        }
        drop(flush);
        tokio::time::resume();
        healthy_ack.release();

        let error = flush_result
            .expect("poison cleanup must not start a second flush timeout")
            .expect_err("the terminal lane should fail flush");
        assert!(
            matches!(&error, ZerobusError::StreamClosedError(status) if status.code() == tonic::Code::PermissionDenied && status.message() == "flush terminal failure"),
            "terminal error should take precedence over sibling timeout: {error:?}"
        );
        mux.close()
            .await
            .expect_err("close should preserve the flush failure");

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_ingest_waiting_for_capacity_fails_after_mux_poison(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting test_concurrent_ingest_waiting_for_capacity_fails_after_mux_poison");

        let (mock_server, server_url) = start_mock_server().await?;
        let first_ack = Arc::new(MockResponseGate::new());
        mock_server
            .inject_responses(
                TABLE_FAIL,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "poisoned".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::GatedRecordAck {
                        ack_up_to_offset: 0,
                        gate: Arc::clone(&first_ack),
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
        let mux = MultiplexedStream::from_streams_for_testing(vec![stream]);

        let first = mux.ingest_record(b"fills-capacity".to_vec()).await?;
        assert_eq!(first.stream_index(), 0);
        assert_eq!(first.sub_offset(), 0);

        const WAITERS: usize = 15;
        let mut waiters = (0..WAITERS)
            .map(|i| Box::pin(mux.ingest_record(format!("record-{i}").into_bytes())))
            .collect::<Vec<_>>();

        for waiter in &mut waiters {
            assert!(
                matches!(poll!(waiter.as_mut()), Poll::Pending),
                "Expected every concurrent ingest to wait for capacity"
            );
        }

        // Cross the one-second diagnostic timeout, then poll in reverse. A
        // cancelled-and-recreated semaphore acquisition would requeue in this
        // reverse order; a persistent acquisition retains waiter 0 at the
        // front of the original FIFO queue.
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(2)).await;
        for waiter in waiters.iter_mut().rev() {
            assert!(
                matches!(poll!(waiter.as_mut()), Poll::Pending),
                "Expected every concurrent ingest to remain blocked across the diagnostic timeout"
            );
        }
        tokio::time::resume();

        first_ack.release();
        let results =
            tokio::time::timeout(Duration::from_secs(5), futures::future::join_all(waiters))
                .await
                .expect("ingest tasks should finish after mux poison");

        let mut successes = Vec::new();
        let mut errors = 0;
        for (waiter_index, result) in results.into_iter().enumerate() {
            match result {
                Ok(message_id) => successes.push((waiter_index, message_id)),
                Err(ZerobusError::InvalidStateError(_))
                | Err(ZerobusError::StreamClosedError(_)) => errors += 1,
                Err(e) => panic!("unexpected ingest error: {e:?}"),
            }
        }

        assert_eq!(
            successes.len(),
            1,
            "Only one waiter should be admitted before poison"
        );
        assert_eq!(successes[0].0, 0, "The oldest waiter must win the permit");
        assert_eq!(successes[0].1.stream_index(), 0);
        assert_eq!(successes[0].1.sub_offset(), 1);
        assert_eq!(errors, WAITERS - 1);
        assert!(mux.is_closed(), "Mux should report the failed sub-stream");
        assert_eq!(mock_server.get_write_count().await, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_times_out_when_capacity_never_recovers(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();

        let (mock_server, server_url) = start_mock_server().await?;
        let stalled_ack = Arc::new(MockResponseGate::new());
        mock_server
            .inject_responses(
                TABLE_OK,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "stalled".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::GatedRecordAck {
                        ack_up_to_offset: 0,
                        gate: Arc::clone(&stalled_ack),
                    },
                ],
            )
            .await;

        let sdk = create_test_sdk(&server_url).await?;
        let stream = create_test_stream(
            &sdk,
            TABLE_OK,
            TestOpts {
                max_inflight_requests: 1,
                flush_timeout_ms: None,
            },
        )
        .await?;
        let mux = MultiplexedStream::from_streams_for_testing(vec![stream]);

        mux.ingest_record(b"fills-capacity".to_vec()).await?;
        let mut timed_ingest = Box::pin(mux.ingest_record(b"times-out".to_vec()));
        assert!(
            matches!(poll!(&mut timed_ingest), Poll::Pending),
            "Expected ingest to wait while the first acknowledgment is gated"
        );

        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(31)).await;
        let result = timed_ingest.await;
        tokio::time::resume();
        stalled_ack.release();

        let message = match result {
            Err(ZerobusError::ConnectionTimeout(message)) => message,
            other => panic!("expected capacity timeout, got {other:?}"),
        };
        assert!(message.contains("preferred sub-stream: 0"), "{message}");
        assert!(message.contains(TABLE_OK), "{message}");
        assert!(
            message.contains("configured timeout: 30000 ms"),
            "{message}"
        );
        assert!(
            message.contains("max_inflight_requests per stream: 1"),
            "{message}"
        );
        assert!(
            !mux.is_closed(),
            "capacity timeout should not poison the mux"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_failed_lane_rejects_ingest_and_poisons_mux(
    ) -> Result<(), Box<dyn std::error::Error>> {
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
                    MockResponse::Error {
                        status: tonic::Status::permission_denied("async close"),
                        delay_ms: 0,
                    },
                ],
            )
            .await;
        mock_server
            .inject_responses(
                TABLE_OK,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "healthy".to_string(),
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
        let failed = create_test_stream(&sdk, TABLE_FAIL, default_options()).await?;
        let healthy = create_test_stream(&sdk, TABLE_OK, default_options()).await?;

        let _ = failed
            .ingest_record_offset(b"trigger-failure".to_vec())
            .await?;
        let mux = MultiplexedStream::from_streams_for_testing(vec![failed, healthy]);

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !mux.is_closed() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("mux public is_closed should report the closed inner stream");

        let ingest_result = mux
            .ingest_record(b"must-not-reach-healthy-lane".to_vec())
            .await;
        assert!(
            matches!(&ingest_result, Err(ZerobusError::StreamClosedError(status)) if status.code() == tonic::Code::PermissionDenied && status.message() == "async close"),
            "a terminal lane must preserve its server error: {ingest_result:?}"
        );
        assert!(mux.is_closed(), "the terminal lane should poison the mux");
        assert_eq!(
            mock_server.get_write_count().await,
            1,
            "the healthy sibling must not accept records after another lane fails"
        );

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
        let mut mux = MultiplexedStream::from_streams_for_testing(vec![s1]);

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
        let mux = MultiplexedStream::from_streams_for_testing(vec![stream]);

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
        let mux = MultiplexedStream::from_streams_for_testing(vec![stream]);

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
