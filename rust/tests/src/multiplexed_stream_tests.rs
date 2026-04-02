mod mock_grpc;
mod utils;

use std::sync::Arc;

use databricks_zerobus_ingest_sdk::{
    MessageId, MultiplexedStream, NoTlsConfig, StreamConfigurationOptions, TableProperties,
    ZerobusError, ZerobusSdk,
};
use mock_grpc::{start_mock_server, MockResponse};
use tracing::info;
use utils::{create_test_descriptor_proto, setup_tracing, TestHeadersProvider};

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
    options: StreamConfigurationOptions,
) -> Result<databricks_zerobus_ingest_sdk::ZerobusStream, ZerobusError> {
    let table_properties = TableProperties {
        table_name: table_name.to_string(),
        descriptor_proto: create_test_descriptor_proto(),
    };
    sdk.create_stream_with_headers_provider(
        table_properties,
        Arc::new(TestHeadersProvider::default()),
        Some(options),
    )
    .await
}

fn default_options() -> StreamConfigurationOptions {
    StreamConfigurationOptions {
        max_inflight_requests: 100,
        recovery: false,
        ..Default::default()
    }
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
    async fn test_wait_for_unknown_message_id_returns_error() -> Result<(), Box<dyn std::error::Error>>
    {
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
