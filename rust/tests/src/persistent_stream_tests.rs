mod mock_grpc;
mod utils;

use std::sync::Arc;
use std::time::Duration;

use databricks_zerobus_ingest_sdk::{NoTlsConfig, ZerobusSdk};
use mock_grpc::start_mock_server;
use prost_types::DescriptorProto;
use tokio::time::timeout;
use utils::TestHeadersProvider;

const TABLE_NAME: &str = "test_catalog.test_schema.test_table";

fn sdk(endpoint: String) -> ZerobusSdk {
    ZerobusSdk::builder()
        .endpoint(endpoint)
        .unity_catalog_url("https://mock-uc.com")
        .tls_config(Arc::new(NoTlsConfig))
        .build()
        .expect("build SDK")
}

#[tokio::test]
async fn create_ingest_flush_and_resume_continues_offsets() {
    let (server, endpoint) = start_mock_server().await.expect("start mock");
    let sdk = sdk(endpoint);

    let mut stream = sdk
        .persistent_stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .json()
        .recovery(false)
        .build()
        .await
        .expect("create persistent stream");
    let stream_id = stream.stream_id().expect("stream id").to_owned();

    assert_eq!(
        stream
            .ingest_record_offset(r#"{"id":1}"#.to_string())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        stream
            .ingest_record_offset(r#"{"id":2}"#.to_string())
            .await
            .unwrap(),
        1
    );
    stream.flush().await.expect("flush");
    assert_eq!(
        server.persistent_committed_offset(&stream_id).await,
        Some(1)
    );
    stream.close().await.expect("close");

    let mut resumed = sdk
        .persistent_stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .json()
        .recovery(false)
        .resume(stream_id.clone())
        .await
        .expect("resume persistent stream");
    assert_eq!(resumed.last_committed_offset(), Some(1));
    assert_eq!(
        resumed
            .ingest_record_offset(r#"{"id":3}"#.to_string())
            .await
            .unwrap(),
        2
    );
    resumed.flush().await.expect("flush resumed stream");
    resumed.close().await.expect("close resumed stream");
    assert_eq!(
        server.persistent_committed_offset(&stream_id).await,
        Some(2)
    );
}

#[tokio::test]
async fn resume_unknown_stream_fails() {
    let (_server, endpoint) = start_mock_server().await.expect("start mock");
    let result = sdk(endpoint)
        .persistent_stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .json()
        .recovery(false)
        .resume("missing-stream")
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn resume_without_committed_records_starts_at_offset_zero() {
    let (_server, endpoint) = start_mock_server().await.expect("start mock");
    let sdk = sdk(endpoint);

    let mut created = sdk
        .persistent_stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .json()
        .recovery(false)
        .build()
        .await
        .expect("create persistent stream");
    let stream_id = created.stream_id().expect("stream id").to_owned();
    assert_eq!(created.last_committed_offset(), None);
    created.close().await.expect("close created stream");

    let mut resumed = sdk
        .persistent_stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .json()
        .recovery(false)
        .resume(stream_id)
        .await
        .expect("resume empty persistent stream");
    assert_eq!(resumed.last_committed_offset(), None);
    assert_eq!(
        resumed
            .ingest_record_offset(r#"{"id":1}"#.to_string())
            .await
            .expect("ingest after resume"),
        0
    );
    resumed.flush().await.expect("flush resumed stream");
    resumed.close().await.expect("close resumed stream");
}

#[tokio::test]
async fn batch_uses_one_offset_and_acknowledges_every_record() {
    let (server, endpoint) = start_mock_server().await.expect("start mock");
    let mut stream = sdk(endpoint)
        .persistent_stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .json()
        .recovery(false)
        .build()
        .await
        .expect("create persistent stream");
    let stream_id = stream.stream_id().expect("stream id").to_owned();

    let offset = stream
        .ingest_records_offset([
            r#"{"id":1}"#.to_string(),
            r#"{"id":2}"#.to_string(),
            r#"{"id":3}"#.to_string(),
        ])
        .await
        .expect("ingest batch")
        .expect("non-empty batch offset");
    assert_eq!(offset, 0);
    stream
        .wait_for_offset(offset)
        .await
        .expect("wait for batch");
    assert_eq!(server.get_write_count().await, 3);
    assert_eq!(
        server.persistent_committed_offset(&stream_id).await,
        Some(0)
    );
    stream.close().await.expect("close stream");
}

#[tokio::test]
async fn persistent_stream_offsets_are_isolated_by_stream_id() {
    let (server, endpoint) = start_mock_server().await.expect("start mock");
    let sdk = sdk(endpoint);

    let mut first = sdk
        .persistent_stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .json()
        .recovery(false)
        .build()
        .await
        .expect("create first stream");
    let mut second = sdk
        .persistent_stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .json()
        .recovery(false)
        .build()
        .await
        .expect("create second stream");
    let first_id = first.stream_id().expect("first stream id").to_owned();
    let second_id = second.stream_id().expect("second stream id").to_owned();
    assert_ne!(first_id, second_id);

    assert_eq!(
        first
            .ingest_record_offset(r#"{"id":1}"#.to_string())
            .await
            .expect("ingest first record"),
        0
    );
    assert_eq!(
        first
            .ingest_record_offset(r#"{"id":2}"#.to_string())
            .await
            .expect("ingest second record"),
        1
    );
    assert_eq!(
        second
            .ingest_record_offset(r#"{"id":3}"#.to_string())
            .await
            .expect("ingest other stream record"),
        0
    );
    first.flush().await.expect("flush first stream");
    second.flush().await.expect("flush second stream");

    assert_eq!(server.persistent_committed_offset(&first_id).await, Some(1));
    assert_eq!(
        server.persistent_committed_offset(&second_id).await,
        Some(0)
    );
    first.close().await.expect("close first stream");
    second.close().await.expect("close second stream");
}

#[tokio::test]
async fn create_without_stream_id_fails() {
    let (server, endpoint) = start_mock_server().await.expect("start mock");
    server.omit_next_persistent_stream_id().await;

    let result = sdk(endpoint)
        .persistent_stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .json()
        .recovery(false)
        .build()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn unexpected_open_response_fails() {
    let (server, endpoint) = start_mock_server().await.expect("start mock");
    server.send_unexpected_next_persistent_open_response().await;

    let result = sdk(endpoint)
        .persistent_stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .json()
        .recovery(false)
        .build()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn lost_ack_is_reconciled_from_resume_watermark() {
    let (server, endpoint) = start_mock_server().await.expect("start mock");
    let mut stream = sdk(endpoint)
        .persistent_stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .json()
        .recovery(true)
        .recovery_backoff_ms(0)
        .recovery_timeout_ms(1_000)
        .recovery_retries(2)
        .flush_timeout_ms(5_000)
        .build()
        .await
        .expect("create persistent stream");
    let stream_id = stream.stream_id().expect("stream id").to_owned();
    server.fail_next_persistent_ack_after_commit();

    let offset = stream
        .ingest_record_offset(r#"{"id":1}"#.to_string())
        .await
        .expect("queue record");
    timeout(Duration::from_secs(5), stream.wait_for_offset(offset))
        .await
        .expect("recovery timed out")
        .expect("lost ACK should reconcile during recovery");

    assert_eq!(server.get_write_count().await, 1);
    assert_eq!(
        server.persistent_committed_offset(&stream_id).await,
        Some(0)
    );
    assert_eq!(
        stream
            .ingest_record_offset(r#"{"id":2}"#.to_string())
            .await
            .expect("ingest after recovery"),
        1
    );
    stream.flush().await.expect("flush after recovery");
    stream.close().await.expect("close recovered stream");
}

#[tokio::test]
async fn lost_ack_fails_when_recovery_is_disabled() {
    let (server, endpoint) = start_mock_server().await.expect("start mock");
    let mut stream = sdk(endpoint)
        .persistent_stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .json()
        .recovery(false)
        .flush_timeout_ms(5_000)
        .build()
        .await
        .expect("create persistent stream");
    server.fail_next_persistent_ack_after_commit();

    let offset = stream
        .ingest_record_offset(r#"{"id":1}"#.to_string())
        .await
        .expect("queue record");
    let result = timeout(Duration::from_secs(5), stream.wait_for_offset(offset))
        .await
        .expect("failure propagation timed out");
    assert!(result.is_err());
    assert!(stream.is_closed());
    stream.close().await.expect("close remains idempotent");
}

#[tokio::test]
async fn resume_with_mismatched_record_type_fails() {
    let (_server, endpoint) = start_mock_server().await.expect("start mock");
    let sdk = sdk(endpoint);
    let mut created = sdk
        .persistent_stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .json()
        .recovery(false)
        .build()
        .await
        .expect("create JSON persistent stream");
    let stream_id = created.stream_id().expect("stream id").to_owned();
    created.close().await.expect("close created stream");

    let result = sdk
        .persistent_stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .compiled_proto(DescriptorProto::default())
        .recovery(false)
        .resume(stream_id)
        .await;
    assert!(result.is_err());
}
