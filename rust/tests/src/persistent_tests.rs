//! **Prototype**: end-to-end tests for the persistent (resumable) stream path, run against the
//! stateful mock in `persistent_mock.rs`. Proves create → ingest → close → resume works: the
//! server reports the right `last_committed_offset` on reconnect and the client continues from
//! there.

mod persistent_mock;
mod utils;

use std::sync::Arc;
use std::time::Duration;

use databricks_zerobus_ingest_sdk::{NoTlsConfig, PersistentStream, ZerobusSdk};
use persistent_mock::start_persistent_mock;
use utils::{setup_tracing, TestHeadersProvider};

const TABLE_NAME: &str = "test_catalog.test_schema.test_table";

/// Builds an SDK pointed at the mock (plaintext, stub auth).
fn sdk_for(url: &str) -> ZerobusSdk {
    ZerobusSdk::builder()
        .endpoint(url.to_string())
        .unity_catalog_url("https://mock-uc.com")
        .tls_config(Arc::new(NoTlsConfig))
        .build()
        .expect("failed to build SDK")
}

/// Polls `last_durable_offset()` until it reaches `target`, or panics after a timeout.
async fn wait_for_durable(stream: &PersistentStream, target: i64) {
    for _ in 0..200 {
        if stream.last_durable_offset() == Some(target) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "timed out waiting for durable offset {target}; last = {:?}",
        stream.last_durable_offset()
    );
}

async fn open_persistent(sdk: &ZerobusSdk) -> PersistentStream {
    sdk.stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .json()
        .build_persistent()
        .await
        .expect("build_persistent failed")
}

async fn resume_persistent(sdk: &ZerobusSdk, stream_id: &str) -> PersistentStream {
    sdk.stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .json()
        .resume_persistent(stream_id.to_string())
        .await
        .expect("resume_persistent failed")
}

#[tokio::test]
async fn create_close_resume_round_trip() {
    // Test plan: open a persistent stream (no stream_id), ingest 3 records and confirm the server
    // acks up to offset 2, close, then resume by stream_id and verify the server reports
    // last_committed_offset == 2 and the next offset continues at 3.
    setup_tracing();
    let (state, url) = start_persistent_mock().await.expect("mock start failed");
    let sdk = sdk_for(&url);

    // 1. Create: fresh stream, no committed offset yet.
    let stream = open_persistent(&sdk).await;
    let stream_id = stream.stream_id().to_string();
    assert!(
        stream.last_committed_offset().is_none(),
        "a freshly created stream should have no committed offset"
    );

    // 2. Ingest offsets 0, 1, 2 and wait for the server to ack them.
    for id in 0..3 {
        stream
            .ingest(format!(r#"{{"id": {id}}}"#))
            .await
            .expect("ingest failed");
    }
    wait_for_durable(&stream, 2).await;

    // 3. Close from the client side.
    stream.close().await.expect("close failed");

    // The mock persisted the committed offset under the stream id.
    assert_eq!(state.committed_offset(&stream_id).await, Some(2));

    // 4. Resume by stream_id: the server reports where we left off.
    let resumed = resume_persistent(&sdk, &stream_id).await;
    assert_eq!(resumed.stream_id(), stream_id);
    assert_eq!(
        resumed.last_committed_offset(),
        Some(2),
        "resume should report the server's last committed offset"
    );

    // 5. Continue ingesting; offsets pick up at 3 and the server acks them.
    let next_offset = resumed
        .ingest(r#"{"id": 3}"#.to_string())
        .await
        .expect("ingest after resume failed");
    assert_eq!(next_offset, 3, "resumed stream should continue at offset 3");
    wait_for_durable(&resumed, 3).await;

    resumed.close().await.expect("close after resume failed");
    assert_eq!(state.committed_offset(&stream_id).await, Some(3));
}

#[tokio::test]
async fn fresh_create_assigns_distinct_stream_ids() {
    // Test plan: two separate create calls (no stream_id) must get distinct server-assigned ids,
    // confirming the create path mints a new stream each time.
    setup_tracing();
    let (_state, url) = start_persistent_mock().await.expect("mock start failed");
    let sdk = sdk_for(&url);

    let first = open_persistent(&sdk).await;
    let second = open_persistent(&sdk).await;
    assert_ne!(first.stream_id(), second.stream_id());

    first.close().await.expect("close failed");
    second.close().await.expect("close failed");
}
