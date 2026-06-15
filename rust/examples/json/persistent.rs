//! **Prototype**: persistent (resumable) stream example.
//!
//! Demonstrates the new persistent-stream path:
//!   1. open a NEW persistent stream and fetch its server-assigned `stream_id`,
//!   2. ingest a few records and close the stream,
//!   3. RESUME by that `stream_id` and keep ingesting.
//!
//! Both step 1 and step 3 go through the same `EphemeralStream` RPC — once sending `CreateStream`
//! *without* a `stream_id` (create) and once *with* it (resume).
//!
//! NOTE: server-side resume (offset persistence + dedup) is not implemented yet, so against
//! today's server `resume_persistent` opens a fresh stream rather than truly resuming. This
//! example exercises the client path and wire contract.
//!
//! Run with: `cargo run -p rust-examples-json --example json_persistent`

use std::error::Error;
use std::ops::Range;

use databricks_zerobus_ingest_sdk::{PersistentStream, ZerobusSdk};

// Change constants to match your data.
const TABLE_NAME: &str = "<your_table_name>";
const DATABRICKS_CLIENT_ID: &str = "<your_databricks_client_id>";
const DATABRICKS_CLIENT_SECRET: &str = "<your_databricks_client_secret>";

// Uncomment the appropriate lines for your cloud.

// For AWS:
const DATABRICKS_WORKSPACE_URL: &str = "https://<your-workspace>.cloud.databricks.com";
const SERVER_ENDPOINT: &str = "https://<your-shard-id>.zerobus.<region>.cloud.databricks.com";

// For Azure:
// const DATABRICKS_WORKSPACE_URL: &str = "https://<your-workspace>.azuredatabricks.net";
// const SERVER_ENDPOINT: &str = "https://<your-shard-id>.zerobus.<region>.azuredatabricks.net";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let sdk = ZerobusSdk::builder()
        .endpoint(SERVER_ENDPOINT)
        .unity_catalog_url(DATABRICKS_WORKSPACE_URL)
        .build()?;

    // 1. Open a NEW persistent stream (CreateStream without a stream_id).
    let stream = sdk
        .stream_builder()
        .table(TABLE_NAME)
        .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
        .json()
        .build_persistent()
        .await?;

    // The server assigns the id; keep it so we can resume later.
    let stream_id = stream.stream_id().to_string();
    println!("opened persistent stream: stream_id={stream_id}");

    ingest_some(&stream, 0..3).await?;
    println!(
        "last durable offset before close: {:?}",
        stream.last_durable_offset()
    );

    // 2. Close the stream from the client side.
    stream.close().await?;
    println!("closed stream {stream_id}");

    // 3. Resume by stream_id (CreateStream WITH the stream_id) and keep going.
    let resumed = sdk
        .stream_builder()
        .table(TABLE_NAME)
        .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
        .json()
        .resume_persistent(stream_id)
        .await?;

    println!(
        "resumed stream {}: server last_committed_offset={:?}",
        resumed.stream_id(),
        resumed.last_committed_offset()
    );

    ingest_some(&resumed, 3..6).await?;
    resumed.close().await?;
    println!("done");

    Ok(())
}

/// Ingests a few JSON records, printing the offset assigned to each.
async fn ingest_some(stream: &PersistentStream, ids: Range<i32>) -> Result<(), Box<dyn Error>> {
    // Delta TIMESTAMP is int64 microseconds since epoch UTC.
    let now = chrono::Utc::now().timestamp_micros();
    for id in ids {
        let record = format!(
            r#"{{"id": {id}, "customer_name": "Example {id}", "product_name": "Widget", "quantity": 1, "price": 9.99, "status": "pending", "created_at": {now}, "updated_at": {now}}}"#
        );
        let offset = stream.ingest(record).await?;
        println!("ingested id={id} at offset={offset}");
    }
    Ok(())
}
