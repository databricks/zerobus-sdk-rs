//! Dynamic protobuf ingestion: no compiled `.proto`, no generated Rust types.
//!
//! Two descriptor sources are shown:
//!  1. `.proto_from_uc()` — the SDK fetches the table's schema from Unity Catalog
//!     at stream creation and derives the protobuf descriptor (the default,
//!     used in `main`).
//!  2. `TableDescriptorBuilder` — build the descriptor in code when you already
//!     know the schema and have no Unity Catalog metadata (see
//!     `ingest_with_in_code_descriptor`).
//!
//! In both cases records are supplied as JSON and encoded to protobuf bytes by
//! the stream's `encoder()`, then ingested on the proto path.

use std::error::Error;

use databricks_zerobus_ingest_sdk::schema::TableDescriptorBuilder;
use databricks_zerobus_ingest_sdk::{ZerobusSdk, ZerobusStream};

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

    // No `.proto` file and no generated types: the descriptor is fetched from
    // Unity Catalog at `build()` time.
    let mut stream = sdk
        .stream_builder()
        .table(TABLE_NAME)
        .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
        .proto_from_uc()
        .max_inflight_requests(100)
        .build()
        .await?;

    ingest_dynamic_records(&mut stream).await?;

    stream.close().await?;
    println!("Stream closed successfully");

    Ok(())
}

/// Encode JSON records against the UC-derived descriptor and ingest them.
async fn ingest_dynamic_records(stream: &mut ZerobusStream) -> Result<(), Box<dyn Error>> {
    // Build the encoder once from the stream's descriptor and reuse it.
    let encoder = stream.encoder()?;

    // Delta TIMESTAMP is int64 microseconds since epoch UTC.
    let now = chrono::Utc::now().timestamp_micros();

    // 1. Encode a JSON string. The `Vec<u8>` it returns is protobuf wire bytes,
    //    exactly what a compiled `.proto` type's `encode_to_vec()` would yield.
    let record = format!(
        r#"{{
            "id": 1,
            "customer_name": "Alice Smith",
            "product_name": "Wireless Mouse",
            "quantity": 2,
            "price": 25.99,
            "status": "pending",
            "created_at": {now},
            "updated_at": {now}
        }}"#
    );
    let offset = stream
        .ingest_record_offset(encoder.encode(&record)?)
        .await?;
    println!("[JSON string] Record sent with offset ID: {offset}");
    stream.wait_for_offset(offset).await?;
    println!("[JSON string] Record acknowledged with offset ID: {offset}");

    // 2. Encode a `serde_json::Value` built with the `json!` macro.
    let value = serde_json::json!({
        "id": 2,
        "customer_name": "Bob Johnson",
        "product_name": "Mechanical Keyboard",
        "quantity": 1,
        "price": 89.99,
        "status": "shipped",
        "created_at": now,
        "updated_at": now,
    });
    let offset = stream
        .ingest_record_offset(encoder.encode_value(&value)?)
        .await?;
    println!("[JSON value] Record sent with offset ID: {offset}");
    stream.wait_for_offset(offset).await?;
    println!("[JSON value] Record acknowledged with offset ID: {offset}");

    Ok(())
}

/// Alternative descriptor source: build it in code with `TableDescriptorBuilder`
/// (no Unity Catalog round-trip), open the stream with `.compiled_proto(...)`,
/// then encode records the same way.
///
/// Not called by `main`; shown as a reference for the no-UC path.
#[allow(dead_code)]
async fn ingest_with_in_code_descriptor(sdk: &ZerobusSdk) -> Result<(), Box<dyn Error>> {
    let descriptor = TableDescriptorBuilder::new("table_Orders")
        .column("id", "BIGINT", false)
        .column("customer_name", "STRING", true)
        .column("product_name", "STRING", true)
        .column("quantity", "INT", true)
        .column("price", "DOUBLE", true)
        .column("status", "STRING", true)
        .column("created_at", "TIMESTAMP", true)
        .column("updated_at", "TIMESTAMP", true)
        .build()?;

    let mut stream = sdk
        .stream_builder()
        .table(TABLE_NAME)
        .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
        .compiled_proto(descriptor)
        .build()
        .await?;

    let encoder = stream.encoder()?;
    let offset = stream
        .ingest_record_offset(encoder.encode(r#"{"id": 100, "customer_name": "Carol"}"#)?)
        .await?;
    stream.wait_for_offset(offset).await?;
    stream.close().await?;
    Ok(())
}
