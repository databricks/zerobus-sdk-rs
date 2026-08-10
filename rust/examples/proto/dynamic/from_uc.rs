//! Dynamic protobuf ingestion with the schema fetched from Unity Catalog.
//!
//! Unlike `dynamic/single.rs`, which builds the descriptor in code, this fetches
//! it with `fetch_message_descriptor` and feeds it to the usual `.dynamic_proto(...)`.
//!
//! Throughput: ingest in a loop, then `flush()` once — never wait per record.

use std::error::Error;

use databricks_zerobus_ingest_sdk::{ProtoBytes, ZerobusSdk, ZerobusStream};

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
    // `unity_catalog_url` is required: it is where the schema is fetched from.
    let sdk_handle = ZerobusSdk::builder()
        .endpoint(SERVER_ENDPOINT)
        .unity_catalog_url(DATABRICKS_WORKSPACE_URL)
        .build()?;

    // Descriptor from live table metadata — no columns or `.proto` needed up front.
    let descriptor = sdk_handle
        .fetch_message_descriptor(TABLE_NAME, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
        .await?;

    // The fetched schema can be inspected when the columns are unknown to the program.
    println!("Fetched schema '{}':", descriptor.name());
    for field in descriptor.fields() {
        println!("  {} ({:?})", field.name(), field.kind());
    }

    let mut stream = sdk_handle
        .stream_builder()
        .table(TABLE_NAME)
        .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
        .dynamic_proto(descriptor)
        .max_inflight_requests(100)
        .build()
        .await?;

    ingest_records(&mut stream).await?;

    stream.close().await?;
    println!("Stream closed successfully");

    Ok(())
}

async fn ingest_records(stream: &mut ZerobusStream) -> Result<(), Box<dyn Error>> {
    // (customer_name, quantity, price) — adjust the field names below to your table.
    let orders = [
        ("Alice Smith", 2i32, 25.99f64),
        ("Bob Johnson", 1, 89.99),
        ("Carol Williams", 3, 45.00),
    ];

    for (i, (customer_name, quantity, price)) in orders.iter().enumerate() {
        // set()'s value must match the column's proto type (BIGINT -> i64, INT -> i32).
        let mut record = stream.new_record()?;
        record
            .set("id", i as i64)?
            .set("customer_name", *customer_name)?
            .set("quantity", *quantity)?
            .set("price", *price)?;

        // encode() enforces proto2 required fields; queues without waiting for the ack.
        let offset_id = stream
            .ingest_record_offset(ProtoBytes(record.encode()?))
            .await?;
        println!("Record {i} queued with offset ID: {offset_id}");
    }

    // Wait once for all pending acks — not after each ingest.
    stream.flush().await?;
    println!("All records acknowledged");

    Ok(())
}
