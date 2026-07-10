//! Dynamic protobuf batch ingestion: build the schema at runtime, no compiled `.proto`.
//!
//! Like `single.rs`, but ingests many records in one call with
//! `ingest_records_offset()` (all-or-nothing). Each `DynamicRecord` is encoded
//! up front — `encode()` enforces proto2 required fields — and the resulting
//! bytes are sent as a single batch.

use std::error::Error;

use databricks_zerobus_ingest_sdk::schema::{descriptor_from_uc_columns, UcColumn};
use databricks_zerobus_ingest_sdk::{
    message_descriptor, ProtoBytes, ZerobusResult, ZerobusSdk, ZerobusStream,
};

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
    // Build the descriptor at runtime instead of from a `.proto` file. A column's
    // proto field number is its `position + 1`, so positions must be distinct.
    let columns = vec![
        col("id", "BIGINT", 0),
        col("customer_name", "STRING", 1),
        col("quantity", "INT", 2),
        col("price", "DOUBLE", 3),
    ];
    let descriptor_proto = descriptor_from_uc_columns(&columns, "table_Orders")?;
    let descriptor = message_descriptor(&descriptor_proto)?;

    let sdk_handle = ZerobusSdk::builder()
        .endpoint(SERVER_ENDPOINT)
        .unity_catalog_url(DATABRICKS_WORKSPACE_URL)
        .build()?;

    let mut stream = sdk_handle
        .stream_builder()
        .table(TABLE_NAME)
        .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
        .dynamic_proto(descriptor)
        .max_inflight_requests(100)
        .build()
        .await?;

    ingest_dynamic_batch(&mut stream).await?;

    stream.close().await?;
    println!("Stream closed successfully");

    Ok(())
}

async fn ingest_dynamic_batch(stream: &mut ZerobusStream) -> Result<(), Box<dyn Error>> {
    // (customer_name, quantity, price)
    let orders = [
        ("Alice Smith", 2i32, 25.99f64),
        ("Bob Johnson", 1, 89.99),
        ("Carol Williams", 3, 45.00),
    ];

    // Build and encode each record; encode() enforces proto2 required fields.
    // Collecting into a ZerobusResult surfaces the first bad record as an error.
    let batch: Vec<ProtoBytes> = orders
        .iter()
        .enumerate()
        .map(|(i, (customer_name, quantity, price))| {
            let mut record = stream.new_record()?;
            record
                .set("id", i as i64)?
                .set("customer_name", *customer_name)?
                .set("quantity", *quantity)?
                .set("price", *price)?;
            Ok(ProtoBytes(record.encode()?))
        })
        .collect::<ZerobusResult<_>>()?;

    // The whole batch is queued in one call; a single offset covers it.
    if let Some(offset) = stream.ingest_records_offset(batch).await? {
        println!("Batch queued with offset ID: {offset}");
    }

    // Wait once for the batch's acknowledgment.
    stream.flush().await?;
    println!("Batch acknowledged");

    Ok(())
}

/// A non-nullable top-level column of the given UC type at a 0-based position.
fn col(name: &str, type_name: &str, position: i32) -> UcColumn {
    UcColumn {
        name: name.to_string(),
        type_name: type_name.to_string(),
        type_text: type_name.to_string(),
        nullable: false,
        type_json: String::new(),
        position,
    }
}
