//! Dynamic protobuf ingestion: build the schema at runtime, no compiled `.proto`.
//!
//! Builds a `DescriptorProto` in code with `schema::descriptor_from_uc_columns`,
//! selects it with `.dynamic_proto(...)`, and fills records field-by-field with
//! `DynamicRecord` — no generated Rust structs.
//!
//! Throughput: ingest in a loop, then `flush()` once. Waiting per record forces a
//! server round-trip each time and collapses throughput.

use std::error::Error;

use databricks_zerobus_ingest_sdk::schema::{descriptor_from_uc_columns, UcColumn};
use databricks_zerobus_ingest_sdk::{message_descriptor, ProtoBytes, ZerobusSdk, ZerobusStream};

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

    ingest_dynamic_records(&mut stream).await?;

    stream.close().await?;
    println!("Stream closed successfully");

    Ok(())
}

async fn ingest_dynamic_records(stream: &mut ZerobusStream) -> Result<(), Box<dyn Error>> {
    // (customer_name, quantity, price)
    let orders = [
        ("Alice Smith", 2i32, 25.99f64),
        ("Bob Johnson", 1, 89.99),
        ("Carol Williams", 3, 45.00),
    ];

    for (i, (customer_name, quantity, price)) in orders.iter().enumerate() {
        // set()'s value must match the field's proto type (BIGINT -> i64, INT -> i32).
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

/// A non-nullable top-level column of the given UC type at a 0-based position.
fn col(name: &str, type_name: &str, position: i32) -> UcColumn {
    UcColumn {
        name: name.to_string(),
        type_name: type_name.to_string(),
        type_text: type_name.to_string(),
        type_json: String::new(),
        nullable: false,
        position,
    }
}
