//! Single-record JSON ingestion, demonstrating each record wrapper type.
//!
//! Note on throughput: `ingest_record_offset()` returns as soon as the record is queued.
//! This example ingests several records and then calls `flush()` ONCE to confirm them.
//! Do not call `wait_for_offset()` after every record in a real workload — that forces a
//! server round-trip per record and collapses throughput. For high volume, prefer the
//! batch API in `batch.rs`.

use std::error::Error;

use databricks_zerobus_ingest_sdk::{JsonString, JsonValue, ZerobusSdk, ZerobusStream};
use serde::Serialize;

/// Order struct that can be automatically serialized to JSON using JsonValue wrapper.
#[derive(Serialize, Clone)]
struct Order {
    id: i32,
    customer_name: String,
    product_name: String,
    quantity: i32,
    price: f64,
    status: String,
    created_at: i64,
    updated_at: i64,
}

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
    let sdk_handle = ZerobusSdk::builder()
        .endpoint(SERVER_ENDPOINT)
        .unity_catalog_url(DATABRICKS_WORKSPACE_URL)
        .build()?;

    let mut stream = sdk_handle
        .stream_builder()
        .table(TABLE_NAME)
        .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
        .json()
        .max_inflight_requests(100)
        .build()
        .await?;

    ingest_with_offset_api(&mut stream).await?;

    stream.close().await?;
    println!("Stream closed successfully");

    Ok(())
}

/// Recommended API: returns offset directly after queuing.
async fn ingest_with_offset_api(stream: &mut ZerobusStream) -> Result<(), Box<dyn Error>> {
    println!("=== Offset-based API (Recommended) ===");

    // Delta TIMESTAMP is int64 microseconds since epoch UTC.
    let now = chrono::Utc::now().timestamp_micros();

    // 1. Auto-serializing: JsonValue - pass struct, SDK handles JSON conversion.
    let order = Order {
        id: 1,
        customer_name: "Alice Smith".to_string(),
        product_name: "Wireless Mouse".to_string(),
        quantity: 2,
        price: 25.99,
        status: "pending".to_string(),
        created_at: now,
        updated_at: now,
    };

    // Queue the record; the call returns immediately without waiting for the ack.
    let offset_id = stream.ingest_record_offset(JsonValue(order)).await?;
    println!(
        "[Auto-serializing] Record queued with offset ID: {}",
        offset_id
    );

    // 2. Pre-serialized: JsonString - pass JSON string with explicit wrapper.
    let json_string = format!(
        r#"{{
            "id": 2,
            "customer_name": "Bob Johnson",
            "product_name": "Mechanical Keyboard",
            "quantity": 1,
            "price": 89.99,
            "status": "shipped",
            "created_at": {},
            "updated_at": {}
        }}"#,
        now, now
    );

    let offset_id = stream.ingest_record_offset(JsonString(json_string)).await?;
    println!(
        "[Pre-serialized] Record queued with offset ID: {}",
        offset_id
    );

    // 3. Backward-compatible: raw String - no wrapper needed, works the same as JsonString.
    let raw_json = format!(
        r#"{{
            "id": 3,
            "customer_name": "Carol Williams",
            "product_name": "USB-C Hub",
            "quantity": 3,
            "price": 45.00,
            "status": "delivered",
            "created_at": {},
            "updated_at": {}
        }}"#,
        now, now
    );

    let offset_id = stream.ingest_record_offset(raw_json).await?;
    println!(
        "[Backward-compatible] Record queued with offset ID: {}",
        offset_id
    );

    // Confirm all queued records at once. flush() waits for every pending acknowledgment;
    // this is the right place to wait, not after each individual ingest above.
    stream.flush().await?;
    println!("All records acknowledged");

    Ok(())
}
