//! Single-record protobuf ingestion, demonstrating each record wrapper type.
//!
//! Note on throughput: `ingest_record_offset()` returns as soon as the record is queued.
//! This example ingests several records and then calls `flush()` ONCE to confirm them.
//! Do not call `wait_for_offset()` after every record in a real workload — that forces a
//! server round-trip per record and collapses throughput. For high volume, prefer the
//! batch API in `batch.rs`.

use std::error::Error;
use std::fs;

use prost::Message;
use prost_reflect::prost_types;

use databricks_zerobus_ingest_sdk::{ProtoBytes, ProtoMessage, ZerobusSdk, ZerobusStream};

pub mod orders {
    include!("output/orders.rs");
}
use crate::orders::TableOrders;

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
    let descriptor_proto =
        load_descriptor_proto("output/orders.descriptor", "orders.proto", "table_Orders");
    let sdk_handle = ZerobusSdk::builder()
        .endpoint(SERVER_ENDPOINT)
        .unity_catalog_url(DATABRICKS_WORKSPACE_URL)
        .build()?;

    let mut stream = sdk_handle
        .stream_builder()
        .table(TABLE_NAME)
        .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
        .compiled_proto(descriptor_proto)
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

    // 1. Auto-encoding: ProtoMessage - pass message directly, SDK handles encoding.
    let order = TableOrders {
        id: Some(1),
        customer_name: Some("Alice Smith".to_string()),
        product_name: Some("Wireless Mouse".to_string()),
        quantity: Some(2),
        price: Some(25.99),
        status: Some("pending".to_string()),
        created_at: Some(now),
        updated_at: Some(now),
    };

    // Queue the record; the call returns immediately without waiting for the ack.
    let offset_id = stream.ingest_record_offset(ProtoMessage(order)).await?;
    println!("[Auto-encoding] Record queued with offset ID: {}", offset_id);

    // 2. Pre-encoded: ProtoBytes - pass bytes with explicit wrapper.
    let order = TableOrders {
        id: Some(2),
        customer_name: Some("Bob Johnson".to_string()),
        product_name: Some("Mechanical Keyboard".to_string()),
        quantity: Some(1),
        price: Some(89.99),
        status: Some("shipped".to_string()),
        created_at: Some(now),
        updated_at: Some(now),
    };
    let bytes = order.encode_to_vec();

    let offset_id = stream.ingest_record_offset(ProtoBytes(bytes)).await?;
    println!("[Pre-encoded] Record queued with offset ID: {}", offset_id);

    // 3. Backward-compatible: raw Vec<u8> - no wrapper needed, works the same as ProtoBytes.
    let order = TableOrders {
        id: Some(3),
        customer_name: Some("Carol Williams".to_string()),
        product_name: Some("USB-C Hub".to_string()),
        quantity: Some(3),
        price: Some(45.00),
        status: Some("delivered".to_string()),
        created_at: Some(now),
        updated_at: Some(now),
    };
    let raw_bytes = order.encode_to_vec();

    let offset_id = stream.ingest_record_offset(raw_bytes).await?;
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

fn load_descriptor_proto(
    path: &str,
    file_name: &str,
    message_name: &str,
) -> prost_types::DescriptorProto {
    let descriptor_bytes = fs::read(path).expect("Failed to read proto descriptor file");
    let file_descriptor_set =
        prost_types::FileDescriptorSet::decode(descriptor_bytes.as_ref()).unwrap();

    let file_descriptor_proto = file_descriptor_set
        .file
        .into_iter()
        .find(|f| f.name.as_deref() == Some(file_name))
        .unwrap();

    file_descriptor_proto
        .message_type
        .into_iter()
        .find(|m| m.name.as_deref() == Some(message_name))
        .unwrap()
}
