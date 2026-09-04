//! Avro ingestion (Beta) with both pre-encoded `AvroBytes` and object-based `AvroRecord` patterns.
//!
//! Demonstrates:
//! - `AvroRecord`: Serialize Rust structs/maps to Avro (recommended for most use cases)
//! - `AvroBytes`: Pre-encoded Avro binary data (for special cases)
//!
//! Ingest in a loop, then `flush()` once — never wait per record.

use serde::Serialize;
use std::error::Error;

use databricks_zerobus_ingest_sdk::{AvroBytes, AvroRecord, ZerobusSdk, ZerobusStream};

// Change constants to match your data.
const TABLE_NAME: &str = "<your_table_name>";
const DATABRICKS_CLIENT_ID: &str = "<your_databricks_client_id>";
const DATABRICKS_CLIENT_SECRET: &str = "<your_databricks_client_secret>";
const DATABRICKS_WORKSPACE_URL: &str = "https://<your-workspace>.cloud.databricks.com";
const SERVER_ENDPOINT: &str = "https://<your-shard-id>.zerobus.<region>.cloud.databricks.com";

// The Avro writer schema (JSON), declared once at stream creation.
const AVRO_SCHEMA: &str = r#"{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "customer_name", "type": "string"}
  ]
}"#;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let sdk = ZerobusSdk::builder()
        .endpoint(SERVER_ENDPOINT)
        .unity_catalog_url(DATABRICKS_WORKSPACE_URL)
        .build()?;

    let mut stream = sdk
        .stream_builder()
        .table(TABLE_NAME)
        .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
        .avro(AVRO_SCHEMA)
        .build()
        .await?;

    ingest(&mut stream).await?;

    stream.close().await?;
    Ok(())
}

/// Rust struct that will be serialized to Avro.
/// Field names and types must match the schema.
#[derive(Serialize, Clone)]
struct Order {
    id: i64,
    customer_name: String,
}

async fn ingest(stream: &mut ZerobusStream) -> Result<(), Box<dyn Error>> {
    println!("Ingesting via AvroRecord (object serialization)...");
    ingest_avro_records(stream).await?;

    println!("Ingesting via AvroBytes (pre-encoded)...");
    ingest_avro_bytes(stream).await?;

    Ok(())
}

async fn ingest_avro_records(stream: &mut ZerobusStream) -> Result<(), Box<dyn Error>> {
    // Create Order objects - the SDK will serialize them to Avro.
    let orders = vec![
        Order {
            id: 1,
            customer_name: "Alice".to_string(),
        },
        Order {
            id: 2,
            customer_name: "Bob".to_string(),
        },
    ];

    // Queue records (send and ack happen in the background).
    for order in orders {
        stream
            .ingest_record_offset(AvroRecord(order))
            .await?;
    }

    // Wait once for every pending ack.
    stream.flush().await?;
    println!("  {} records acknowledged", 2);
    Ok(())
}

async fn ingest_avro_bytes(stream: &mut ZerobusStream) -> Result<(), Box<dyn Error>> {
    // Raw Avro datums pre-encoded against AVRO_SCHEMA (placeholder bytes here).
    // In production, use a library like apache_avro to encode.
    let records: Vec<Vec<u8>> = vec![vec![0x02, 0x0a], vec![0x04, 0x0b]];

    // Queue records (send and ack happen in the background).
    for record in records {
        stream.ingest_record_offset(AvroBytes(record)).await?;
    }

    // Wait once for every pending ack.
    stream.flush().await?;
    println!("  {} records acknowledged", 2);
    Ok(())
}
