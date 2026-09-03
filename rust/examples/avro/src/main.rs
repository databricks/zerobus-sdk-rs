//! Avro ingestion (Beta) with pre-encoded `AvroBytes` records.
//!
//! Ingest in a loop, then `flush()` once — never wait per record.

use std::error::Error;

use databricks_zerobus_ingest_sdk::{AvroBytes, ZerobusSdk, ZerobusStream};

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

async fn ingest(stream: &mut ZerobusStream) -> Result<(), Box<dyn Error>> {
    // Raw Avro datums encoded against AVRO_SCHEMA (placeholder bytes here).
    let records: Vec<Vec<u8>> = vec![vec![0x02, 0x0a], vec![0x04, 0x0b]];

    for record in records {
        // Queues only; the send and ack happen in the background.
        stream.ingest_record_offset(AvroBytes(record)).await?;
    }

    // Wait once for every pending ack.
    stream.flush().await?;
    Ok(())
}
