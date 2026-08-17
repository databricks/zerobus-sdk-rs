use std::error::Error;
use std::sync::Arc;

use arrow_array::{
    Float64Array, Int32Array, LargeStringArray, RecordBatch, TimestampMicrosecondArray,
};
use arrow_ipc::CompressionType;
use databricks_zerobus_ingest_sdk::{ArrowSchema, DataType, Field, TimeUnit, ZerobusSdk};

/// One row of the `orders` table.
#[derive(Clone)]
struct Order {
    id: i32,
    customer_name: &'static str,
    product_name: &'static str,
    quantity: i32,
    price: f64,
    status: &'static str,
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

/// Builds the Arrow schema for the `orders` table, matching the canonical
/// Arrow schema the Databricks Arrow Flight server derives from a Delta
/// table: Delta `STRING` → `LargeUtf8`, Delta `TIMESTAMP` → `Timestamp(
/// Microsecond, Some("UTC"))`, all columns nullable unless declared
/// `NOT NULL` in the Delta DDL.
fn orders_schema() -> Arc<ArrowSchema> {
    let utc_micros = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("customer_name", DataType::LargeUtf8, true),
        Field::new("product_name", DataType::LargeUtf8, true),
        Field::new("quantity", DataType::Int32, true),
        Field::new("price", DataType::Float64, true),
        Field::new("status", DataType::LargeUtf8, true),
        Field::new("created_at", utc_micros.clone(), true),
        Field::new("updated_at", utc_micros, true),
    ]))
}

/// Builds a `RecordBatch` containing one row per `Order`.
fn build_orders_batch(schema: &Arc<ArrowSchema>, orders: &[Order]) -> RecordBatch {
    let ids: Vec<i32> = orders.iter().map(|o| o.id).collect();
    let customer_names: Vec<&str> = orders.iter().map(|o| o.customer_name).collect();
    let product_names: Vec<&str> = orders.iter().map(|o| o.product_name).collect();
    let quantities: Vec<i32> = orders.iter().map(|o| o.quantity).collect();
    let prices: Vec<f64> = orders.iter().map(|o| o.price).collect();
    let statuses: Vec<&str> = orders.iter().map(|o| o.status).collect();
    let created_at: Vec<i64> = orders.iter().map(|o| o.created_at).collect();
    let updated_at: Vec<i64> = orders.iter().map(|o| o.updated_at).collect();

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(LargeStringArray::from(customer_names)),
            Arc::new(LargeStringArray::from(product_names)),
            Arc::new(Int32Array::from(quantities)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(LargeStringArray::from(statuses)),
            Arc::new(TimestampMicrosecondArray::from(created_at).with_timezone("UTC")),
            Arc::new(TimestampMicrosecondArray::from(updated_at).with_timezone("UTC")),
        ],
    )
    .expect("Failed to build RecordBatch")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let schema = orders_schema();

    let sdk_handle = ZerobusSdk::builder()
        .endpoint(SERVER_ENDPOINT)
        .unity_catalog_url(DATABRICKS_WORKSPACE_URL)
        .build()?;

    // Optional IPC compression. Trades client CPU for fewer bytes on the wire —
    // enable only when network bandwidth limits throughput. `LZ4_FRAME` is fast
    // with a modest ratio; `ZSTD` compresses more at higher CPU cost.
    let mut stream = sdk_handle
        .stream_builder()
        .table(TABLE_NAME)
        .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
        .arrow(schema.clone())
        .ipc_compression(Some(CompressionType::ZSTD))
        .max_inflight_batches(100)
        .build_arrow()
        .await?;

    // Use application-sized batches to amortize Arrow encoding and Flight RPC overhead.
    const NUM_BATCHES: usize = 10;
    const ROWS_PER_BATCH: usize = 10_000;

    let now = chrono::Utc::now().timestamp_micros();

    for i in 0..NUM_BATCHES {
        let orders: Vec<Order> = (0..ROWS_PER_BATCH)
            .map(|j| Order {
                id: (i * ROWS_PER_BATCH + j) as i32,
                customer_name: "Customer",
                product_name: "Product",
                quantity: 1,
                price: 19.99,
                status: "pending",
                created_at: now,
                updated_at: now,
            })
            .collect();

        let batch = build_orders_batch(&schema, &orders);
        stream.ingest_batch(batch).await?;
    }

    stream.flush().await?;
    println!("Flushed all in-flight batches");

    stream.close().await?;
    println!("Stream closed successfully");

    Ok(())
}
