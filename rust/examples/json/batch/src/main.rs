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
        .stream_builder(TABLE_NAME)
        .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
        .json()
        .max_inflight_requests(100)
        .build()
        .await?;

    ingest_with_offset_api(&mut stream).await?;
    ingest_with_future_api(&mut stream).await?;

    stream.close().await?;
    println!("Stream closed successfully");

    Ok(())
}

/// Recommended API: returns offset directly after queuing.
async fn ingest_with_offset_api(stream: &mut ZerobusStream) -> Result<(), Box<dyn Error>> {
    println!("=== Offset-based API (Recommended) ===");

    let now = chrono::Utc::now().timestamp();

    // 1. Auto-serializing: JsonValue - pass structs, SDK handles JSON conversion.
    let batch: Vec<JsonValue<Order>> = vec![
        JsonValue(Order {
            id: 1,
            customer_name: "Alice Smith".to_string(),
            product_name: "Wireless Mouse".to_string(),
            quantity: 2,
            price: 25.99,
            status: "pending".to_string(),
            created_at: now,
            updated_at: now,
        }),
        JsonValue(Order {
            id: 2,
            customer_name: "Bob Johnson".to_string(),
            product_name: "Mechanical Keyboard".to_string(),
            quantity: 1,
            price: 89.99,
            status: "shipped".to_string(),
            created_at: now,
            updated_at: now,
        }),
        JsonValue(Order {
            id: 3,
            customer_name: "Carol Williams".to_string(),
            product_name: "USB-C Hub".to_string(),
            quantity: 3,
            price: 45.00,
            status: "delivered".to_string(),
            created_at: now,
            updated_at: now,
        }),
    ];

    if let Some(offset_id) = stream.ingest_records_offset(batch).await? {
        println!(
            "[Auto-serializing] Batch of 3 records sent with offset ID: {}",
            offset_id
        );
        stream.wait_for_offset(offset_id).await?;
        println!(
            "[Auto-serializing] Batch acknowledged with offset ID: {}",
            offset_id
        );
    }

    // 2. Pre-serialized: JsonString - pass JSON strings with explicit wrapper.
    let batch: Vec<JsonString> = vec![
        JsonString(format!(
            r#"{{
                "id": 4,
                "customer_name": "David Green",
                "product_name": "Monitor",
                "quantity": 1,
                "price": 299.99,
                "status": "pending",
                "created_at": {},
                "updated_at": {}
            }}"#,
            now, now
        )),
        JsonString(format!(
            r#"{{
                "id": 5,
                "customer_name": "Emma White",
                "product_name": "Webcam",
                "quantity": 2,
                "price": 59.99,
                "status": "shipped",
                "created_at": {},
                "updated_at": {}
            }}"#,
            now, now
        )),
        JsonString(format!(
            r#"{{
                "id": 6,
                "customer_name": "Frank Brown",
                "product_name": "Mouse Pad",
                "quantity": 5,
                "price": 15.99,
                "status": "delivered",
                "created_at": {},
                "updated_at": {}
            }}"#,
            now, now
        )),
    ];

    if let Some(offset_id) = stream.ingest_records_offset(batch).await? {
        println!(
            "[Pre-serialized] Batch of 3 records sent with offset ID: {}",
            offset_id
        );
        stream.wait_for_offset(offset_id).await?;
        println!(
            "[Pre-serialized] Batch acknowledged with offset ID: {}",
            offset_id
        );
    }

    // 3. Backward-compatible: raw String - no wrapper needed, works the same as JsonString.
    let batch: Vec<String> = vec![
        format!(
            r#"{{
                "id": 7,
                "customer_name": "Grace Lee",
                "product_name": "Headphones",
                "quantity": 1,
                "price": 149.99,
                "status": "pending",
                "created_at": {},
                "updated_at": {}
            }}"#,
            now, now
        ),
        format!(
            r#"{{
                "id": 8,
                "customer_name": "Henry Wilson",
                "product_name": "Desk Lamp",
                "quantity": 2,
                "price": 35.99,
                "status": "shipped",
                "created_at": {},
                "updated_at": {}
            }}"#,
            now, now
        ),
        format!(
            r#"{{
                "id": 9,
                "customer_name": "Ivy Chen",
                "product_name": "Cable Organizer",
                "quantity": 4,
                "price": 12.99,
                "status": "delivered",
                "created_at": {},
                "updated_at": {}
            }}"#,
            now, now
        ),
    ];

    if let Some(offset_id) = stream.ingest_records_offset(batch).await? {
        println!(
            "[Backward-compatible] Batch of 3 records sent with offset ID: {}",
            offset_id
        );
        stream.wait_for_offset(offset_id).await?;
        println!(
            "[Backward-compatible] Batch acknowledged with offset ID: {}",
            offset_id
        );
    }

    Ok(())
}

/// Deprecated API: returns future that resolves to offset.
#[allow(deprecated)]
async fn ingest_with_future_api(stream: &mut ZerobusStream) -> Result<(), Box<dyn Error>> {
    println!("=== Future-based API (Deprecated) ===");

    let now = chrono::Utc::now().timestamp();

    // 1. Auto-serializing: JsonValue - pass structs, SDK handles JSON conversion.
    let batch: Vec<JsonValue<Order>> = vec![
        JsonValue(Order {
            id: 10,
            customer_name: "Jack Taylor".to_string(),
            product_name: "Laptop Stand".to_string(),
            quantity: 1,
            price: 89.99,
            status: "pending".to_string(),
            created_at: now,
            updated_at: now,
        }),
        JsonValue(Order {
            id: 11,
            customer_name: "Kate Martinez".to_string(),
            product_name: "Screen Protector".to_string(),
            quantity: 3,
            price: 19.99,
            status: "shipped".to_string(),
            created_at: now,
            updated_at: now,
        }),
        JsonValue(Order {
            id: 12,
            customer_name: "Leo Anderson".to_string(),
            product_name: "Phone Charger".to_string(),
            quantity: 2,
            price: 24.99,
            status: "delivered".to_string(),
            created_at: now,
            updated_at: now,
        }),
    ];

    let ack_future = stream.ingest_records(batch).await?;
    if let Some(offset_id) = ack_future.await? {
        println!(
            "[Auto-serializing] Batch acknowledged with offset ID: {}",
            offset_id
        );
    }

    // 2. Pre-serialized: JsonString - pass JSON strings with explicit wrapper.
    let batch: Vec<JsonString> = vec![
        JsonString(format!(
            r#"{{
                "id": 13,
                "customer_name": "Mia Thompson",
                "product_name": "USB Drive",
                "quantity": 5,
                "price": 9.99,
                "status": "pending",
                "created_at": {},
                "updated_at": {}
            }}"#,
            now, now
        )),
        JsonString(format!(
            r#"{{
                "id": 14,
                "customer_name": "Noah Garcia",
                "product_name": "HDMI Cable",
                "quantity": 2,
                "price": 14.99,
                "status": "shipped",
                "created_at": {},
                "updated_at": {}
            }}"#,
            now, now
        )),
        JsonString(format!(
            r#"{{
                "id": 15,
                "customer_name": "Olivia Davis",
                "product_name": "Webcam Cover",
                "quantity": 10,
                "price": 4.99,
                "status": "delivered",
                "created_at": {},
                "updated_at": {}
            }}"#,
            now, now
        )),
    ];

    let ack_future = stream.ingest_records(batch).await?;
    if let Some(offset_id) = ack_future.await? {
        println!(
            "[Pre-serialized] Batch acknowledged with offset ID: {}",
            offset_id
        );
    }

    // 3. Backward-compatible: raw String - no wrapper needed, works the same as JsonString.
    let batch: Vec<String> = vec![
        format!(
            r#"{{
                "id": 16,
                "customer_name": "Peter Robinson",
                "product_name": "Keyboard Wrist Rest",
                "quantity": 1,
                "price": 22.99,
                "status": "pending",
                "created_at": {},
                "updated_at": {}
            }}"#,
            now, now
        ),
        format!(
            r#"{{
                "id": 17,
                "customer_name": "Quinn Harris",
                "product_name": "Monitor Arm",
                "quantity": 1,
                "price": 79.99,
                "status": "shipped",
                "created_at": {},
                "updated_at": {}
            }}"#,
            now, now
        ),
        format!(
            r#"{{
                "id": 18,
                "customer_name": "Rachel Clark",
                "product_name": "Desk Mat",
                "quantity": 1,
                "price": 29.99,
                "status": "delivered",
                "created_at": {},
                "updated_at": {}
            }}"#,
            now, now
        ),
    ];

    let ack_future = stream.ingest_records(batch).await?;
    if let Some(offset_id) = ack_future.await? {
        println!(
            "[Backward-compatible] Batch acknowledged with offset ID: {}",
            offset_id
        );
    }

    Ok(())
}
