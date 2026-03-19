use std::error::Error;
use std::fs;

use prost::Message;
use prost_reflect::prost_types;

use databricks_zerobus_ingest_sdk::{ProtoBytes, ProtoMessage, ZerobusSdk, ZerobusStream};

pub mod orders {
    include!("../output/orders.rs");
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
        .stream_builder(TABLE_NAME)
        .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
        .compiled_proto(descriptor_proto)
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

    // 1. Auto-encoding: ProtoMessage - pass messages directly, SDK handles encoding.
    let batch: Vec<ProtoMessage<TableOrders>> = vec![
        ProtoMessage(TableOrders {
            id: Some(1),
            customer_name: Some("Alice Smith".to_string()),
            product_name: Some("Wireless Mouse".to_string()),
            quantity: Some(2),
            price: Some(25.99),
            status: Some("pending".to_string()),
            created_at: Some(now),
            updated_at: Some(now),
        }),
        ProtoMessage(TableOrders {
            id: Some(2),
            customer_name: Some("Bob Johnson".to_string()),
            product_name: Some("Mechanical Keyboard".to_string()),
            quantity: Some(1),
            price: Some(89.99),
            status: Some("shipped".to_string()),
            created_at: Some(now),
            updated_at: Some(now),
        }),
        ProtoMessage(TableOrders {
            id: Some(3),
            customer_name: Some("Carol Williams".to_string()),
            product_name: Some("USB-C Hub".to_string()),
            quantity: Some(3),
            price: Some(45.00),
            status: Some("delivered".to_string()),
            created_at: Some(now),
            updated_at: Some(now),
        }),
    ];

    if let Some(offset_id) = stream.ingest_records_offset(batch).await? {
        println!(
            "[Auto-encoding] Batch of 3 records sent with offset ID: {}",
            offset_id
        );
        stream.wait_for_offset(offset_id).await?;
        println!(
            "[Auto-encoding] Batch acknowledged with offset ID: {}",
            offset_id
        );
    }

    // 2. Pre-encoded: ProtoBytes - pass bytes with explicit wrapper.
    let batch: Vec<ProtoBytes> = vec![
        ProtoBytes(
            TableOrders {
                id: Some(4),
                customer_name: Some("David Green".to_string()),
                product_name: Some("Monitor".to_string()),
                quantity: Some(1),
                price: Some(299.99),
                status: Some("pending".to_string()),
                created_at: Some(now),
                updated_at: Some(now),
            }
            .encode_to_vec(),
        ),
        ProtoBytes(
            TableOrders {
                id: Some(5),
                customer_name: Some("Emma White".to_string()),
                product_name: Some("Webcam".to_string()),
                quantity: Some(2),
                price: Some(59.99),
                status: Some("shipped".to_string()),
                created_at: Some(now),
                updated_at: Some(now),
            }
            .encode_to_vec(),
        ),
        ProtoBytes(
            TableOrders {
                id: Some(6),
                customer_name: Some("Frank Brown".to_string()),
                product_name: Some("Mouse Pad".to_string()),
                quantity: Some(5),
                price: Some(15.99),
                status: Some("delivered".to_string()),
                created_at: Some(now),
                updated_at: Some(now),
            }
            .encode_to_vec(),
        ),
    ];

    if let Some(offset_id) = stream.ingest_records_offset(batch).await? {
        println!(
            "[Pre-encoded] Batch of 3 records sent with offset ID: {}",
            offset_id
        );
        stream.wait_for_offset(offset_id).await?;
        println!(
            "[Pre-encoded] Batch acknowledged with offset ID: {}",
            offset_id
        );
    }

    // 3. Backward-compatible: raw Vec<u8> - no wrapper needed, works the same as ProtoBytes.
    let batch: Vec<Vec<u8>> = vec![
        TableOrders {
            id: Some(7),
            customer_name: Some("Grace Lee".to_string()),
            product_name: Some("Headphones".to_string()),
            quantity: Some(1),
            price: Some(149.99),
            status: Some("pending".to_string()),
            created_at: Some(now),
            updated_at: Some(now),
        }
        .encode_to_vec(),
        TableOrders {
            id: Some(8),
            customer_name: Some("Henry Wilson".to_string()),
            product_name: Some("Desk Lamp".to_string()),
            quantity: Some(2),
            price: Some(35.99),
            status: Some("shipped".to_string()),
            created_at: Some(now),
            updated_at: Some(now),
        }
        .encode_to_vec(),
        TableOrders {
            id: Some(9),
            customer_name: Some("Ivy Chen".to_string()),
            product_name: Some("Cable Organizer".to_string()),
            quantity: Some(4),
            price: Some(12.99),
            status: Some("delivered".to_string()),
            created_at: Some(now),
            updated_at: Some(now),
        }
        .encode_to_vec(),
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

    // 1. Auto-encoding: ProtoMessage - pass messages directly, SDK handles encoding.
    let batch: Vec<ProtoMessage<TableOrders>> = vec![
        ProtoMessage(TableOrders {
            id: Some(10),
            customer_name: Some("Jack Taylor".to_string()),
            product_name: Some("Laptop Stand".to_string()),
            quantity: Some(1),
            price: Some(89.99),
            status: Some("pending".to_string()),
            created_at: Some(now),
            updated_at: Some(now),
        }),
        ProtoMessage(TableOrders {
            id: Some(11),
            customer_name: Some("Kate Martinez".to_string()),
            product_name: Some("Screen Protector".to_string()),
            quantity: Some(3),
            price: Some(19.99),
            status: Some("shipped".to_string()),
            created_at: Some(now),
            updated_at: Some(now),
        }),
        ProtoMessage(TableOrders {
            id: Some(12),
            customer_name: Some("Leo Anderson".to_string()),
            product_name: Some("Phone Charger".to_string()),
            quantity: Some(2),
            price: Some(24.99),
            status: Some("delivered".to_string()),
            created_at: Some(now),
            updated_at: Some(now),
        }),
    ];

    let ack_future = stream.ingest_records(batch).await?;
    if let Some(offset_id) = ack_future.await? {
        println!(
            "[Auto-encoding] Batch acknowledged with offset ID: {}",
            offset_id
        );
    }

    // 2. Pre-encoded: ProtoBytes - pass bytes with explicit wrapper.
    let batch: Vec<ProtoBytes> = vec![
        ProtoBytes(
            TableOrders {
                id: Some(13),
                customer_name: Some("Mia Thompson".to_string()),
                product_name: Some("USB Drive".to_string()),
                quantity: Some(5),
                price: Some(9.99),
                status: Some("pending".to_string()),
                created_at: Some(now),
                updated_at: Some(now),
            }
            .encode_to_vec(),
        ),
        ProtoBytes(
            TableOrders {
                id: Some(14),
                customer_name: Some("Noah Garcia".to_string()),
                product_name: Some("HDMI Cable".to_string()),
                quantity: Some(2),
                price: Some(14.99),
                status: Some("shipped".to_string()),
                created_at: Some(now),
                updated_at: Some(now),
            }
            .encode_to_vec(),
        ),
        ProtoBytes(
            TableOrders {
                id: Some(15),
                customer_name: Some("Olivia Davis".to_string()),
                product_name: Some("Webcam Cover".to_string()),
                quantity: Some(10),
                price: Some(4.99),
                status: Some("delivered".to_string()),
                created_at: Some(now),
                updated_at: Some(now),
            }
            .encode_to_vec(),
        ),
    ];

    let ack_future = stream.ingest_records(batch).await?;
    if let Some(offset_id) = ack_future.await? {
        println!(
            "[Pre-encoded] Batch acknowledged with offset ID: {}",
            offset_id
        );
    }

    // 3. Backward-compatible: raw Vec<u8> - no wrapper needed, works the same as ProtoBytes.
    let batch: Vec<Vec<u8>> = vec![
        TableOrders {
            id: Some(16),
            customer_name: Some("Peter Robinson".to_string()),
            product_name: Some("Keyboard Wrist Rest".to_string()),
            quantity: Some(1),
            price: Some(22.99),
            status: Some("pending".to_string()),
            created_at: Some(now),
            updated_at: Some(now),
        }
        .encode_to_vec(),
        TableOrders {
            id: Some(17),
            customer_name: Some("Quinn Harris".to_string()),
            product_name: Some("Monitor Arm".to_string()),
            quantity: Some(1),
            price: Some(79.99),
            status: Some("shipped".to_string()),
            created_at: Some(now),
            updated_at: Some(now),
        }
        .encode_to_vec(),
        TableOrders {
            id: Some(18),
            customer_name: Some("Rachel Clark".to_string()),
            product_name: Some("Desk Mat".to_string()),
            quantity: Some(1),
            price: Some(29.99),
            status: Some("delivered".to_string()),
            created_at: Some(now),
            updated_at: Some(now),
        }
        .encode_to_vec(),
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
