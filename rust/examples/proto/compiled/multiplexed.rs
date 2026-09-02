use std::error::Error;
use std::sync::Arc;

use databricks_zerobus_ingest_sdk::{AckCallback, MessageId, ProtoMessage, ZerobusSdk};
use prost::Message;
use prost_reflect::prost_types;

pub mod orders {
    include!("output/orders.rs");
}
use crate::orders::TableOrders;

const TABLE_NAME: &str = "<your_table_name>";
const DATABRICKS_CLIENT_ID: &str = "<your_databricks_client_id>";
const DATABRICKS_CLIENT_SECRET: &str = "<your_databricks_client_secret>";
const DATABRICKS_WORKSPACE_URL: &str = "https://<your-workspace>.cloud.databricks.com";
const SERVER_ENDPOINT: &str = "https://<your-shard-id>.zerobus.<region>.cloud.databricks.com";

struct AckLogger;

impl AckCallback<MessageId> for AckLogger {
    fn on_ack(&self, message_id: MessageId) {
        println!("Acknowledged {message_id}");
    }

    fn on_error(&self, message_id: MessageId, error_message: &str) {
        eprintln!("Error for {message_id}: {error_message}");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let descriptor = load_descriptor_proto(
        include_bytes!("output/orders.descriptor"),
        "orders.proto",
        "table_Orders",
    );
    let sdk = ZerobusSdk::builder()
        .endpoint(SERVER_ENDPOINT)
        .unity_catalog_url(DATABRICKS_WORKSPACE_URL)
        .build()?;

    let mut stream = sdk
        .stream_builder()
        .table(TABLE_NAME)
        .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
        .compiled_proto(descriptor)
        .max_inflight_requests(10_000)
        .multiplexed_ack_callback(Arc::new(AckLogger))
        .multiplexed(4)
        .build()
        .await?;

    for id in 0..10_000 {
        let record = TableOrders {
            id: Some(id),
            customer_name: Some("Alice Smith".to_string()),
            ..Default::default()
        };
        let _message_id = stream.ingest_record(ProtoMessage(record)).await?;

        if (id + 1) % 1_000 == 0 {
            stream.flush().await?;
        }
    }

    stream.flush().await?;
    stream.close().await?;
    Ok(())
}

fn load_descriptor_proto(
    descriptor_bytes: &[u8],
    file_name: &str,
    message_name: &str,
) -> prost_types::DescriptorProto {
    let file_descriptor_set = prost_types::FileDescriptorSet::decode(descriptor_bytes).unwrap();
    let file = file_descriptor_set
        .file
        .into_iter()
        .find(|file| file.name.as_deref() == Some(file_name))
        .unwrap();
    file.message_type
        .into_iter()
        .find(|message| message.name.as_deref() == Some(message_name))
        .unwrap()
}
