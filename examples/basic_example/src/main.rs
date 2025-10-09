use std::error::Error;
use std::fs;

use prost::Message;
use prost_reflect::prost_types;

use zerobus::{StreamConfigurationOptions, TableProperties, ZerobusSdk};
pub mod orders {
    include!("../output/orders.rs");
}
use crate::orders::TableOrders;

// Change constants to match your data.
const DATABRICKS_WORKSPACE_URL: &str = "<your_workspace_url>";
const TABLE_NAME: &str = "<your_table_name>";
const DATABRICKS_CLIENT_ID: &str = "<your_databricks_client_id>";
const DATABRICKS_CLIENT_SECRET: &str = "<your_databricks_client_secret>";
const SERVER_ENDPOINT: &str = "<your_server_endpoint>";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let descriptor_proto =
        load_descriptor_proto("output/orders.descriptor", "orders.proto", "table_Orders"); //<your_descriptor_file>, <your_proto_file>, <your_proto_message_name>
    let table_properties = TableProperties {
        table_name: TABLE_NAME.to_string(),
        descriptor_proto,
    };
    let stream_configuration_options = StreamConfigurationOptions {
        max_inflight_records: 100,
        ..Default::default()
    };
    let sdk_handle = ZerobusSdk::new(
        SERVER_ENDPOINT.to_string(),
        DATABRICKS_WORKSPACE_URL.to_string(),
    )?;

    let mut stream = sdk_handle
        .create_stream(
            table_properties.clone(),
            DATABRICKS_CLIENT_ID.to_string(),
            DATABRICKS_CLIENT_SECRET.to_string(),
            Some(stream_configuration_options),
        )
        .await
        .expect("Failed to create a stream.");

    // Change the values to match your data.
    let ack_future = stream
        .ingest_record(
            TableOrders {
                id: Some(1),
                customer_name: Some("Alice Smith".to_string()),
                product_name: Some("Wireless Mouse".to_string()),
                quantity: Some(2),
                price: Some(25.99),
                status: Some("pending".to_string()),
                created_at: Some(chrono::Utc::now().timestamp()),
                updated_at: Some(chrono::Utc::now().timestamp()),
            }
            .encode_to_vec(),
        )
        .await
        .unwrap();

    let _ack = ack_future.await.unwrap();
    println!("Record acknowledged with offset Id: 0");
    let close_future = stream.close();
    close_future.await?;
    println!("Stream closed successfully");
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
        .find(|f| f.name.as_ref().map(|n| n.as_str()) == Some(file_name))
        .unwrap();

    file_descriptor_proto
        .message_type
        .into_iter()
        .find(|m| m.name.as_ref().map(|n| n.as_str()) == Some(message_name))
        .unwrap()
}
