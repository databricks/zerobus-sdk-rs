use async_trait::async_trait;
use databricks_zerobus_ingest_sdk::HeadersProvider;
use databricks_zerobus_ingest_sdk::ZerobusResult;
use prost_reflect::prost_types;
use std::collections::HashMap;
use std::sync::Once;
use tracing_subscriber::EnvFilter;

static SETUP: Once = Once::new();

#[derive(Default)]
pub struct TestHeadersProvider {}

#[async_trait]
impl HeadersProvider for TestHeadersProvider {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        let mut headers = HashMap::new();
        headers.insert("authorization", "Bearer test_token".to_string());
        headers.insert("x-databricks-zerobus-table-name", "test_table".to_string());
        Ok(headers)
    }
}

/// Helper function to create a simple descriptor proto for testing.
pub fn create_test_descriptor_proto() -> Option<prost_types::DescriptorProto> {
    Some(prost_types::DescriptorProto {
        name: Some("TestMessage".to_string()),
        field: vec![
            prost_types::FieldDescriptorProto {
                name: Some("id".to_string()),
                number: Some(1),
                r#type: Some(prost_types::field_descriptor_proto::Type::Int64 as i32),
                ..Default::default()
            },
            prost_types::FieldDescriptorProto {
                name: Some("message".to_string()),
                number: Some(2),
                r#type: Some(prost_types::field_descriptor_proto::Type::String as i32),
                ..Default::default()
            },
        ],
        ..Default::default()
    })
}

/// Setup tracing for tests.
pub fn setup_tracing() {
    SETUP.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_writer(std::io::stdout)
            .with_env_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
            )
            .try_init()
            .ok();
    });
}
