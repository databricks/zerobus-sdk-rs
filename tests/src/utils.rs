use async_trait::async_trait;
use databricks_zerobus_ingest_sdk::HeadersProvider;
use databricks_zerobus_ingest_sdk::ZerobusResult;
use std::collections::HashMap;

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
