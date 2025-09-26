use crate::{TokenFactory, ZerobusError, ZerobusResult};

pub struct DefaultTokenFactory {
    pub uc_endpoint: String,
    pub table_name: String,
    pub client_id: String,
    pub client_secret: String,
    pub workspace_id: String,
}

impl TokenFactory for DefaultTokenFactory {
    fn get_token(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ZerobusResult<String>> + Send + '_>>
    {
        let (catalog, schema, table) = self.parse_table_name().unwrap();

        let uc_endpoint = self.uc_endpoint.clone();
        let databricks_client_id = self.client_id.clone();
        let databricks_client_secret = self.client_secret.clone();
        let workspace_id = self.workspace_id.clone();

        Box::pin(async move {
            let authorization_details = serde_json::json!([
                {
                    "type": "unity_catalog_privileges",
                    "privileges": ["USE CATALOG"],
                    "object_type": "CATALOG",
                    "object_full_path": catalog
                },
                {
                    "type": "unity_catalog_privileges",
                    "privileges": ["USE SCHEMA"],
                    "object_type": "SCHEMA",
                    "object_full_path": format!("{}.{}", catalog, schema)
                },
                {
                    "type": "unity_catalog_privileges",
                    "privileges": ["SELECT", "MODIFY"],
                    "object_type": "TABLE",
                    "object_full_path": format!("{}.{}.{}", catalog, schema, table)
                }
            ]);

            let client = reqwest::Client::new();

            let params = [
                ("grant_type", "client_credentials".to_string()),
                ("scope", "all-apis".to_string()),
                (
                    "resource",
                    format!(
                        "api://databricks/workspaces/{}/zerobusDirectWriteApi",
                        workspace_id
                    )
                    .to_string(),
                ),
                ("authorization_details", authorization_details.to_string()),
            ];

            let token_endpoint = format!("{}/oidc/v1/token", uc_endpoint);
            let resp = client
                .post(&token_endpoint)
                .basic_auth(databricks_client_id, Some(databricks_client_secret))
                .form(&params)
                .send()
                .await
                .map_err(|e| {
                    ZerobusError::InvalidUCTokenError(format!("Request failed with error: {}", e))
                })?;

            if !resp.status().is_success() {
                let status = resp.status();
                let error_body = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "Failed to read error body".to_string());
                return Err(ZerobusError::InvalidUCTokenError(format!(
                    "Unexpected status: {}. Response body: {}",
                    status.as_str(),
                    error_body
                )));
            }

            let body: serde_json::Value = resp.json().await.map_err(|e| {
                ZerobusError::InvalidUCTokenError(format!("Parse failed with error: {}", e))
            })?;

            let token = body["access_token"]
                .as_str()
                .ok_or_else(|| {
                    ZerobusError::InvalidUCTokenError("access_token missing".to_string())
                })?
                .to_string();
            Ok(token)
        })
    }
}

impl DefaultTokenFactory {
    fn parse_table_name(&self) -> Result<(String, String, String), ZerobusError> {
        let mut parts = self.table_name.splitn(3, '.');

        let catalog = parts.next().ok_or_else(|| {
            ZerobusError::InvalidUCTokenError("Missing catalog in table name".to_string())
        })?;
        let schema = parts.next().ok_or_else(|| {
            ZerobusError::InvalidUCTokenError("Missing schema in table name".to_string())
        })?;
        let table = parts.next().ok_or_else(|| {
            ZerobusError::InvalidUCTokenError("Missing table in table name".to_string())
        })?;

        Ok((catalog.to_string(), schema.to_string(), table.to_string()))
    }
}
