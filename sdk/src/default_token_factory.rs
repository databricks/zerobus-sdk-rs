use crate::{ZerobusError, ZerobusResult};

pub struct DefaultTokenFactory {}

impl DefaultTokenFactory {
    pub async fn get_token(
        uc_endpoint: &str,
        table_name: &str,
        client_id: &str,
        client_secret: &str,
        workspace_id: &str,
    ) -> ZerobusResult<String> {
        #[cfg(any(test, feature = "testing"))]
        {
            if table_name.split('.').count() != 3 {
                return Err(ZerobusError::InvalidUCTokenError(
                    "Missing schema in table name".to_string(),
                ));
            }
            return Ok("mock-test-token".to_string());
        }

        #[cfg(not(test))]
        {
            if table_name.split('.').count() != 3 {
                return Err(ZerobusError::InvalidUCTokenError(
                    "Missing schema in table name".to_string(),
                ));
            }

            let (catalog, schema, table) = Self::parse_table_name(table_name).unwrap();

            let databricks_client_id = client_id.to_string();
            let databricks_client_secret = client_secret.to_string();
            let workspace_id = workspace_id.to_string();

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
            let response = client
                .post(token_endpoint)
                .basic_auth(databricks_client_id, Some(databricks_client_secret))
                .form(&params)
                .send()
                .await
                .map_err(|e| {
                    ZerobusError::InvalidUCTokenError(format!("Request failed with error: {}", e))
                })?;

            if !response.status().is_success() {
                return Err(ZerobusError::InvalidUCTokenError(format!(
                    "Request failed with status: {}",
                    response.status()
                )));
            }

            let token_response: serde_json::Value = response.json().await.map_err(|e| {
                ZerobusError::InvalidUCTokenError(format!("Failed to parse response: {}", e))
            })?;

            let token = token_response["access_token"]
                .as_str()
                .ok_or_else(|| {
                    ZerobusError::InvalidUCTokenError(
                        "Missing access_token in response".to_string(),
                    )
                })?
                .to_string();

            Ok(token)
        }
    }

    fn parse_table_name(table_name: &str) -> Result<(String, String, String), ZerobusError> {
        let mut parts = table_name.splitn(3, '.');

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
