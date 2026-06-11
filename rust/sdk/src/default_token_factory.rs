use crate::{ZerobusError, ZerobusResult};

/// Default OAuth 2.0 token factory for Unity Catalog authentication.
///
/// This factory implements the OAuth 2.0 client credentials flow with Unity Catalog
/// authorization details to obtain access tokens for Zerobus API access.
pub struct DefaultTokenFactory {}

impl DefaultTokenFactory {
    /// Obtains an OAuth 2.0 access token for Zerobus API access.
    ///
    /// # Arguments
    ///
    /// * `uc_endpoint` - Unity Catalog endpoint URL
    /// * `table_name` - Full table name in format "catalog.schema.table"
    /// * `client_id` - OAuth client ID
    /// * `client_secret` - OAuth client secret
    /// * `workspace_id` - Databricks workspace ID
    ///
    /// # Returns
    ///
    /// Returns an access token string on success, or a `ZerobusError` on failure.
    ///
    /// # Errors
    ///
    /// * `InvalidUCTokenError` - If the token request fails or returns invalid data
    pub async fn get_token(
        uc_endpoint: &str,
        table_name: &str,
        client_id: &str,
        client_secret: &str,
        workspace_id: &str,
    ) -> ZerobusResult<String> {
        let (catalog, schema, table) = Self::parse_table_name(table_name)?;

        let uc_endpoint = uc_endpoint.to_string();
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
                "object_full_path": format!("{}.{}.{}", catalog, schema, table),
                "operations": ["zerobuswrite"]
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
            .map_err(Self::handle_http_error)?;

        if !resp.status().is_success() {
            let status_code = resp.status().as_u16();
            let error_body = resp
                .text()
                .await
                .unwrap_or_else(|_| "Failed to read error body".to_string());

            return Err(Self::classify_status_code(status_code, error_body));
        }

        let body: serde_json::Value = resp.json().await.map_err(|e| {
            ZerobusError::InvalidUCTokenError(format!("Parse failed with error: {}", e))
        })?;

        let token = body["access_token"]
            .as_str()
            .ok_or_else(|| ZerobusError::InvalidUCTokenError("access_token missing".to_string()))?
            .to_string();
        Ok(token)
    }

    /// Classifies HTTP status codes as retryable or non-retryable errors.
    ///
    /// # Arguments
    ///
    /// * `status_code` - HTTP status code (e.g., 404, 500)
    /// * `message` - Error message or response body
    ///
    /// # Returns
    ///
    /// * `TokenFetchError` for 5xx server errors (retryable)
    /// * `InvalidUCTokenError` for 4xx client errors (non-retryable)
    fn classify_status_code(status_code: u16, message: String) -> ZerobusError {
        if status_code >= 500 {
            ZerobusError::TokenFetchError(format!(
                "Unity catalog server error ({}): {}",
                status_code, message
            ))
        } else {
            ZerobusError::InvalidUCTokenError(format!(
                "Client error ({}): {}",
                status_code, message
            ))
        }
    }

    /// Helper to classify HTTP errors as retryable (TokenFetchError) or non-retryable.
    ///
    /// Retryable:
    /// - Network errors (timeout, connection failure)
    /// - Server errors (5xx status codes)
    ///
    /// Non-retryable:
    /// - Client errors (4xx status codes - bad credentials, invalid request, etc.)
    fn handle_http_error(error: reqwest::Error) -> ZerobusError {
        if error.is_timeout() || error.is_connect() {
            return ZerobusError::TokenFetchError(format!("Network error: {}", error));
        }
        if let Some(status) = error.status() {
            return Self::classify_status_code(status.as_u16(), error.to_string());
        }
        ZerobusError::InvalidUCTokenError(format!("Request failed: {}", error))
    }

    /// Parses a fully qualified table name into its components.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Full table name in format "catalog.schema.table"
    ///
    /// # Returns
    ///
    /// Returns a tuple of (catalog, schema, table) on success.
    ///
    /// # Errors
    ///
    /// * `InvalidTableName` - If the table name doesn't have exactly 3 non-empty parts.
    #[allow(clippy::result_large_err)]
    fn parse_table_name(table_name: &str) -> Result<(String, String, String), ZerobusError> {
        let parts: Vec<&str> = table_name.split('.').collect();

        if parts.len() != 3 {
            return Err(ZerobusError::InvalidTableName(format!(
                "Table name must have exactly 3 parts (catalog.schema.table), found {} parts",
                parts.len()
            )));
        }

        let catalog = parts[0];
        let schema = parts[1];
        let table = parts[2];

        if catalog.is_empty() {
            return Err(ZerobusError::InvalidTableName(
                "Catalog name cannot be empty".to_string(),
            ));
        }
        if schema.is_empty() {
            return Err(ZerobusError::InvalidTableName(
                "Schema name cannot be empty".to_string(),
            ));
        }
        if table.is_empty() {
            return Err(ZerobusError::InvalidTableName(
                "Table name cannot be empty".to_string(),
            ));
        }

        Ok((catalog.to_string(), schema.to_string(), table.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_table_name_valid() {
        let result = DefaultTokenFactory::parse_table_name("catalog_1.schema_2.table_3");
        assert!(result.is_ok());
        let (catalog, schema, table) = result.unwrap();
        assert_eq!(catalog, "catalog_1");
        assert_eq!(schema, "schema_2");
        assert_eq!(table, "table_3");
    }

    #[test]
    fn test_parse_table_name_invalid() {
        let invalid_cases = vec![
            ("catalog.schema.table.extra", "exactly 3 parts"),
            ("catalog.schema.table.with.dots", "exactly 3 parts"),
            ("catalog", "exactly 3 parts"),
            ("catalog.schema", "exactly 3 parts"),
            ("", "exactly 3 parts"),
            (".schema.table", "Catalog name cannot be empty"),
            ("catalog..table", "Schema name cannot be empty"),
            ("catalog.schema.", "Table name cannot be empty"),
            ("..", "Catalog name cannot be empty"),
            ("..table", "Catalog name cannot be empty"),
            ("catalog..", "Schema name cannot be empty"),
        ];

        for (input, expected_error) in invalid_cases {
            let result = DefaultTokenFactory::parse_table_name(input);
            assert!(
                result.is_err(),
                "Expected '{}' to be invalid, but it was parsed successfully",
                input
            );
            match result {
                Err(ZerobusError::InvalidTableName(msg)) => {
                    assert!(
                        msg.contains(expected_error),
                        "For input '{}', expected error to contain '{}', but got: '{}'",
                        input,
                        expected_error,
                        msg
                    );
                }
                _ => panic!("Expected InvalidTableName error for '{}'", input),
            }
        }
    }
}
