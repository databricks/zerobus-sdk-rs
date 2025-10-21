use anyhow::{Result, anyhow};

pub async fn get_token(
    uc_endpoint: &str,
    table_name: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<String> {
    let (catalog, schema, table) = parse_table_name(table_name)?;

    let uc_endpoint = uc_endpoint.to_string();
    let databricks_client_id = client_id.to_string();
    let databricks_client_secret = client_secret.to_string();

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
            "privileges": ["SELECT"],
            "object_type": "TABLE",
            "object_full_path": format!("{}.{}.{}", catalog, schema, table)
        }
    ]);

    let client = reqwest::Client::new();

    let params = [
        ("grant_type", "client_credentials".to_string()),
        ("scope", "all-apis".to_string()),
        ("authorization_details", authorization_details.to_string()),
    ];

    let token_endpoint = format!("{}/oidc/v1/token", uc_endpoint);
    let resp = client
        .post(&token_endpoint)
        .basic_auth(databricks_client_id, Some(databricks_client_secret))
        .form(&params)
        .send()
        .await
        .map_err(|e| anyhow!("OAuth request failed: {}", e))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let error_body = resp
            .text()
            .await
            .unwrap_or_else(|_| "Failed to read error body".to_string());
        return Err(anyhow!(
            "OAuth request failed with status: {}. Response body: {}",
            status,
            error_body
        ));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| anyhow!("Failed to parse OAuth response: {}", e))?;

    let token = body["access_token"]
        .as_str()
        .ok_or_else(|| anyhow!("access_token missing from OAuth response"))?
        .to_string();
    Ok(token)
}

fn parse_table_name(table_name: &str) -> Result<(String, String, String)> {
    let parts: Vec<&str> = table_name.splitn(3, '.').collect();
    if parts.len() != 3 {
        return Err(anyhow!(
            "Invalid table name format. Expected 'catalog.schema.table', got '{}'",
            table_name
        ));
    }
    Ok((
        parts[0].to_string(),
        parts[1].to_string(),
        parts[2].to_string(),
    ))
}
