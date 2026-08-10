//! Fetch a table's schema from Unity Catalog and resolve it to a protobuf
//! [`MessageDescriptor`], for [`dynamic_proto`](crate::StreamBuilder::dynamic_proto)
//! when the schema is only known at runtime. The runtime counterpart to
//! [`crate::schema`]: reads `GET /api/2.1/unity-catalog/tables/{full_name}` and
//! converts it via [`descriptor_from_uc_schema`].
//!
//! Fetching is a separate step, so the descriptor can be inspected and reused
//! across streams (cloning it is cheap — Arc-backed).
//! [`ZerobusSdk::fetch_message_descriptor`](crate::ZerobusSdk::fetch_message_descriptor)
//! wraps [`fetch_message_descriptor`] with the SDK's `unity_catalog_url`:
//!
//! ```no_run
//! # use databricks_zerobus_ingest_sdk::ZerobusSdk;
//! # async fn example(sdk: &ZerobusSdk) -> Result<(), Box<dyn std::error::Error>> {
//! let descriptor = sdk
//!     .fetch_message_descriptor("catalog.schema.table", "client-id", "client-secret")
//!     .await?;
//! let stream = sdk
//!     .stream_builder()
//!     .table("catalog.schema.table")
//!     .oauth("client-id", "client-secret")
//!     .dynamic_proto(descriptor)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! Columns map per [`crate::schema`] (note `DATE`/`TIMESTAMP` become integers,
//! not `google.protobuf.Timestamp`). The descriptor is a snapshot: if the table
//! changes afterwards the server rejects stream creation with
//! [`ZerobusError::InvalidSchema`], so re-fetch and rebuild.

use std::time::Duration;

use prost_reflect::MessageDescriptor;
use tracing::debug;

use crate::dynamic_proto::message_descriptor;
use crate::schema::{descriptor_from_uc_schema, UcTableSchema};
use crate::{ZerobusError, ZerobusResult};

/// Deadline for a single fetch (token mint plus schema read).
const FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Fetch `table_name`'s schema from Unity Catalog and resolve it to a
/// [`MessageDescriptor`] for [`dynamic_proto`](crate::StreamBuilder::dynamic_proto).
///
/// `unity_catalog_url` is the workspace URL; `table_name` is `catalog.schema.table`.
/// The OAuth credentials must be able to read the table's metadata. Makes two HTTP
/// requests (token mint + table read), so reuse the result — cloning it is cheap
/// (Arc-backed).
///
/// # Errors
///
/// - [`ZerobusError::InvalidTableName`] if `table_name` is not `catalog.schema.table`.
/// - [`ZerobusError::InvalidUCEndpointError`] if `unity_catalog_url` is unusable.
/// - [`ZerobusError::SchemaFetchError`] if the token mint or table read fails.
/// - [`ZerobusError::InvalidArgument`] if the schema has no protobuf
///   representation (e.g. an unsupported column type).
pub async fn fetch_message_descriptor(
    unity_catalog_url: &str,
    table_name: &str,
    client_id: &str,
    client_secret: &str,
) -> ZerobusResult<MessageDescriptor> {
    let schema =
        fetch_table_schema(unity_catalog_url, table_name, client_id, client_secret).await?;
    let descriptor = descriptor_from_uc_schema(&schema).map_err(|e| {
        ZerobusError::InvalidArgument(format!(
            "cannot convert Unity Catalog schema for table '{table_name}' to a protobuf descriptor: {e}"
        ))
    })?;
    message_descriptor(&descriptor)
}

/// Fetch `table_name`'s raw Unity Catalog schema, without converting it to a
/// protobuf descriptor. Useful to inspect the columns directly; most callers
/// want [`fetch_message_descriptor`].
///
/// # Errors
///
/// The same as [`fetch_message_descriptor`], minus the descriptor conversion.
pub async fn fetch_table_schema(
    unity_catalog_url: &str,
    table_name: &str,
    client_id: &str,
    client_secret: &str,
) -> ZerobusResult<UcTableSchema> {
    validate_table_name(table_name)?;
    let base = normalize_endpoint(unity_catalog_url)?;

    let client = reqwest::Client::builder()
        .timeout(FETCH_TIMEOUT)
        .build()
        .map_err(|e| fetch_error(format!("failed to build HTTP client: {e}")))?;

    debug!(table = %table_name, "fetching UC table schema");
    let token = mint_metadata_token(&client, &base, client_id, client_secret).await?;

    // `join_path` percent-encodes the segment, so the table name can't alter the path.
    let url = join_path(&base, ["api", "2.1", "unity-catalog", "tables", table_name]);
    let body = client
        .get(url)
        .bearer_auth(&token)
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|e| fetch_error(format!("schema request failed: {e}")))?
        .bytes()
        .await
        .map_err(|e| fetch_error(format!("reading schema response failed: {e}")))?;

    let schema: UcTableSchema = serde_json::from_slice(&body)
        .map_err(|e| fetch_error(format!("could not parse Unity Catalog response: {e}")))?;
    if schema.columns.is_empty() {
        return Err(fetch_error(format!(
            "Unity Catalog returned no columns for table '{table_name}'"
        )));
    }
    Ok(schema)
}

/// Mint an OAuth token for reading table metadata.
///
/// Separate from [`crate::DefaultTokenFactory`], which mints an ingestion token
/// (`zerobusDirectWriteApi`/`zerobuswrite`) the UC REST API rejects; this
/// requests plain `all-apis` client credentials.
async fn mint_metadata_token(
    client: &reqwest::Client,
    base: &reqwest::Url,
    client_id: &str,
    client_secret: &str,
) -> ZerobusResult<String> {
    let url = join_path(base, ["oidc", "v1", "token"]);
    let params = [("grant_type", "client_credentials"), ("scope", "all-apis")];

    let body = client
        .post(url)
        .basic_auth(client_id, Some(client_secret))
        .form(&params)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|e| fetch_error(format!("token request failed: {e}")))?
        .bytes()
        .await
        .map_err(|e| fetch_error(format!("reading token response failed: {e}")))?;

    let body: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|e| fetch_error(format!("could not parse token response: {e}")))?;
    let token = body["access_token"]
        .as_str()
        .ok_or_else(|| fetch_error("token response has no access_token".to_string()))?;

    // Reject a token that can't be a header value here, not opaquely on the next request.
    if token.is_empty() || !token.bytes().all(|b| b >= 0x20 && b != 0x7f) {
        return Err(fetch_error(
            "token response contains an unusable access_token".to_string(),
        ));
    }
    Ok(token.to_string())
}

fn fetch_error(message: String) -> ZerobusError {
    ZerobusError::SchemaFetchError(message)
}

/// Parse the workspace URL, defaulting a missing scheme to `https` (matching
/// [`ZerobusSdkBuilder::endpoint`](crate::ZerobusSdkBuilder::endpoint)).
fn normalize_endpoint(unity_catalog_url: &str) -> ZerobusResult<reqwest::Url> {
    let trimmed = unity_catalog_url.trim();
    if trimmed.is_empty() {
        return Err(ZerobusError::InvalidUCEndpointError(
            "unity_catalog_url is required; set it on the SDK builder".to_string(),
        ));
    }

    let candidate = if trimmed.contains("://") {
        trimmed.to_string()
    } else {
        format!("https://{trimmed}")
    };

    let url = reqwest::Url::parse(&candidate)
        .map_err(|e| ZerobusError::InvalidUCEndpointError(format!("{unity_catalog_url}: {e}")))?;
    if !matches!(url.scheme(), "http" | "https") || !url.has_host() {
        return Err(ZerobusError::InvalidUCEndpointError(format!(
            "{unity_catalog_url}: expected an http or https URL with a host"
        )));
    }
    // Reject embedded credentials so a secret can't leak into a quoted-URL error.
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ZerobusError::InvalidUCEndpointError(
            "unity_catalog_url must not embed credentials".to_string(),
        ));
    }
    Ok(url)
}

/// Append `segments` to `base`'s path, percent-encoding each one.
fn join_path<'a>(base: &reqwest::Url, segments: impl IntoIterator<Item = &'a str>) -> reqwest::Url {
    let mut url = base.clone();
    {
        // `base` has a host (validated), so it's never a cannot-be-a-base URL.
        let mut path = url
            .path_segments_mut()
            .expect("validated endpoint always has a host");
        // pop_if_empty drops the empty segment a trailing slash would leave.
        path.pop_if_empty().extend(segments);
    }
    url
}

/// Reject a table name that is not `catalog.schema.table`, before any network call.
fn validate_table_name(table_name: &str) -> ZerobusResult<()> {
    let parts: Vec<&str> = table_name.split('.').collect();
    if parts.len() != 3 || parts.iter().any(|p| p.trim().is_empty()) {
        return Err(ZerobusError::InvalidTableName(format!(
            "expected 'catalog.schema.table', got '{table_name}'"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_table_name_requires_three_nonempty_parts() {
        assert!(validate_table_name("cat.sch.tbl").is_ok());
        for bad in ["cat.sch", "cat.sch.tbl.extra", "", ".sch.tbl", "cat. .tbl"] {
            assert!(
                matches!(
                    validate_table_name(bad),
                    Err(ZerobusError::InvalidTableName(_))
                ),
                "expected {bad:?} to be rejected"
            );
        }
    }

    #[test]
    fn normalize_endpoint_defaults_to_https_and_rejects_bad_input() {
        assert_eq!(
            normalize_endpoint("workspace.cloud.databricks.com")
                .unwrap()
                .as_str(),
            "https://workspace.cloud.databricks.com/"
        );
        assert_eq!(
            normalize_endpoint("  http://localhost:8080  ")
                .unwrap()
                .as_str(),
            "http://localhost:8080/"
        );
        for bad in [
            "",
            "   ",
            "ftp://example.com",
            "not a url",
            // Credentials in the URL would end up in error messages.
            "https://user:secret@workspace.cloud.databricks.com",
        ] {
            assert!(
                matches!(
                    normalize_endpoint(bad),
                    Err(ZerobusError::InvalidUCEndpointError(_))
                ),
                "expected {bad:?} to be rejected"
            );
        }
    }

    #[test]
    fn join_path_percent_encodes_and_handles_trailing_slash() {
        let base = normalize_endpoint("https://workspace.cloud.databricks.com/").unwrap();
        let url = join_path(&base, ["api", "2.1", "unity-catalog", "tables", "c.s.t"]);
        assert_eq!(
            url.as_str(),
            "https://workspace.cloud.databricks.com/api/2.1/unity-catalog/tables/c.s.t"
        );

        // A name needing escaping must not escape its path segment.
        let url = join_path(&base, ["tables", "c.s.odd name/../x"]);
        assert_eq!(
            url.as_str(),
            "https://workspace.cloud.databricks.com/tables/c.s.odd%20name%2F..%2Fx"
        );
    }
}
