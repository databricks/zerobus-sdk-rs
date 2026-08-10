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
use reqwest::StatusCode;
use tracing::{debug, warn};

use crate::dynamic_proto::message_descriptor;
use crate::schema::{descriptor_from_uc_schema, UcTableSchema};
use crate::{ZerobusError, ZerobusResult};

/// Deadline for a single fetch (token mint plus schema read).
const FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Cap on a buffered response body. Both responses are small (a token, a column
/// list); the bound guards against an unexpected reply. Rejected outright, not
/// truncated, so we never act on a partial schema.
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

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
/// - [`ZerobusError::SchemaFetchError`] if the token mint or table read fails
///   (`retryable` set for transport errors and 5xx/429).
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
    descriptor_from_schema(&schema)
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
        .map_err(|e| fetch_error(format!("failed to build HTTP client: {e}"), false))?;

    debug!(table = %table_name, "fetching UC table schema");
    let token = mint_metadata_token(&client, &base, client_id, client_secret).await?;
    let schema = get_table(&client, &base, &token, table_name).await?;

    if schema.columns.is_empty() {
        return Err(fetch_error(
            format!("Unity Catalog returned no columns for table '{table_name}'"),
            false,
        ));
    }
    Ok(schema)
}

/// Convert a fetched [`UcTableSchema`] into a [`MessageDescriptor`], mapping a
/// conversion failure to [`ZerobusError::InvalidArgument`].
fn descriptor_from_schema(schema: &UcTableSchema) -> ZerobusResult<MessageDescriptor> {
    let descriptor = descriptor_from_uc_schema(schema).map_err(|e| {
        ZerobusError::InvalidArgument(format!(
            "cannot convert Unity Catalog schema for table '{}' to a protobuf descriptor: {e}",
            schema.name
        ))
    })?;
    message_descriptor(&descriptor)
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

    let response = client
        .post(url)
        .basic_auth(client_id, Some(client_secret))
        .form(&params)
        .send()
        .await
        .map_err(|e| transport_error("token request", &e))?;

    let body = read_body("token request", response).await?;
    let body: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|e| fetch_error(format!("could not parse token response: {e}"), false))?;

    let token = body["access_token"]
        .as_str()
        .ok_or_else(|| fetch_error("token response has no access_token".to_string(), false))?;

    // Reject a token that can't be a header value here, not opaquely on the next request.
    if token.is_empty() || !token.bytes().all(|b| b >= 0x20 && b != 0x7f) {
        return Err(fetch_error(
            "token response contains an unusable access_token".to_string(),
            false,
        ));
    }
    Ok(token.to_string())
}

/// Read one table's metadata from the Unity Catalog REST API.
async fn get_table(
    client: &reqwest::Client,
    base: &reqwest::Url,
    token: &str,
    table_name: &str,
) -> ZerobusResult<UcTableSchema> {
    // `join_path` percent-encodes the segment, so the table name can't alter the path.
    let url = join_path(base, ["api", "2.1", "unity-catalog", "tables", table_name]);

    let response = client
        .get(url)
        .bearer_auth(token)
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
        .map_err(|e| transport_error("schema request", &e))?;

    let body = read_body("schema request", response).await?;
    serde_json::from_slice(&body).map_err(|e| {
        fetch_error(
            format!("could not parse Unity Catalog response for table '{table_name}': {e}"),
            false,
        )
    })
}

/// Read a response body, failing on a non-success status or an oversized body.
async fn read_body(operation: &str, response: reqwest::Response) -> ZerobusResult<Vec<u8>> {
    let status = response.status();

    // Early reject via Content-Length; the streamed read bounds an absent/understated one.
    if response
        .content_length()
        .is_some_and(|len| len > MAX_RESPONSE_BYTES as u64)
    {
        return Err(oversized_body_error(operation, status));
    }

    let mut response = response;
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| transport_error(operation, &e))?
    {
        if body.len() + chunk.len() > MAX_RESPONSE_BYTES {
            return Err(oversized_body_error(operation, status));
        }
        body.extend_from_slice(&chunk);
    }

    if !status.is_success() {
        // Truncate the server's error body so an HTML page can't swamp the log.
        let detail = String::from_utf8_lossy(&body);
        let detail: String = detail.trim().chars().take(512).collect();
        let retryable = status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS;
        let message = if detail.is_empty() {
            format!("{operation} failed with HTTP {status}")
        } else {
            format!("{operation} failed with HTTP {status}: {detail}")
        };
        warn!(%status, retryable, "{operation} to Unity Catalog failed");
        return Err(fetch_error(message, retryable));
    }

    Ok(body)
}

fn oversized_body_error(operation: &str, status: StatusCode) -> ZerobusError {
    fetch_error(
        format!(
            "{operation} returned a response larger than {MAX_RESPONSE_BYTES} bytes (HTTP {status})"
        ),
        false,
    )
}

/// Classify a `reqwest` transport failure: timeouts, connection, and incomplete
/// request/response errors are transient; anything else terminal.
fn transport_error(operation: &str, error: &reqwest::Error) -> ZerobusError {
    let retryable = error.is_timeout() || error.is_connect() || error.is_request();
    fetch_error(format!("{operation} failed: {error}"), retryable)
}

fn fetch_error(message: String, retryable: bool) -> ZerobusError {
    ZerobusError::SchemaFetchError { message, retryable }
}

/// Parse the workspace URL, defaulting a missing scheme to `https` (matching
/// [`ZerobusSdkBuilder::endpoint`](crate::ZerobusSdkBuilder::endpoint)).
fn normalize_endpoint(unity_catalog_url: &str) -> ZerobusResult<reqwest::Url> {
    let trimmed = unity_catalog_url.trim();
    if trimmed.is_empty() {
        return Err(ZerobusError::InvalidUCEndpointError(
            "unity_catalog_url is required to fetch a schema from Unity Catalog; set it on the SDK builder".to_string(),
        ));
    }

    let candidate = if trimmed.contains("://") {
        trimmed.to_string()
    } else {
        format!("https://{trimmed}")
    };

    let url = reqwest::Url::parse(&candidate)
        .map_err(|e| ZerobusError::InvalidUCEndpointError(format!("{unity_catalog_url}: {e}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(ZerobusError::InvalidUCEndpointError(format!(
            "{unity_catalog_url}: expected an http or https URL"
        )));
    }
    if !url.has_host() {
        return Err(ZerobusError::InvalidUCEndpointError(format!(
            "{unity_catalog_url}: URL has no host"
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
    use crate::schema::UcColumn;

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

    #[test]
    fn descriptor_from_schema_converts_columns() {
        let schema = UcTableSchema {
            name: "orders".to_string(),
            catalog_name: "main".to_string(),
            schema_name: "sales".to_string(),
            columns: vec![
                UcColumn {
                    name: "id".to_string(),
                    type_name: "BIGINT".to_string(),
                    type_text: "bigint".to_string(),
                    type_json: String::new(),
                    nullable: false,
                    position: 0,
                },
                UcColumn {
                    name: "customer".to_string(),
                    type_name: "STRING".to_string(),
                    type_text: "string".to_string(),
                    type_json: String::new(),
                    nullable: true,
                    position: 1,
                },
            ],
        };

        let md = descriptor_from_schema(&schema).unwrap();
        // Message name comes from `descriptor_from_uc_schema`: <schema>_<table>.
        assert_eq!(md.name(), "SalesOrders");
        assert_eq!(md.get_field_by_name("id").unwrap().number(), 1);
        assert_eq!(md.get_field_by_name("customer").unwrap().number(), 2);
    }

    #[test]
    fn descriptor_from_schema_rejects_unsupported_column() {
        let schema = UcTableSchema {
            name: "t".to_string(),
            catalog_name: "c".to_string(),
            schema_name: "s".to_string(),
            columns: vec![UcColumn {
                name: "weird".to_string(),
                type_name: "INTERVAL".to_string(),
                type_text: String::new(),
                type_json: String::new(),
                nullable: true,
                position: 0,
            }],
        };

        match descriptor_from_schema(&schema) {
            Err(ZerobusError::InvalidArgument(msg)) => assert!(msg.contains("INTERVAL"), "{msg}"),
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn schema_fetch_error_retryability_is_carried() {
        assert!(fetch_error("boom".to_string(), true).is_retryable());
        assert!(!fetch_error("boom".to_string(), false).is_retryable());
    }
}
