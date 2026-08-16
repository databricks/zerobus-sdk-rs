use std::time::Duration;

use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::{ZerobusError, ZerobusResult};

/// An access token together with its time-to-live as reported by the OAuth
/// server, if any.
pub(crate) struct FetchedToken {
    /// The OAuth 2.0 access token.
    pub(crate) token: String,
    /// Lifetime of the token derived from the `expires_in` field of the OAuth
    /// response. `None` if the server did not return a usable `expires_in`, in
    /// which case the token must not be cached.
    pub(crate) expires_in: Option<Duration>,
}

/// Why a token mint was triggered. Logged on the mint so operators can tell a
/// cold start from a proactive refresh from caching being off.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MintReason {
    /// No usable cached token (cold start or the cached token had expired).
    ColdMiss,
    /// A cached token entered the refresh window and was proactively renewed.
    Refresh,
    /// Token caching is disabled, so every stream creation mints.
    CacheDisabled,
    /// Minted outside the cache via the public `get_token`.
    Direct,
}

impl MintReason {
    fn as_str(self) -> &'static str {
        match self {
            MintReason::ColdMiss => "cold_miss",
            MintReason::Refresh => "refresh",
            MintReason::CacheDisabled => "cache_disabled",
            MintReason::Direct => "direct",
        }
    }
}

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
        Self::fetch_token(
            uc_endpoint,
            table_name,
            client_id,
            client_secret,
            workspace_id,
            MintReason::Direct,
        )
        .await
        .map(|fetched| fetched.token)
    }

    /// Obtains an OAuth 2.0 access token along with its reported lifetime.
    ///
    /// This is the caching-aware variant of [`get_token`](Self::get_token): in
    /// addition to the token it returns the `expires_in` value from the OAuth
    /// response so callers can cache the token until it nears expiry. Uses the
    /// client-credentials grant (client_id + secret).
    pub(crate) async fn fetch_token(
        uc_endpoint: &str,
        table_name: &str,
        client_id: &str,
        client_secret: &str,
        workspace_id: &str,
        reason: MintReason,
    ) -> ZerobusResult<FetchedToken> {
        debug!(table = %table_name, "requesting Zerobus token (client_credentials)");
        let started = Instant::now();
        let result = Self::fetch_token_inner(
            uc_endpoint,
            table_name,
            client_id,
            client_secret,
            workspace_id,
        )
        .await;
        Self::log_mint_outcome(
            table_name,
            reason,
            started.elapsed().as_millis() as u64,
            &result,
            "client_credentials",
        );
        result
    }

    /// Obtains a Databricks access token by exchanging an external IdP token
    /// (e.g. an Entra ID JWT) for a Zerobus-scoped Databricks token via the
    /// RFC 8693 token-exchange grant.
    ///
    /// Used by [`FederatedTokenProvider`](crate::FederatedTokenProvider). The
    /// request is shaped identically to the client-credentials grant (same
    /// Zerobus resource, scope, and table-scoped authorization details); only
    /// the grant-specific parameters differ: `grant_type=token-exchange`, the
    /// `subject_token` carrying the IdP JWT, and — for workload identity
    /// federation — the Databricks service principal `client_id`. `client_id`
    /// is `None` for account-level federation (identity resolved via SCIM).
    pub(crate) async fn fetch_exchanged_token(
        uc_endpoint: &str,
        table_name: &str,
        client_id: Option<&str>,
        subject_token: &str,
        workspace_id: &str,
        reason: MintReason,
    ) -> ZerobusResult<FetchedToken> {
        debug!(table = %table_name, "requesting Zerobus token (token-exchange)");
        let started = Instant::now();
        let result = Self::fetch_exchanged_token_inner(
            uc_endpoint,
            table_name,
            client_id,
            subject_token,
            workspace_id,
        )
        .await;
        Self::log_mint_outcome(
            table_name,
            reason,
            started.elapsed().as_millis() as u64,
            &result,
            "token_exchange",
        );
        result
    }

    /// Emits the structured mint log shared by every grant type. `grant`
    /// distinguishes the client-credentials path from the token-exchange path.
    fn log_mint_outcome(
        table_name: &str,
        reason: MintReason,
        elapsed_ms: u64,
        result: &ZerobusResult<FetchedToken>,
        grant: &'static str,
    ) {
        match result {
            Ok(FetchedToken {
                expires_in: Some(ttl),
                ..
            }) => info!(
                table = %table_name,
                reason = reason.as_str(),
                grant,
                expires_in_secs = ttl.as_secs(),
                elapsed_ms,
                "minted Zerobus token"
            ),
            Ok(FetchedToken {
                expires_in: None, ..
            }) => warn!(
                table = %table_name,
                reason = reason.as_str(),
                grant,
                elapsed_ms,
                "minted Zerobus token but UC returned no expires_in; token will not be cached"
            ),
            Err(err) => warn!(
                table = %table_name,
                reason = reason.as_str(),
                grant,
                retryable = err.is_retryable(),
                elapsed_ms,
                "failed to mint Zerobus token: {err}"
            ),
        }
    }

    /// Client-credentials grant: builds the shared Zerobus-scoped request and
    /// adds `grant_type=client_credentials`, authenticating with HTTP Basic
    /// (client_id + secret).
    async fn fetch_token_inner(
        uc_endpoint: &str,
        table_name: &str,
        client_id: &str,
        client_secret: &str,
        workspace_id: &str,
    ) -> ZerobusResult<FetchedToken> {
        let params = Self::client_credentials_form_params(table_name, workspace_id)?;
        Self::post_token_request(uc_endpoint, &params, Some((client_id, client_secret))).await
    }

    /// Builds the full client-credentials form parameters: the shared
    /// Zerobus-scoped parameters plus `grant_type=client_credentials`.
    #[allow(clippy::result_large_err)]
    fn client_credentials_form_params(
        table_name: &str,
        workspace_id: &str,
    ) -> ZerobusResult<Vec<(&'static str, String)>> {
        let mut params = Self::zerobus_scoped_form_params(table_name, workspace_id)?;
        params.push(("grant_type", "client_credentials".to_string()));
        Ok(params)
    }

    /// Token-exchange grant (RFC 8693): builds the same shared Zerobus-scoped
    /// request and adds the exchange-specific parameters — `grant_type`,
    /// `subject_token` (the external IdP JWT), `subject_token_type`, and, for
    /// workload identity federation, the Databricks SP `client_id`. No HTTP
    /// Basic auth: the subject token is the credential.
    async fn fetch_exchanged_token_inner(
        uc_endpoint: &str,
        table_name: &str,
        client_id: Option<&str>,
        subject_token: &str,
        workspace_id: &str,
    ) -> ZerobusResult<FetchedToken> {
        let params =
            Self::exchange_form_params(table_name, client_id, subject_token, workspace_id)?;
        Self::post_token_request(uc_endpoint, &params, None).await
    }

    /// Builds the full RFC 8693 token-exchange form parameters: the shared
    /// Zerobus-scoped parameters plus the exchange-specific parameters. The
    /// Databricks SP `client_id` is included only for workload identity
    /// federation (Story 2) and omitted for account-level federation (Story 1).
    #[allow(clippy::result_large_err)]
    fn exchange_form_params(
        table_name: &str,
        client_id: Option<&str>,
        subject_token: &str,
        workspace_id: &str,
    ) -> ZerobusResult<Vec<(&'static str, String)>> {
        let mut params = Self::zerobus_scoped_form_params(table_name, workspace_id)?;
        params.push((
            "grant_type",
            "urn:ietf:params:oauth:grant-type:token-exchange".to_string(),
        ));
        params.push(("subject_token", subject_token.to_string()));
        params.push((
            "subject_token_type",
            "urn:ietf:params:oauth:token-type:jwt".to_string(),
        ));
        // Present for workload identity federation (Story 2), naming the
        // Databricks service principal; omitted for account-level federation
        // (Story 1), where the subject is resolved to a SCIM-synced identity.
        if let Some(client_id) = client_id {
            params.push(("client_id", client_id.to_string()));
        }
        Ok(params)
    }

    /// Builds the Zerobus-scoped OAuth form parameters shared by every grant
    /// type: `scope=all-apis`, the `zerobusDirectWriteApi` resource for this
    /// workspace, and the table-scoped Unity Catalog `authorization_details`.
    /// Both the client-credentials grant and the RFC 8693 token-exchange grant
    /// send an identical Zerobus-scoped request; only the grant-specific
    /// parameters (grant_type, credentials/subject_token) differ. Keeping this
    /// in one place keeps the two grants at parity.
    #[allow(clippy::result_large_err)]
    fn zerobus_scoped_form_params(
        table_name: &str,
        workspace_id: &str,
    ) -> ZerobusResult<Vec<(&'static str, String)>> {
        let (catalog, schema, table) = Self::parse_table_name(table_name)?;

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

        Ok(vec![
            ("scope", "all-apis".to_string()),
            (
                "resource",
                format!(
                    "api://databricks/workspaces/{}/zerobusDirectWriteApi",
                    workspace_id
                ),
            ),
            ("authorization_details", authorization_details.to_string()),
        ])
    }

    /// Posts a token request to the UC OIDC endpoint and parses the response.
    /// Shared by every grant: `basic_auth` carries the client-credentials
    /// Basic header when present, and is `None` for the token-exchange grant.
    async fn post_token_request(
        uc_endpoint: &str,
        params: &[(&str, String)],
        basic_auth: Option<(&str, &str)>,
    ) -> ZerobusResult<FetchedToken> {
        let client = reqwest::Client::new();
        let token_endpoint = format!("{}/oidc/v1/token", uc_endpoint);

        let mut request = client.post(&token_endpoint);
        if let Some((client_id, client_secret)) = basic_auth {
            request = request.basic_auth(client_id, Some(client_secret));
        }

        let resp = request
            .form(params)
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

        // Reject a token that can't be a header value before it is returned, so
        // an unusable token never enters the cache and poisons it until expiry.
        if !Self::is_usable_as_header(&token) {
            return Err(ZerobusError::InvalidUCTokenError(
                "access token is not a valid HTTP header value".to_string(),
            ));
        }

        let expires_in = Self::parse_expires_in(&body);

        Ok(FetchedToken { token, expires_in })
    }

    /// Reports whether `token` can be sent as the `authorization` header value
    /// (`Bearer <token>`). The gRPC and Arrow paths both encode it this way, so a
    /// token that fails here is unusable and must not be cached.
    fn is_usable_as_header(token: &str) -> bool {
        tonic::metadata::AsciiMetadataValue::try_from(format!("Bearer {token}").as_str()).is_ok()
    }

    /// Parses the OAuth `expires_in` field (token lifetime in seconds) into a
    /// `Duration`. It is optional in the OAuth spec; if it is missing or not a
    /// positive integer the token has no known TTL and must not be cached.
    fn parse_expires_in(body: &serde_json::Value) -> Option<Duration> {
        body["expires_in"]
            .as_u64()
            .filter(|secs| *secs > 0)
            .map(Duration::from_secs)
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
    fn test_parse_expires_in() {
        let with_ttl = serde_json::json!({ "expires_in": 3600 });
        assert_eq!(
            DefaultTokenFactory::parse_expires_in(&with_ttl),
            Some(Duration::from_secs(3600))
        );

        let missing = serde_json::json!({ "access_token": "abc" });
        assert_eq!(DefaultTokenFactory::parse_expires_in(&missing), None);

        let zero = serde_json::json!({ "expires_in": 0 });
        assert_eq!(DefaultTokenFactory::parse_expires_in(&zero), None);

        // A string value (non-integer) is not usable and yields no TTL.
        let non_numeric = serde_json::json!({ "expires_in": "3600" });
        assert_eq!(DefaultTokenFactory::parse_expires_in(&non_numeric), None);
    }

    #[test]
    fn test_is_usable_as_header() {
        // A normal JWT-shaped token is a valid header value.
        assert!(DefaultTokenFactory::is_usable_as_header(
            "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIxIn0.sig-_value"
        ));
        // Control characters (e.g. an embedded newline) make it unusable.
        assert!(!DefaultTokenFactory::is_usable_as_header("bad\ntoken"));
        assert!(!DefaultTokenFactory::is_usable_as_header("bad\0token"));
    }

    /// Looks up the single value for `key` in a form-param list, asserting it is
    /// present exactly once.
    fn param<'a>(params: &'a [(&str, String)], key: &str) -> &'a str {
        let matches: Vec<&str> = params
            .iter()
            .filter(|(k, _)| *k == key)
            .map(|(_, v)| v.as_str())
            .collect();
        assert_eq!(
            matches.len(),
            1,
            "expected exactly one '{}' param, found {}",
            key,
            matches.len()
        );
        matches[0]
    }

    fn has_param(params: &[(&str, String)], key: &str) -> bool {
        params.iter().any(|(k, _)| *k == key)
    }

    /// The Zerobus-scoped parameters (scope, resource, authorization_details)
    /// must be byte-identical between the client-credentials grant and the
    /// token-exchange grant. This is the parity guarantee the refactor exists to
    /// enforce.
    #[test]
    fn client_credentials_and_exchange_share_identical_zerobus_scoping() {
        let table = "cat.sch.tbl";
        let workspace = "1234567890";

        let cc = DefaultTokenFactory::client_credentials_form_params(table, workspace).unwrap();
        let ex =
            DefaultTokenFactory::exchange_form_params(table, Some("sp-id"), "idp-jwt", workspace)
                .unwrap();

        for key in ["scope", "resource", "authorization_details"] {
            assert_eq!(
                param(&cc, key),
                param(&ex, key),
                "'{}' must match across grants",
                key
            );
        }
    }

    #[test]
    fn client_credentials_form_params_shape() {
        let params =
            DefaultTokenFactory::client_credentials_form_params("cat.sch.tbl", "42").unwrap();

        assert_eq!(param(&params, "grant_type"), "client_credentials");
        assert_eq!(param(&params, "scope"), "all-apis");
        assert_eq!(
            param(&params, "resource"),
            "api://databricks/workspaces/42/zerobusDirectWriteApi"
        );
        // The token-exchange-only params must never appear on this grant.
        assert!(!has_param(&params, "subject_token"));
        assert!(!has_param(&params, "subject_token_type"));

        // authorization_details is downscoped to the specific table.
        let details: serde_json::Value =
            serde_json::from_str(param(&params, "authorization_details")).unwrap();
        assert_eq!(details[2]["object_full_path"], "cat.sch.tbl");
        assert_eq!(details[2]["operations"][0], "zerobuswrite");
    }

    #[test]
    fn exchange_form_params_without_client_id_is_account_level() {
        // Account-level federation (Story 1): no client_id in the request.
        let params =
            DefaultTokenFactory::exchange_form_params("cat.sch.tbl", None, "idp-jwt-token", "99")
                .unwrap();

        assert_eq!(
            param(&params, "grant_type"),
            "urn:ietf:params:oauth:grant-type:token-exchange"
        );
        assert_eq!(param(&params, "subject_token"), "idp-jwt-token");
        assert_eq!(
            param(&params, "subject_token_type"),
            "urn:ietf:params:oauth:token-type:jwt"
        );
        assert!(
            !has_param(&params, "client_id"),
            "account-level federation must omit client_id"
        );
        // Client-credentials-only auth is never sent on the exchange grant.
        assert!(!has_param(&params, "client_secret"));
    }

    #[test]
    fn exchange_form_params_with_client_id_is_workload_identity() {
        // Workload identity federation (Story 2): client_id names the SP.
        let params = DefaultTokenFactory::exchange_form_params(
            "cat.sch.tbl",
            Some("sp-client-id-uuid"),
            "idp-jwt-token",
            "99",
        )
        .unwrap();

        assert_eq!(
            param(&params, "grant_type"),
            "urn:ietf:params:oauth:grant-type:token-exchange"
        );
        assert_eq!(param(&params, "client_id"), "sp-client-id-uuid");
        assert_eq!(param(&params, "subject_token"), "idp-jwt-token");
    }

    #[test]
    fn exchange_form_params_rejects_bad_table_name() {
        let err = DefaultTokenFactory::exchange_form_params("not_three_parts", None, "jwt", "1");
        assert!(matches!(err, Err(ZerobusError::InvalidTableName(_))));
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
