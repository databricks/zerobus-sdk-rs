//! Tests for fetching a table's schema from Unity Catalog and using it as the
//! dynamic-proto descriptor (`uc_schema`, `ZerobusSdk::fetch_message_descriptor`).

mod mock_uc;
mod utils;

use databricks_zerobus_ingest_sdk::uc_schema::{fetch_message_descriptor, fetch_table_schema};
use databricks_zerobus_ingest_sdk::{ZerobusError, ZerobusSdk};
use mock_uc::{
    simple_columns_json, start_mock_uc, start_mock_uc_serving_columns, table_response, MockReply,
};
use utils::setup_tracing;

const TABLE_NAME: &str = "main.sales.orders";

/// A token reply minting `access_token`.
fn token_ok() -> MockReply {
    MockReply::Json(r#"{"access_token":"test-token","expires_in":3600}"#.to_string())
}

mod fetch_tests {
    use super::*;

    #[tokio::test]
    async fn fetches_and_resolves_descriptor() {
        setup_tracing();
        let mock = start_mock_uc_serving_columns("test-token", simple_columns_json()).await;

        let descriptor =
            fetch_message_descriptor(&mock.url, TABLE_NAME, "client-id", "client-secret")
                .await
                .expect("fetch should succeed");

        // Message name is <schema>_<table>, sanitized.
        assert_eq!(descriptor.name(), "SalesOrders");
        // Proto field number is the UC position + 1.
        assert_eq!(descriptor.get_field_by_name("id").unwrap().number(), 1);
        assert_eq!(
            descriptor.get_field_by_name("customer").unwrap().number(),
            2
        );

        assert_eq!(mock.token_calls(), 1, "should mint exactly one token");
        assert_eq!(mock.schema_calls(), 1, "should make one schema request");
    }

    #[tokio::test]
    async fn sends_basic_auth_token_request_then_bearer_schema_request() {
        setup_tracing();
        let mock = start_mock_uc_serving_columns("test-token", simple_columns_json()).await;

        fetch_message_descriptor(&mock.url, TABLE_NAME, "client-id", "client-secret")
            .await
            .expect("fetch should succeed");

        // The token request authenticates with client credentials...
        let token_auth = mock.requests.token_auth.lock().unwrap().clone();
        assert_eq!(token_auth.len(), 1);
        assert!(
            token_auth[0].starts_with("Basic "),
            "expected Basic auth on the token request, got {:?}",
            token_auth[0]
        );

        // ...and requests plain all-apis client credentials, not an ingestion
        // token (which UC's REST API would reject).
        let token_body = mock.requests.token_bodies.lock().unwrap().clone();
        assert!(
            token_body[0].contains("grant_type=client_credentials")
                && token_body[0].contains("scope=all-apis"),
            "unexpected token request body: {:?}",
            token_body[0]
        );
        assert!(
            !token_body[0].contains("authorization_details")
                && !token_body[0].contains("zerobusDirectWriteApi"),
            "token request must not request ingestion scopes: {:?}",
            token_body[0]
        );

        // The schema request presents the minted token as a bearer token.
        let schema_auth = mock.requests.schema_auth.lock().unwrap().clone();
        assert_eq!(schema_auth, vec!["Bearer test-token".to_string()]);
    }

    #[tokio::test]
    async fn requests_the_expected_table_path() {
        setup_tracing();
        let mock = start_mock_uc_serving_columns("test-token", simple_columns_json()).await;

        fetch_message_descriptor(&mock.url, TABLE_NAME, "client-id", "client-secret")
            .await
            .expect("fetch should succeed");

        let paths = mock.requests.schema_paths.lock().unwrap().clone();
        assert_eq!(
            paths,
            vec![format!("/api/2.1/unity-catalog/tables/{TABLE_NAME}")]
        );
    }

    #[tokio::test]
    async fn fetch_table_schema_returns_raw_columns() {
        setup_tracing();
        let mock = start_mock_uc_serving_columns("test-token", simple_columns_json()).await;

        let schema = fetch_table_schema(&mock.url, TABLE_NAME, "client-id", "client-secret")
            .await
            .expect("fetch should succeed");

        assert_eq!(schema.name, "orders");
        assert_eq!(schema.catalog_name, "main");
        assert_eq!(schema.schema_name, "sales");
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].name, "id");
        assert_eq!(schema.columns[0].type_name, "BIGINT");
        assert!(!schema.columns[0].nullable);
    }

    #[tokio::test]
    async fn resolves_complex_columns_from_type_json() {
        setup_tracing();
        let columns = r#"[
            {"name":"id","type_name":"BIGINT","type_text":"bigint","type_json":"","nullable":false,"position":0},
            {"name":"address","type_name":"STRUCT","type_text":"struct<street:string>","type_json":"{\"type\":\"struct\",\"fields\":[{\"name\":\"street\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","nullable":true,"position":1}
        ]"#;
        let mock = start_mock_uc_serving_columns("test-token", columns).await;

        let descriptor =
            fetch_message_descriptor(&mock.url, TABLE_NAME, "client-id", "client-secret")
                .await
                .expect("fetch should succeed");

        let address = descriptor.get_field_by_name("address").unwrap();
        let nested = match address.kind() {
            prost_reflect::Kind::Message(m) => m,
            other => panic!("expected a nested message, got {other:?}"),
        };
        assert!(nested.get_field_by_name("street").is_some());
    }
}

mod error_classification_tests {
    use super::*;

    /// Assert `result` failed with a `SchemaFetchError` whose retryability
    /// matches `retryable` and whose message contains `needle`.
    fn assert_schema_fetch_error<T: std::fmt::Debug>(
        result: Result<T, ZerobusError>,
        retryable: bool,
        needle: &str,
    ) {
        match result {
            Err(err @ ZerobusError::SchemaFetchError { .. }) => {
                assert_eq!(
                    err.is_retryable(),
                    retryable,
                    "unexpected retryability for {err}"
                );
                assert!(
                    err.to_string().contains(needle),
                    "expected {needle:?} in error, got: {err}"
                );
            }
            other => panic!("expected SchemaFetchError, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn server_error_on_schema_request_is_retryable() {
        setup_tracing();
        let mock = start_mock_uc(
            token_ok(),
            MockReply::Status(503, "upstream unavailable".to_string()),
        )
        .await;

        let result = fetch_message_descriptor(&mock.url, TABLE_NAME, "cid", "csec").await;
        assert_schema_fetch_error(result, true, "503");
    }

    #[tokio::test]
    async fn too_many_requests_is_retryable() {
        setup_tracing();
        let mock = start_mock_uc(token_ok(), MockReply::Status(429, "slow down".to_string())).await;

        let result = fetch_message_descriptor(&mock.url, TABLE_NAME, "cid", "csec").await;
        assert_schema_fetch_error(result, true, "429");
    }

    #[tokio::test]
    async fn client_errors_are_not_retryable() {
        setup_tracing();
        for status in [400u16, 401, 403, 404] {
            let mock =
                start_mock_uc(token_ok(), MockReply::Status(status, "denied".to_string())).await;

            let result = fetch_message_descriptor(&mock.url, TABLE_NAME, "cid", "csec").await;
            assert_schema_fetch_error(result, false, &status.to_string());
        }
    }

    #[tokio::test]
    async fn failed_token_request_is_reported_as_such() {
        setup_tracing();
        let mock = start_mock_uc(
            MockReply::Status(401, "bad credentials".to_string()),
            MockReply::Json(table_response(simple_columns_json())),
        )
        .await;

        let result = fetch_message_descriptor(&mock.url, TABLE_NAME, "cid", "csec").await;
        assert_schema_fetch_error(result, false, "token request");
        assert_eq!(
            mock.schema_calls(),
            0,
            "must not request the schema without a token"
        );
    }

    #[tokio::test]
    async fn token_response_without_access_token_is_rejected() {
        setup_tracing();
        let mock = start_mock_uc(
            MockReply::Json(r#"{"expires_in":3600}"#.to_string()),
            MockReply::Json(table_response(simple_columns_json())),
        )
        .await;

        let result = fetch_message_descriptor(&mock.url, TABLE_NAME, "cid", "csec").await;
        assert_schema_fetch_error(result, false, "access_token");
    }

    #[tokio::test]
    async fn unparseable_schema_response_is_rejected() {
        setup_tracing();
        let mock = start_mock_uc(token_ok(), MockReply::Json("this is not json".to_string())).await;

        let result = fetch_message_descriptor(&mock.url, TABLE_NAME, "cid", "csec").await;
        assert_schema_fetch_error(result, false, "could not parse");
    }

    #[tokio::test]
    async fn empty_column_list_is_rejected() {
        setup_tracing();
        let mock = start_mock_uc_serving_columns("test-token", "[]").await;

        let result = fetch_message_descriptor(&mock.url, TABLE_NAME, "cid", "csec").await;
        assert_schema_fetch_error(result, false, "no columns");
    }

    #[tokio::test]
    async fn oversized_response_is_rejected() {
        setup_tracing();
        let mock = start_mock_uc(
            token_ok(),
            MockReply::OverlongContentLength(64 * 1024 * 1024),
        )
        .await;

        let result = fetch_message_descriptor(&mock.url, TABLE_NAME, "cid", "csec").await;
        assert_schema_fetch_error(result, false, "larger than");
    }

    #[tokio::test]
    async fn dropped_connection_is_retryable() {
        setup_tracing();
        let mock = start_mock_uc(MockReply::Hangup, MockReply::Hangup).await;

        let result = fetch_message_descriptor(&mock.url, TABLE_NAME, "cid", "csec").await;
        assert_schema_fetch_error(result, true, "token request");
    }

    #[tokio::test]
    async fn unsupported_column_type_is_invalid_argument() {
        setup_tracing();
        let columns = r#"[
            {"name":"span","type_name":"INTERVAL","type_text":"interval","type_json":"","nullable":true,"position":0}
        ]"#;
        let mock = start_mock_uc_serving_columns("test-token", columns).await;

        // A schema the server returned but that has no protobuf representation is
        // a caller-facing argument problem, not a fetch failure.
        match fetch_message_descriptor(&mock.url, TABLE_NAME, "cid", "csec").await {
            Err(ZerobusError::InvalidArgument(msg)) => {
                assert!(msg.contains("INTERVAL"), "unexpected message: {msg}");
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn invalid_table_name_fails_before_any_request() {
        setup_tracing();
        let mock = start_mock_uc_serving_columns("test-token", simple_columns_json()).await;

        for bad in ["orders", "sales.orders", "main.sales.orders.extra", ""] {
            match fetch_message_descriptor(&mock.url, bad, "cid", "csec").await {
                Err(ZerobusError::InvalidTableName(msg)) => {
                    assert!(msg.contains("catalog.schema.table"), "got: {msg}");
                }
                other => panic!("expected InvalidTableName for {bad:?}, got {other:?}"),
            }
        }
        assert_eq!(mock.token_calls(), 0, "must not hit the network");
    }

    #[tokio::test]
    async fn invalid_endpoint_fails_before_any_request() {
        setup_tracing();
        for bad in ["", "   ", "ftp://example.com"] {
            match fetch_message_descriptor(bad, TABLE_NAME, "cid", "csec").await {
                Err(ZerobusError::InvalidUCEndpointError(_)) => {}
                other => panic!("expected InvalidUCEndpointError for {bad:?}, got {other:?}"),
            }
        }
    }
}

mod sdk_convenience_tests {
    use super::*;

    /// An SDK whose `unity_catalog_url` points at `uc_url`. The zerobus endpoint
    /// is never dialed here — these tests only exercise the schema fetch and the
    /// builder wiring, both of which precede the gRPC connection.
    fn sdk_with_uc(uc_url: &str) -> ZerobusSdk {
        ZerobusSdk::builder()
            .endpoint("http://127.0.0.1:1")
            .unity_catalog_url(uc_url)
            .no_tls()
            .build()
            .expect("sdk should build")
    }

    #[tokio::test]
    async fn sdk_fetch_message_descriptor_uses_configured_uc_url() {
        setup_tracing();
        let mock = start_mock_uc_serving_columns("test-token", simple_columns_json()).await;
        let sdk = sdk_with_uc(&mock.url);

        let descriptor = sdk
            .fetch_message_descriptor(TABLE_NAME, "client-id", "client-secret")
            .await
            .expect("fetch should succeed");

        assert_eq!(descriptor.name(), "SalesOrders");
        assert_eq!(mock.schema_calls(), 1);
    }

    #[tokio::test]
    async fn fetched_descriptor_plugs_into_dynamic_proto_builder() {
        setup_tracing();
        let mock = start_mock_uc_serving_columns("test-token", simple_columns_json()).await;
        let sdk = sdk_with_uc(&mock.url);

        // The whole point of the utility: fetch once, then hand the descriptor to
        // the existing `.dynamic_proto()` selector. `validate()` confirms the
        // builder accepts it without any further schema work.
        let descriptor = sdk
            .fetch_message_descriptor(TABLE_NAME, "cid", "csec")
            .await
            .expect("fetch should succeed");

        let builder = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .oauth("cid", "csec")
            .dynamic_proto(descriptor);
        builder.validate().expect("validation should succeed");
    }

    #[tokio::test]
    async fn fetch_failure_surfaces_from_sdk_helper() {
        setup_tracing();
        let mock = start_mock_uc(
            token_ok(),
            MockReply::Status(404, "table not found".to_string()),
        )
        .await;
        let sdk = sdk_with_uc(&mock.url);

        match sdk
            .fetch_message_descriptor(TABLE_NAME, "cid", "csec")
            .await
        {
            Err(err @ ZerobusError::SchemaFetchError { .. }) => {
                assert!(!err.is_retryable());
                assert!(err.to_string().contains("404"), "got: {err}");
            }
            other => panic!("expected SchemaFetchError, got {other:?}"),
        }
        assert_eq!(mock.schema_calls(), 1);
    }
}
