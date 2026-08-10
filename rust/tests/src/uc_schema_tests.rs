//! Tests for fetching a table's schema from Unity Catalog
//! (`uc_schema::fetch_message_descriptor`), against a tiny in-process HTTP mock.

use std::sync::{Arc, Mutex};

use databricks_zerobus_ingest_sdk::uc_schema::fetch_message_descriptor;
use databricks_zerobus_ingest_sdk::ZerobusError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const TABLE: &str = "main.sales.orders";

/// One recorded request: its target path and `authorization` header.
type Recorded = Vec<(String, String)>;

/// A running mock. Serves `POST /oidc/v1/token` with a fixed token, then the
/// table route with `schema_status`/`schema_body`. Dropping it stops the loop.
struct MockUc {
    url: String,
    requests: Arc<Mutex<Recorded>>,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

/// Start a mock replying to the schema route with `schema_status` and `schema_body`.
async fn start_mock(schema_status: u16, schema_body: &'static str) -> MockUc {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Recorded::new()));
    let (tx, mut rx) = tokio::sync::oneshot::channel();

    let recorded = Arc::clone(&requests);
    tokio::spawn(async move {
        loop {
            let sock = tokio::select! {
                a = listener.accept() => a,
                _ = &mut rx => break,
            };
            let Ok((mut sock, _)) = sock else { break };
            let recorded = Arc::clone(&recorded);
            tokio::spawn(async move {
                // The head is enough; the token request's small body follows the
                // client's Content-Length but we don't assert on it.
                let mut buf = vec![0u8; 4096];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let head = String::from_utf8_lossy(&buf[..n]);
                let target = head
                    .lines()
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .unwrap_or_default()
                    .to_string();
                let auth = header(&head, "authorization");
                recorded.lock().unwrap().push((target.clone(), auth));

                let (status, body) = if target.contains("/oidc/") {
                    (200, r#"{"access_token":"tok-123","expires_in":3600}"#)
                } else {
                    (schema_status, schema_body)
                };
                let resp = format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });

    MockUc {
        url,
        requests,
        _shutdown: tx,
    }
}

fn header(head: &str, name: &str) -> String {
    head.lines()
        .find(|l| l.to_ascii_lowercase().starts_with(&format!("{name}:")))
        .and_then(|l| l.split_once(':'))
        .map(|(_, v)| v.trim().to_string())
        .unwrap_or_default()
}

fn table_json() -> &'static str {
    r#"{"name":"orders","catalog_name":"main","schema_name":"sales","columns":[
        {"name":"id","type_name":"BIGINT","type_text":"bigint","type_json":"","nullable":false,"position":0},
        {"name":"customer","type_name":"STRING","type_text":"string","type_json":"","nullable":true,"position":1}
    ]}"#
}

#[tokio::test]
async fn fetches_descriptor_and_sends_expected_requests() {
    let mock = start_mock(200, table_json()).await;

    let descriptor = fetch_message_descriptor(&mock.url, TABLE, "cid", "csec")
        .await
        .expect("fetch should succeed");

    // Descriptor: message name is <schema>_<table>, field numbers are position + 1.
    assert_eq!(descriptor.name(), "SalesOrders");
    assert_eq!(descriptor.get_field_by_name("id").unwrap().number(), 1);
    assert_eq!(
        descriptor.get_field_by_name("customer").unwrap().number(),
        2
    );

    // Request shapes: Basic-auth token mint, then a bearer schema request at the
    // expected path.
    let reqs = mock.requests.lock().unwrap().clone();
    assert_eq!(reqs.len(), 2, "expected a token then a schema request");
    assert!(reqs[0].0.starts_with("/oidc/v1/token"), "got {}", reqs[0].0);
    assert!(reqs[0].1.starts_with("Basic "), "got {}", reqs[0].1);
    assert_eq!(reqs[1].0, format!("/api/2.1/unity-catalog/tables/{TABLE}"));
    assert_eq!(reqs[1].1, "Bearer tok-123");
}

#[tokio::test]
async fn schema_request_failure_is_schema_fetch_error() {
    let mock = start_mock(404, "table not found").await;

    match fetch_message_descriptor(&mock.url, TABLE, "cid", "csec").await {
        Err(ZerobusError::SchemaFetchError(msg)) => assert!(msg.contains("404"), "got: {msg}"),
        other => panic!("expected SchemaFetchError, got {other:?}"),
    }
}

#[tokio::test]
async fn invalid_table_name_fails_before_any_request() {
    let mock = start_mock(200, table_json()).await;

    match fetch_message_descriptor(&mock.url, "not.qualified", "cid", "csec").await {
        Err(ZerobusError::InvalidTableName(_)) => {}
        other => panic!("expected InvalidTableName, got {other:?}"),
    }
    assert!(
        mock.requests.lock().unwrap().is_empty(),
        "must not hit the network"
    );
}
