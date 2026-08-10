//! A minimal HTTP mock of the Unity Catalog endpoints the SDK's schema fetch
//! uses: `POST /oidc/v1/token` and `GET /api/2.1/unity-catalog/tables/{name}`.
//!
//! Hand-rolled over a `TcpListener` rather than pulling in an HTTP mock crate:
//! the fetch path only needs these two routes, and the tests assert on the raw
//! request line and headers.

#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// How a route should respond.
#[derive(Clone, Debug)]
pub enum MockReply {
    /// `200` with this JSON body.
    Json(String),
    /// This status with this body (used for error classification tests).
    Status(u16, String),
    /// A `200` JSON body, but with a `Content-Length` claiming `usize` bytes so
    /// the client's size guard is exercised without sending that much data.
    OverlongContentLength(usize),
    /// Accept the connection, then drop it without replying.
    Hangup,
}

/// What the mock recorded about the requests it served.
#[derive(Default, Debug)]
pub struct MockRequests {
    /// Request target of every schema request, in order (e.g.
    /// `/api/2.1/unity-catalog/tables/c.s.t`).
    pub schema_paths: Mutex<Vec<String>>,
    /// `authorization` header of every schema request, in order.
    pub schema_auth: Mutex<Vec<String>>,
    /// `authorization` header of every token request, in order.
    pub token_auth: Mutex<Vec<String>>,
    /// Body of every token request, in order.
    pub token_bodies: Mutex<Vec<String>>,
    pub token_calls: AtomicUsize,
    pub schema_calls: AtomicUsize,
}

/// A running mock. Dropping it stops the accept loop.
pub struct MockUc {
    pub url: String,
    pub requests: Arc<MockRequests>,
    shutdown: tokio::sync::oneshot::Sender<()>,
}

impl MockUc {
    pub fn token_calls(&self) -> usize {
        self.requests.token_calls.load(Ordering::SeqCst)
    }

    pub fn schema_calls(&self) -> usize {
        self.requests.schema_calls.load(Ordering::SeqCst)
    }

    /// Stop serving. Not required — dropping the handle does the same.
    pub fn stop(self) {
        let _ = self.shutdown.send(());
    }
}

/// Start a mock serving `token_reply` on the token route and `schema_reply` on
/// the table route, bound to an ephemeral loopback port.
pub async fn start_mock_uc(token_reply: MockReply, schema_reply: MockReply) -> MockUc {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}");
    let requests = Arc::new(MockRequests::default());
    let (shutdown, mut shutdown_rx) = tokio::sync::oneshot::channel();

    let served = Arc::clone(&requests);
    tokio::spawn(async move {
        loop {
            let accepted = tokio::select! {
                accepted = listener.accept() => accepted,
                _ = &mut shutdown_rx => break,
            };
            let Ok((socket, _)) = accepted else { break };

            let served = Arc::clone(&served);
            let token_reply = token_reply.clone();
            let schema_reply = schema_reply.clone();
            tokio::spawn(async move {
                handle_connection(socket, served, token_reply, schema_reply).await;
            });
        }
    });

    MockUc {
        url,
        requests,
        shutdown,
    }
}

/// Convenience: a mock that mints `token` and serves `columns_json` as the
/// table's `columns` array.
pub async fn start_mock_uc_serving_columns(token: &str, columns_json: &str) -> MockUc {
    start_mock_uc(
        MockReply::Json(format!(r#"{{"access_token":"{token}","expires_in":3600}}"#)),
        MockReply::Json(table_response(columns_json)),
    )
    .await
}

/// A UC `tables/{name}` response body wrapping `columns_json`.
pub fn table_response(columns_json: &str) -> String {
    format!(
        r#"{{"name":"orders","catalog_name":"main","schema_name":"sales","columns":{columns_json}}}"#
    )
}

/// Two simple columns: `id` (BIGINT, non-null) and `customer` (STRING, nullable).
pub fn simple_columns_json() -> &'static str {
    r#"[
        {"name":"id","type_name":"BIGINT","type_text":"bigint","type_json":"","nullable":false,"position":0},
        {"name":"customer","type_name":"STRING","type_text":"string","type_json":"","nullable":true,"position":1}
    ]"#
}

async fn handle_connection(
    mut socket: tokio::net::TcpStream,
    requests: Arc<MockRequests>,
    token_reply: MockReply,
    schema_reply: MockReply,
) {
    let Some(request) = read_request(&mut socket).await else {
        return;
    };

    let is_token = request.target.starts_with("/oidc/v1/token");
    if is_token {
        requests.token_calls.fetch_add(1, Ordering::SeqCst);
        requests
            .token_auth
            .lock()
            .unwrap()
            .push(request.authorization.clone());
        requests.token_bodies.lock().unwrap().push(request.body);
    } else {
        requests.schema_calls.fetch_add(1, Ordering::SeqCst);
        requests
            .schema_paths
            .lock()
            .unwrap()
            .push(request.target.clone());
        requests
            .schema_auth
            .lock()
            .unwrap()
            .push(request.authorization.clone());
    }

    let reply = if is_token { token_reply } else { schema_reply };
    let response = match reply {
        MockReply::Json(body) => http_response(200, "application/json", &body),
        MockReply::Status(status, body) => http_response(status, "text/plain", &body),
        MockReply::OverlongContentLength(claimed) => {
            // Claim a huge body but send a tiny one: the client must reject on the
            // advertised length rather than reading to completion.
            format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {claimed}\r\n\r\n{{}}"
            )
        }
        MockReply::Hangup => return,
    };

    let _ = socket.write_all(response.as_bytes()).await;
    let _ = socket.flush().await;
}

struct MockRequest {
    target: String,
    authorization: String,
    body: String,
}

/// Read one request: the head, then `Content-Length` bytes of body.
async fn read_request(socket: &mut tokio::net::TcpStream) -> Option<MockRequest> {
    let mut raw = Vec::new();
    let mut buf = [0u8; 1024];

    // Read until the end of the head.
    let head_end = loop {
        let n = socket.read(&mut buf).await.ok()?;
        if n == 0 {
            return None;
        }
        raw.extend_from_slice(&buf[..n]);
        if let Some(pos) = find_head_end(&raw) {
            break pos;
        }
        if raw.len() > 64 * 1024 {
            return None;
        }
    };

    let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
    let mut lines = head.lines();
    let target = lines
        .next()?
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string();

    let header = |name: &str| -> Option<String> {
        head.lines()
            .filter(|l| {
                l.split(':')
                    .next()
                    .is_some_and(|k| k.trim().eq_ignore_ascii_case(name))
            })
            .map(|l| l.split_once(':').map(|(_, v)| v.trim().to_string()))
            .next()
            .flatten()
    };
    let authorization = header("authorization").unwrap_or_default();
    let content_length: usize = header("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    // The body may already be (partly) buffered with the head.
    let mut body = raw[head_end..].to_vec();
    while body.len() < content_length {
        let n = socket.read(&mut buf).await.ok()?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&buf[..n]);
    }
    body.truncate(content_length);

    Some(MockRequest {
        target,
        authorization,
        body: String::from_utf8_lossy(&body).to_string(),
    })
}

/// Index just past the blank line ending the request head.
fn find_head_end(raw: &[u8]) -> Option<usize> {
    raw.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
}

fn http_response(status: u16, content_type: &str, body: &str) -> String {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Status",
    };
    format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}
