use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use databricks_zerobus_ingest_sdk::{HeadersProvider, OAuthHeadersProvider};

struct MockResponse {
    status: &'static str,
    body: &'static str,
}

fn read_request(stream: &mut TcpStream) {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let mut request = Vec::new();
    let mut buffer = [0u8; 4096];
    let (header_end, content_length) = loop {
        let read = stream.read(&mut buffer).unwrap();
        assert!(read > 0, "client closed before sending complete headers");
        request.extend_from_slice(&buffer[..read]);

        if let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            let header_end = header_end + 4;
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            break (header_end, content_length);
        }
    };

    while request.len() < header_end + content_length {
        let read = stream.read(&mut buffer).unwrap();
        assert!(read > 0, "client closed before sending complete body");
        request.extend_from_slice(&buffer[..read]);
    }
}

fn start_token_server(responses: Vec<MockResponse>) -> (String, JoinHandle<usize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let handle = thread::spawn(move || {
        let mut served = 0;
        for response in &responses {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break Some(stream),
                    Err(error)
                        if error.kind() == ErrorKind::WouldBlock && Instant::now() < deadline =>
                    {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => break None,
                    Err(error) => panic!("token server accept failed: {error}"),
                }
            };
            let Some(mut stream) = stream.take() else {
                break;
            };
            stream.set_nonblocking(false).unwrap();
            read_request(&mut stream);
            write!(
                stream,
                "HTTP/1.1 {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                response.status,
                response.body.len(),
                response.body
            )
            .unwrap();
            stream.flush().unwrap();
            served += 1;
        }
        served
    });
    (url, handle)
}

fn provider(unity_catalog_url: String) -> OAuthHeadersProvider {
    OAuthHeadersProvider::new(
        "client-id".to_string(),
        "client-secret".to_string(),
        "catalog.schema.table".to_string(),
        "workspace-id".to_string(),
        unity_catalog_url,
    )
}

#[tokio::test]
async fn string_expires_in_is_cached_across_header_requests() {
    let (url, server) = start_token_server(vec![MockResponse {
        status: "200 OK",
        body: r#"{"access_token":"cached-token","expires_in":"3600"}"#,
    }]);
    let provider = provider(url);

    let first = provider.get_headers().await.unwrap();
    let second = provider.get_headers().await.unwrap();

    assert_eq!(first["authorization"], "Bearer cached-token");
    assert_eq!(second["authorization"], "Bearer cached-token");
    assert_eq!(
        server.join().unwrap(),
        1,
        "second request must hit the cache"
    );
}

#[tokio::test]
async fn non_retryable_refresh_failure_uses_still_valid_token() {
    let (url, server) = start_token_server(vec![
        MockResponse {
            status: "200 OK",
            // The default five-minute refresh buffer makes this token due for
            // proactive refresh immediately, while it remains valid for 30s.
            body: r#"{"access_token":"still-valid","expires_in":"30"}"#,
        },
        MockResponse {
            status: "400 Bad Request",
            body: r#"{"error":"invalid_request"}"#,
        },
    ]);
    let provider = provider(url);

    let first = provider.get_headers().await.unwrap();
    let second = provider.get_headers().await.unwrap();

    assert_eq!(first["authorization"], "Bearer still-valid");
    assert_eq!(second["authorization"], "Bearer still-valid");
    assert_eq!(
        server.join().unwrap(),
        2,
        "second request must attempt refresh"
    );
}

#[tokio::test]
async fn rate_limit_on_cold_cache_is_retryable() {
    let (url, server) = start_token_server(vec![MockResponse {
        status: "429 Too Many Requests",
        body: r#"{"error":"rate_limited"}"#,
    }]);
    let provider = provider(url);

    let error = provider.get_headers().await.unwrap_err();
    assert!(error.is_retryable());
    assert_eq!(server.join().unwrap(), 1);
}
