mod mock_grpc;
mod utils;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use databricks_zerobus_ingest_sdk::{NoTlsConfig, ZerobusSdk};
use mock_grpc::{start_mock_server, MockResponse};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::info;
use utils::{setup_tracing, TestHeadersProvider};

const TABLE_NAME: &str = "test_catalog.test_schema.test_table";

/// Minimal HTTP CONNECT proxy for testing.
/// Tracks the number of CONNECT requests received.
async fn start_mock_proxy() -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    let proxy_url = format!("http://{}", proxy_addr);
    let connect_count = Arc::new(AtomicUsize::new(0));
    let count_clone = connect_count.clone();

    tokio::spawn(async move {
        loop {
            let (mut client, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };

            let count = count_clone.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let n = match client.read(&mut buf).await {
                    Ok(n) if n > 0 => n,
                    _ => return,
                };

                let request = String::from_utf8_lossy(&buf[..n]);
                if !request.starts_with("CONNECT ") {
                    return;
                }

                let target = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("");

                info!("[mock-proxy] CONNECT {}", target);
                count.fetch_add(1, Ordering::SeqCst);

                let mut upstream = match tokio::net::TcpStream::connect(target).await {
                    Ok(s) => s,
                    Err(e) => {
                        info!("[mock-proxy] Failed to connect to {}: {}", target, e);
                        let _ = client.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
                        return;
                    }
                };

                if client
                    .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    .await
                    .is_err()
                {
                    return;
                }

                let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
            });
        }
    });

    (proxy_url, connect_count)
}

/// Helper to create SDK, ingest one record, and close.
async fn ingest_one_record(server_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let sdk = ZerobusSdk::builder()
        .endpoint(server_url)
        .unity_catalog_url("https://mock-uc.com")
        .tls_config(Arc::new(NoTlsConfig))
        .build()?;

    let mut stream = sdk
        .stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .json()
        .max_inflight_requests(10)
        .recovery(false)
        .build()
        .await?;

    let json = r#"{"id": 1, "message": "proxy test"}"#.to_string();
    let offset = stream.ingest_record_offset(json).await?;
    stream.wait_for_offset(offset).await?;
    stream.close().await?;

    Ok(())
}

fn mock_responses() -> Vec<MockResponse> {
    vec![
        MockResponse::CreateStream {
            stream_id: "proxy_test_stream".to_string(),
            delay_ms: 0,
        },
        MockResponse::RecordAck {
            ack_up_to_offset: 0,
            delay_ms: 0,
        },
    ]
}

/// Single test function to avoid env var races between parallel tests.
/// Tests both proxy routing and no_proxy bypass sequentially.
#[tokio::test]
async fn test_proxy_and_no_proxy() -> Result<(), Box<dyn std::error::Error>> {
    setup_tracing();

    // === Part 1: Verify traffic routes through proxy ===
    info!("=== Testing proxy routing ===");
    {
        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(TABLE_NAME, mock_responses())
            .await;

        let (proxy_url, connect_count) = start_mock_proxy().await;
        info!(
            "Mock proxy at: {}, mock server at: {}",
            proxy_url, server_url
        );

        std::env::set_var("grpc_proxy", &proxy_url);

        ingest_one_record(&server_url).await?;

        std::env::remove_var("grpc_proxy");

        let connects = connect_count.load(Ordering::SeqCst);
        info!("Proxy received {} CONNECT requests", connects);
        assert!(
            connects > 0,
            "Expected proxy to receive CONNECT requests, got 0"
        );
    }

    // === Part 2: Verify no_proxy bypasses the proxy ===
    info!("=== Testing no_proxy bypass ===");
    {
        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(TABLE_NAME, mock_responses())
            .await;

        let (proxy_url, connect_count) = start_mock_proxy().await;

        std::env::set_var("grpc_proxy", &proxy_url);
        std::env::set_var("no_grpc_proxy", "127.0.0.1, localhost");

        ingest_one_record(&server_url).await?;

        std::env::remove_var("grpc_proxy");
        std::env::remove_var("no_grpc_proxy");

        let connects = connect_count.load(Ordering::SeqCst);
        info!("Proxy received {} CONNECT requests (expected 0)", connects);
        assert_eq!(
            connects, 0,
            "Expected proxy to receive 0 CONNECT requests (bypassed via no_proxy), got {}",
            connects
        );
    }

    Ok(())
}
