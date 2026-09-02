#[allow(dead_code)]
mod mock_arrow_flight;
mod mock_grpc;
mod utils;

use std::ffi::OsString;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use databricks_zerobus_ingest_sdk::{
    ConnectorFactory, NoTlsConfig, ProxyConnector, TlsConfig, ZerobusError, ZerobusResult,
    ZerobusSdk,
};
use mock_arrow_flight::{
    start_mock_flight_server, start_mock_tls_flight_server, MockFlightResponse,
};
use mock_grpc::{start_mock_server, start_mock_tls_server, MockResponse};
use rcgen::{generate_simple_self_signed, CertifiedKey};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::rustls::{pki_types::PrivatePkcs8KeyDer, ServerConfig};
use tokio_rustls::TlsAcceptor;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};
use tracing::info;
use utils::{
    create_test_arrow_schema, create_test_record_batch, setup_tracing, TestHeadersProvider,
};

const TABLE_NAME: &str = "test_catalog.test_schema.test_table";

static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct EnvVarGuard {
    name: &'static str,
    original: Option<OsString>,
}

impl EnvVarGuard {
    fn set(name: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
        let original = std::env::var_os(name);
        std::env::set_var(name, value);
        Self { name, original }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.original {
            Some(value) => std::env::set_var(self.name, value),
            None => std::env::remove_var(self.name),
        }
    }
}

#[derive(Debug)]
struct TestTlsConfig {
    ca_pem: Vec<u8>,
}

impl TlsConfig for TestTlsConfig {
    fn configure_endpoint(&self, endpoint: Endpoint) -> ZerobusResult<Endpoint> {
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&self.ca_pem))
            .domain_name("localhost");
        endpoint
            .tls_config(tls)
            .map_err(|_| ZerobusError::FailedToEstablishTlsConnectionError)
    }
}

/// Minimal HTTP CONNECT proxy for testing.
/// Tracks the number of CONNECT requests received.
async fn start_mock_proxy() -> (String, Arc<AtomicUsize>) {
    start_mock_proxy_inner("http", "127.0.0.1", None).await
}

/// Minimal HTTPS CONNECT proxy backed by a runtime-generated test identity.
async fn start_mock_tls_proxy() -> (String, Arc<AtomicUsize>, Vec<u8>) {
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_pem = cert.pem().into_bytes();
    let key = PrivatePkcs8KeyDer::from(key_pair.serialize_der()).into();
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.der().clone()], key)
        .unwrap();
    let (proxy_url, connect_count) = start_mock_proxy_inner(
        "https",
        "localhost",
        Some(TlsAcceptor::from(Arc::new(config))),
    )
    .await;
    (proxy_url, connect_count, cert_pem)
}

async fn start_mock_proxy_inner(
    scheme: &str,
    host: &str,
    tls_acceptor: Option<TlsAcceptor>,
) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    let proxy_url = format!("{}://{}:{}", scheme, host, proxy_addr.port());
    let connect_count = Arc::new(AtomicUsize::new(0));
    let count_clone = connect_count.clone();

    tokio::spawn(async move {
        loop {
            let (client, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };

            let count = Arc::clone(&count_clone);
            let tls_acceptor = tls_acceptor.clone();
            tokio::spawn(async move {
                if let Some(acceptor) = tls_acceptor {
                    if let Ok(client) = acceptor.accept(client).await {
                        handle_proxy_connection(client, count).await;
                    }
                } else {
                    handle_proxy_connection(client, count).await;
                }
            });
        }
    });

    (proxy_url, connect_count)
}

async fn handle_proxy_connection<S>(mut client: S, count: Arc<AtomicUsize>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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
}

/// Helper to create SDK, ingest one record, and close.
async fn ingest_one_record(server_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    ingest_one_record_with_tls(server_url, Arc::new(NoTlsConfig)).await
}

async fn ingest_one_record_with_tls(
    server_url: &str,
    tls_config: Arc<dyn TlsConfig>,
) -> Result<(), Box<dyn std::error::Error>> {
    let sdk = ZerobusSdk::builder()
        .endpoint(server_url)
        .unity_catalog_url("https://mock-uc.com")
        .tls_config(tls_config)
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
    stream.ingest_record_offset(json).await?;
    stream.flush().await?;
    stream.close().await?;

    Ok(())
}

/// Helper to create an Arrow stream, ingest one batch, flush, and close.
async fn ingest_one_arrow_batch(server_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    ingest_one_arrow_batch_with_tls(server_url, Arc::new(NoTlsConfig)).await
}

async fn ingest_one_arrow_batch_with_tls(
    server_url: &str,
    tls_config: Arc<dyn TlsConfig>,
) -> Result<(), Box<dyn std::error::Error>> {
    let sdk = ZerobusSdk::builder()
        .endpoint(server_url)
        .unity_catalog_url("https://mock-uc.com")
        .tls_config(tls_config)
        .build()?;

    ingest_one_arrow_batch_with_sdk(&sdk, false).await
}

async fn ingest_one_arrow_batch_with_sdk(
    sdk: &ZerobusSdk,
    recovery: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = create_test_arrow_schema();
    let mut stream = sdk
        .stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .arrow(schema.clone())
        .recovery(recovery)
        .recovery_retries(2)
        .recovery_backoff_ms(10)
        .flush_timeout_ms(5_000)
        .build_arrow()
        .await?;

    let batch = create_test_record_batch(schema, vec![1], vec![Some("proxy test")]);
    stream.ingest_batch(batch).await?;
    stream.flush().await?;
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
    let _env_lock = ENV_LOCK.lock().await;
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

    // === Part 3: Verify Arrow Flight traffic routes through proxy ===
    info!("=== Testing Arrow Flight proxy routing ===");
    {
        let (mock_server, server_url) = start_mock_flight_server().await?;
        mock_server.inject_responses(TABLE_NAME, vec![]).await;

        let (proxy_url, connect_count) = start_mock_proxy().await;
        std::env::set_var("grpc_proxy", &proxy_url);

        ingest_one_arrow_batch(&server_url).await?;

        std::env::remove_var("grpc_proxy");

        let connects = connect_count.load(Ordering::SeqCst);
        assert!(
            connects > 0,
            "Expected Arrow Flight to use the configured proxy, got {} CONNECT requests",
            connects
        );
    }

    // === Part 4: Verify no_grpc_proxy bypasses the proxy for Arrow Flight ===
    info!("=== Testing Arrow Flight no-proxy bypass ===");
    {
        let (mock_server, server_url) = start_mock_flight_server().await?;
        mock_server.inject_responses(TABLE_NAME, vec![]).await;

        let (proxy_url, connect_count) = start_mock_proxy().await;
        std::env::set_var("grpc_proxy", &proxy_url);
        std::env::set_var("no_grpc_proxy", "127.0.0.1, localhost");

        ingest_one_arrow_batch(&server_url).await?;

        std::env::remove_var("grpc_proxy");
        std::env::remove_var("no_grpc_proxy");

        let connects = connect_count.load(Ordering::SeqCst);
        assert_eq!(
            connects, 0,
            "Expected Arrow Flight to bypass the proxy via no_grpc_proxy"
        );
    }

    // === Part 5: Verify target TLS is preserved through the CONNECT tunnel ===
    info!("=== Testing Arrow Flight TLS through proxy ===");
    {
        let (mock_server, server_url, ca_pem) = start_mock_tls_flight_server().await?;
        mock_server.inject_responses(TABLE_NAME, vec![]).await;

        let (proxy_url, connect_count) = start_mock_proxy().await;
        std::env::set_var("grpc_proxy", &proxy_url);

        ingest_one_arrow_batch_with_tls(&server_url, Arc::new(TestTlsConfig { ca_pem })).await?;

        std::env::remove_var("grpc_proxy");

        assert!(
            connect_count.load(Ordering::SeqCst) > 0,
            "Expected TLS Arrow Flight traffic to use the configured proxy"
        );
    }

    // === Part 6: Verify JSON/protobuf target TLS through the CONNECT tunnel ===
    info!("=== Testing JSON/protobuf TLS through proxy ===");
    {
        let (mock_server, server_url, ca_pem) = start_mock_tls_server().await?;
        mock_server
            .inject_responses(TABLE_NAME, mock_responses())
            .await;

        let (proxy_url, connect_count) = start_mock_proxy().await;
        std::env::set_var("grpc_proxy", &proxy_url);

        ingest_one_record_with_tls(&server_url, Arc::new(TestTlsConfig { ca_pem })).await?;

        std::env::remove_var("grpc_proxy");

        assert!(
            connect_count.load(Ordering::SeqCst) > 0,
            "Expected TLS JSON/protobuf traffic to use the configured proxy"
        );
    }

    // === Part 7: Verify env-var proxy selection is reused during recovery ===
    info!("=== Testing Arrow Flight env proxy recovery ===");
    {
        let (mock_server, server_url) = start_mock_flight_server().await?;
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockFlightResponse::Error {
                        status: tonic::Status::unavailable("trigger recovery"),
                        delay_ms: 0,
                    },
                    MockFlightResponse::BatchAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                        ack_up_to_records: 1,
                    },
                ],
            )
            .await;

        let (proxy_url, connect_count) = start_mock_proxy().await;
        std::env::set_var("grpc_proxy", &proxy_url);

        let sdk = ZerobusSdk::builder()
            .endpoint(&server_url)
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(NoTlsConfig))
            .build()?;
        ingest_one_arrow_batch_with_sdk(&sdk, true).await?;

        std::env::remove_var("grpc_proxy");

        assert!(
            connect_count.load(Ordering::SeqCst) >= 2,
            "Expected initial and recovered Arrow Flight connections through the env proxy"
        );
    }

    // === Part 8: Verify TLS on the client-to-proxy hop ===
    info!("=== Testing Arrow Flight through an HTTPS proxy ===");
    {
        let (mock_server, server_url) = start_mock_flight_server().await?;
        mock_server.inject_responses(TABLE_NAME, vec![]).await;

        let (proxy_url, connect_count, ca_pem) = start_mock_tls_proxy().await;
        let mut ca_file = tempfile::NamedTempFile::new()?;
        ca_file.write_all(&ca_pem)?;
        let _ca_env = EnvVarGuard::set("SSL_CERT_FILE", ca_file.path());
        let _proxy = EnvVarGuard::set("grpc_proxy", &proxy_url);

        ingest_one_arrow_batch(&server_url).await?;

        assert!(
            connect_count.load(Ordering::SeqCst) > 0,
            "Expected Arrow Flight to establish CONNECT over TLS to the HTTPS proxy"
        );
    }

    // === Part 9: Verify invalid proxy configuration fails closed ===
    info!("=== Testing invalid Arrow Flight proxy configuration ===");
    {
        let (mock_server, server_url) = start_mock_flight_server().await?;
        mock_server.inject_responses(TABLE_NAME, vec![]).await;

        let _proxy = EnvVarGuard::set("grpc_proxy", "http://proxy-user:super-secret@/proxy");
        let error = match ingest_one_arrow_batch(&server_url).await {
            Ok(()) => panic!("expected invalid proxy configuration to fail"),
            Err(error) => error.to_string(),
        };

        assert!(error.contains("failed to parse proxy URL"));
        assert!(!error.contains("proxy-user"));
        assert!(!error.contains("super-secret"));
    }

    Ok(())
}

#[tokio::test]
async fn test_arrow_custom_connector_factory_is_reused_for_recovery(
) -> Result<(), Box<dyn std::error::Error>> {
    let _env_lock = ENV_LOCK.lock().await;
    setup_tracing();

    let (mock_server, server_url) = start_mock_flight_server().await?;
    mock_server
        .inject_responses(
            TABLE_NAME,
            vec![
                MockFlightResponse::Error {
                    status: tonic::Status::unavailable("trigger recovery"),
                    delay_ms: 0,
                },
                MockFlightResponse::BatchAck {
                    ack_up_to_offset: 0,
                    delay_ms: 0,
                    ack_up_to_records: 1,
                },
            ],
        )
        .await;

    let (proxy_url, connect_count) = start_mock_proxy().await;
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&factory_calls);
    let connector_factory: ConnectorFactory = Arc::new(move |host| {
        assert_eq!(host, "127.0.0.1");
        calls.fetch_add(1, Ordering::SeqCst);
        Some(ProxyConnector::new(&proxy_url).expect("valid mock proxy URL"))
    });

    let sdk = ZerobusSdk::builder()
        .endpoint(&server_url)
        .unity_catalog_url("https://mock-uc.com")
        .tls_config(Arc::new(NoTlsConfig))
        .connector_factory(connector_factory)
        .build()?;

    ingest_one_arrow_batch_with_sdk(&sdk, true).await?;

    assert!(
        factory_calls.load(Ordering::SeqCst) >= 2,
        "Expected the connector factory for initial connect and recovery"
    );
    assert!(
        connect_count.load(Ordering::SeqCst) >= 2,
        "Expected initial and recovered Arrow Flight connections through the proxy"
    );

    Ok(())
}

#[tokio::test]
async fn test_arrow_custom_connector_factory_is_reused_for_recreation(
) -> Result<(), Box<dyn std::error::Error>> {
    let _env_lock = ENV_LOCK.lock().await;
    setup_tracing();

    let (mock_server, server_url) = start_mock_flight_server().await?;
    mock_server.inject_responses(TABLE_NAME, vec![]).await;

    let (proxy_url, connect_count) = start_mock_proxy().await;
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&factory_calls);
    let connector_factory: ConnectorFactory = Arc::new(move |host| {
        assert_eq!(host, "127.0.0.1");
        calls.fetch_add(1, Ordering::SeqCst);
        Some(ProxyConnector::new(&proxy_url).expect("valid mock proxy URL"))
    });

    let sdk = ZerobusSdk::builder()
        .endpoint(&server_url)
        .unity_catalog_url("https://mock-uc.com")
        .tls_config(Arc::new(NoTlsConfig))
        .connector_factory(connector_factory)
        .build()?;

    let schema = create_test_arrow_schema();
    let mut stream = sdk
        .stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .arrow(schema)
        .build_arrow()
        .await?;
    stream.close().await?;

    let mut recreated = sdk.recreate_arrow_stream(&stream).await?;
    recreated.close().await?;

    assert_eq!(
        factory_calls.load(Ordering::SeqCst),
        2,
        "Expected the connector factory for the original and recreated streams"
    );
    assert_eq!(
        connect_count.load(Ordering::SeqCst),
        2,
        "Expected the original and recreated Arrow streams through the proxy"
    );

    Ok(())
}
