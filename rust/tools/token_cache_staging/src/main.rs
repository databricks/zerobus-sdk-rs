use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use databricks_zerobus_ingest_sdk::{ZerobusSdk, ZerobusStream};
use reqwest::header::CONTENT_TYPE;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const MAX_REQUEST_BYTES: usize = 1024 * 1024;

struct TokenProxy {
    url: String,
    request_count: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl TokenProxy {
    async fn start(
        workspace_url: String,
        client_id: String,
        client_secret: String,
    ) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("bind loopback OAuth proxy")?;
        let url = format!("http://{}", listener.local_addr()?);
        let request_count = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&request_count);
        let client = reqwest::Client::new();

        let task = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(connection) => connection,
                    Err(error) => {
                        eprintln!("OAuth proxy accept failed: {error}");
                        return;
                    }
                };
                let request_number = count.fetch_add(1, Ordering::SeqCst) + 1;
                let workspace_url = workspace_url.clone();
                let client_id = client_id.clone();
                let client_secret = client_secret.clone();
                let client = client.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_token_request(
                        stream,
                        request_number,
                        &workspace_url,
                        &client_id,
                        &client_secret,
                        &client,
                    )
                    .await
                    {
                        eprintln!("OAuth proxy request {request_number} failed: {error:#}");
                    }
                });
            }
        });

        Ok(Self {
            url,
            request_count,
            task,
        })
    }

    fn requests(&self) -> usize {
        self.request_count.load(Ordering::SeqCst)
    }
}

impl Drop for TokenProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handle_token_request(
    mut stream: TcpStream,
    request_number: usize,
    workspace_url: &str,
    client_id: &str,
    client_secret: &str,
    client: &reqwest::Client,
) -> Result<()> {
    let request_body = read_request_body(&mut stream).await?;

    if request_number == 1 {
        let token_url = format!("{}/oidc/v1/token", workspace_url.trim_end_matches('/'));
        let response = client
            .post(token_url)
            .basic_auth(client_id, Some(client_secret))
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(request_body)
            .send()
            .await
            .context("forward token request to staging workspace")?;
        let status = response.status();
        let response_body = response
            .bytes()
            .await
            .context("read staging token response")?;

        if !status.is_success() {
            let status_line = format!(
                "{} {}",
                status.as_u16(),
                status.canonical_reason().unwrap_or("Upstream Error")
            );
            return write_response(&mut stream, &status_line, &response_body).await;
        }

        let mut body: Value =
            serde_json::from_slice(&response_body).context("parse staging token response")?;
        let expires_in = body
            .get_mut("expires_in")
            .context("staging token response did not contain expires_in")?;
        let seconds = match expires_in {
            Value::Number(value) => value.to_string(),
            Value::String(value) => value.clone(),
            value => bail!("staging expires_in was not an integer: {value}"),
        };
        *expires_in = Value::String(seconds);
        let rewritten = serde_json::to_vec(&body)?;
        write_response(&mut stream, "200 OK", &rewritten).await
    } else {
        write_response(
            &mut stream,
            "429 Too Many Requests",
            br#"{"error":"injected_refresh_failure"}"#,
        )
        .await
    }
}

async fn read_request_body(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut request = Vec::new();
    let mut buffer = [0u8; 4096];

    let (header_end, content_length) = loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            bail!("client closed before sending complete headers");
        }
        request.extend_from_slice(&buffer[..read]);
        if request.len() > MAX_REQUEST_BYTES {
            bail!("token request exceeded {MAX_REQUEST_BYTES} bytes");
        }

        if let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            let header_end = header_end + 4;
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>())
                })
                .transpose()
                .context("invalid content-length")?
                .context("token request did not include content-length")?;
            break (header_end, content_length);
        }
    };

    if header_end + content_length > MAX_REQUEST_BYTES {
        bail!("token request exceeded {MAX_REQUEST_BYTES} bytes");
    }
    while request.len() < header_end + content_length {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            bail!("client closed before sending complete body");
        }
        request.extend_from_slice(&buffer[..read]);
    }

    Ok(request[header_end..header_end + content_length].to_vec())
}

async fn write_response(stream: &mut TcpStream, status: &str, body: &[u8]) -> Result<()> {
    let headers = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(headers.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn open_and_close_stream(
    sdk: &ZerobusSdk,
    table_name: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<()> {
    let mut stream: ZerobusStream = sdk
        .stream_builder()
        .table(table_name)
        .oauth(client_id, client_secret)
        .json()
        .recovery(false)
        .build()
        .await
        .context("open staging Zerobus stream")?;
    stream.close().await.context("close staging Zerobus stream")
}

fn required_env(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("required environment variable {name} is not set"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let client_id = required_env("DATABRICKS_CLIENT_ID")?;
    let client_secret = required_env("DATABRICKS_CLIENT_SECRET")?;
    let workspace_url = required_env("DATABRICKS_WORKSPACE_URL")?;
    let zerobus_endpoint = required_env("ZEROBUS_ENDPOINT")?;
    let table_name = required_env("DATABRICKS_TABLE_NAME")?;

    let proxy = TokenProxy::start(workspace_url, client_id.clone(), client_secret.clone()).await?;
    let sdk = ZerobusSdk::builder()
        .endpoint(zerobus_endpoint)
        .unity_catalog_url(&proxy.url)
        // Force every cache hit into the proactive refresh path. The first
        // token remains valid; only the lead-time calculation overflows.
        .token_refresh_buffer(Duration::MAX)
        .build()?;

    println!("Opening first staging stream with a string-valued expires_in...");
    open_and_close_stream(&sdk, &table_name, &client_id, &client_secret).await?;
    if proxy.requests() != 1 {
        bail!(
            "expected one OAuth request after first stream, observed {}",
            proxy.requests()
        );
    }

    println!(
        "Opening two more staging streams concurrently after an injected HTTP 429 refresh failure..."
    );
    tokio::try_join!(
        open_and_close_stream(&sdk, &table_name, &client_id, &client_secret),
        open_and_close_stream(&sdk, &table_name, &client_id, &client_secret),
    )?;
    if proxy.requests() != 2 {
        bail!(
            "expected concurrent refresh-failure backoff to keep OAuth requests at two, observed {}",
            proxy.requests()
        );
    }

    println!(
        "PASS: quoted expires_in was cached, the valid token survived failed refresh, and the concurrent stream did not repeat the refresh"
    );
    Ok(())
}
