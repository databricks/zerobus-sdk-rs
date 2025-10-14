use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

pub mod databricks {
    pub mod zerobus {
        tonic::include_proto!("databricks.zerobus");
    }
}
use databricks::zerobus::{
    ephemeral_stream_request::Payload as RequestPayload,
    ephemeral_stream_response::Payload as ResponsePayload,
    zerobus_server::{Zerobus, ZerobusServer},
    CloseStreamSignal, CreateIngestStreamResponse, EphemeralStreamRequest, EphemeralStreamResponse,
    IngestRecordResponse,
};
use prost_types::Duration as ProtobufDuration;
use tokio::sync::{mpsc, Mutex};
use tokio::time::sleep;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};

/// Mock response that can be injected into the mock server
#[derive(Debug, Clone)]
pub enum MockResponse {
    /// Successful create stream response
    CreateStream { stream_id: String, delay_ms: u64 },
    /// Successful record acknowledgment
    RecordAck {
        ack_up_to_offset: i64,
        delay_ms: u64,
    },
    /// Close stream signal
    #[allow(dead_code)]
    CloseStreamSignal {
        duration_seconds: i64,
        delay_ms: u64,
    },
    /// Error response
    Error { status: Status, delay_ms: u64 },
}

/// Mock gRPC server for testing the Rust SDK
pub struct MockZerobusServer {
    /// Responses to inject for each stream
    responses: Arc<Mutex<HashMap<String, Vec<MockResponse>>>>,
    /// Counter for generating unique stream IDs
    stream_counter: Arc<Mutex<u32>>,
    /// Track the maximum offset sent by clients
    max_offset_sent: Arc<Mutex<i64>>,
    /// Track number of writes received
    write_count: Arc<Mutex<u64>>,
    /// Track response index across multiple connection attempts
    response_indices: Arc<Mutex<HashMap<String, usize>>>,
}

impl MockZerobusServer {
    pub fn new() -> Self {
        Self {
            responses: Arc::new(Mutex::new(HashMap::new())),
            stream_counter: Arc::new(Mutex::new(0)),
            max_offset_sent: Arc::new(Mutex::new(-1)),
            write_count: Arc::new(Mutex::new(0)),
            response_indices: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Inject responses for a specific stream (identified by table name for simplicity)
    pub async fn inject_responses(&self, table_name: &str, responses: Vec<MockResponse>) {
        let mut response_map = self.responses.lock().await;
        response_map.insert(table_name.to_string(), responses);

        let mut indices = self.response_indices.lock().await;
        indices.insert(table_name.to_string(), 0);
    }

    /// Get the maximum offset sent by clients
    pub async fn get_max_offset_sent(&self) -> i64 {
        *self.max_offset_sent.lock().await
    }

    /// Get the number of writes received
    pub async fn get_write_count(&self) -> u64 {
        *self.write_count.lock().await
    }

    /// Reset the server state
    #[allow(dead_code)]
    pub async fn reset(&self) {
        let mut responses = self.responses.lock().await;
        responses.clear();
        let mut indices = self.response_indices.lock().await;
        indices.clear();
        *self.max_offset_sent.lock().await = -1;
        *self.write_count.lock().await = 0;
        *self.stream_counter.lock().await = 0;
    }
}

#[tonic::async_trait]
impl Zerobus for MockZerobusServer {
    type EphemeralStreamStream =
        Pin<Box<dyn Stream<Item = Result<EphemeralStreamResponse, Status>> + Send>>;

    async fn ephemeral_stream(
        &self,
        request: Request<Streaming<EphemeralStreamRequest>>,
    ) -> Result<Response<Self::EphemeralStreamStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(100);

        let responses = Arc::clone(&self.responses);
        let stream_counter = Arc::clone(&self.stream_counter);
        let max_offset_sent = Arc::clone(&self.max_offset_sent);
        let write_count = Arc::clone(&self.write_count);
        let response_indices = Arc::clone(&self.response_indices);

        tokio::spawn(async move {
            let table_name;
            let stream_id;
            let mut stream_responses: Vec<MockResponse> = Vec::new();
            let mut response_index = 0;

            if let Some(request_result) = stream.message().await.transpose() {
                match request_result {
                    Ok(request) => {
                        if let Some(RequestPayload::CreateStream(create_request)) = request.payload
                        {
                            table_name = create_request.table_name.unwrap_or_default();
                            info!("Received CreateStream request for table: {}", table_name);

                            {
                                let mut counter = stream_counter.lock().await;
                                *counter += 1;
                                stream_id = format!("test_stream_{}", *counter);
                            }
                            debug!("Generated stream ID: {}", stream_id);

                            {
                                let response_map = responses.lock().await;
                                if let Some(responses) = response_map.get(&table_name) {
                                    stream_responses = responses.clone();
                                } else {
                                    warn!(
                                        "No configured responses found for table: {}",
                                        table_name
                                    );
                                }
                            }

                            {
                                let indices = response_indices.lock().await;
                                response_index = *indices.get(&table_name).unwrap_or(&0);
                            }

                            if let Some(mock_response) = stream_responses.get(response_index) {
                                match mock_response {
                                    MockResponse::CreateStream {
                                        stream_id: custom_id,
                                        delay_ms,
                                    } => {
                                        if *delay_ms > 0 {
                                            sleep(Duration::from_millis(*delay_ms)).await;
                                        }
                                        info!(
                                            "Sending CreateStream response with stream_id: {}",
                                            custom_id
                                        );
                                        let response = EphemeralStreamResponse {
                                            payload: Some(ResponsePayload::CreateStreamResponse(
                                                CreateIngestStreamResponse {
                                                    stream_id: Some(custom_id.clone()),
                                                },
                                            )),
                                        };
                                        if tx.send(Ok(response)).await.is_err() {
                                            return;
                                        }
                                        response_index += 1;

                                        {
                                            let mut indices = response_indices.lock().await;
                                            indices.insert(table_name.clone(), response_index);
                                        }
                                    }
                                    MockResponse::Error { status, delay_ms } => {
                                        if *delay_ms > 0 {
                                            sleep(Duration::from_millis(*delay_ms)).await;
                                        }
                                        info!(
                                            "Sending error response at index {}: {:?}",
                                            response_index, status
                                        );

                                        {
                                            let mut indices = response_indices.lock().await;
                                            indices.insert(table_name.clone(), response_index + 1);
                                        }

                                        let _ = tx.send(Err(status.clone())).await;
                                        return;
                                    }
                                    _ => {
                                        let response = EphemeralStreamResponse {
                                            payload: Some(ResponsePayload::CreateStreamResponse(
                                                CreateIngestStreamResponse {
                                                    stream_id: Some(stream_id.clone()),
                                                },
                                            )),
                                        };
                                        if tx.send(Ok(response)).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                            } else {
                                let response = EphemeralStreamResponse {
                                    payload: Some(ResponsePayload::CreateStreamResponse(
                                        CreateIngestStreamResponse {
                                            stream_id: Some(stream_id.clone()),
                                        },
                                    )),
                                };
                                if tx.send(Ok(response)).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        return;
                    }
                }
            }

            while let Some(request_result) = stream.message().await.transpose() {
                match request_result {
                    Ok(request) => {
                        if let Some(RequestPayload::IngestRecord(ingest_request)) = request.payload
                        {
                            debug!(
                                "Received IngestRecord request with offset_id: {:?}",
                                ingest_request.offset_id
                            );
                            if let Some(offset_id) = ingest_request.offset_id {
                                let mut max_offset = max_offset_sent.lock().await;
                                if offset_id > *max_offset {
                                    *max_offset = offset_id;
                                }
                            }

                            {
                                let mut count = write_count.lock().await;
                                *count += 1;
                                debug!("Incremented write count to: {}", *count);
                            }

                            if response_index < stream_responses.len() {
                                match &stream_responses[response_index] {
                                    MockResponse::RecordAck {
                                        ack_up_to_offset,
                                        delay_ms,
                                    } => {
                                        if ingest_request.offset_id == Some(*ack_up_to_offset) {
                                            if *delay_ms > 0 {
                                                sleep(Duration::from_millis(*delay_ms)).await;
                                            }
                                            info!(
                                                "Sending RecordAck response with ack_up_to_offset: {}",
                                                ack_up_to_offset
                                            );
                                            let response = EphemeralStreamResponse {
                                                payload: Some(
                                                    ResponsePayload::IngestRecordResponse(
                                                        IngestRecordResponse {
                                                            durability_ack_up_to_offset: Some(
                                                                *ack_up_to_offset,
                                                            ),
                                                        },
                                                    ),
                                                ),
                                            };
                                            if tx.send(Ok(response)).await.is_err() {
                                                return;
                                            }
                                            response_index += 1;
                                        }
                                    }
                                    MockResponse::CloseStreamSignal {
                                        duration_seconds,
                                        delay_ms,
                                    } => {
                                        if *delay_ms > 0 {
                                            sleep(Duration::from_millis(*delay_ms)).await;
                                        }
                                        info!(
                                            "Sending CloseStreamSignal with duration: {}s",
                                            duration_seconds
                                        );
                                        let response = EphemeralStreamResponse {
                                            payload: Some(ResponsePayload::CloseStreamSignal(
                                                CloseStreamSignal {
                                                    duration: Some(ProtobufDuration {
                                                        seconds: *duration_seconds,
                                                        nanos: 0,
                                                    }),
                                                },
                                            )),
                                        };
                                        if tx.send(Ok(response)).await.is_err() {
                                            return;
                                        }
                                        response_index += 1;
                                    }
                                    MockResponse::Error { status, delay_ms } => {
                                        if *delay_ms > 0 {
                                            sleep(Duration::from_millis(*delay_ms)).await;
                                        }
                                        let _ = tx.send(Err(status.clone())).await;
                                        return;
                                    }
                                    _ => {
                                        response_index += 1;
                                    }
                                }
                            }
                        }
                    }
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        return;
                    }
                }
            }
        });

        let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream)))
    }
}

/// Helper function to create a mock server and return its address
pub async fn start_mock_server() -> Result<(MockZerobusServer, String), Box<dyn std::error::Error>>
{
    info!("Starting mock Zerobus server");
    let mock_server = MockZerobusServer::new();
    let server_clone = MockZerobusServer {
        responses: Arc::clone(&mock_server.responses),
        stream_counter: Arc::clone(&mock_server.stream_counter),
        max_offset_sent: Arc::clone(&mock_server.max_offset_sent),
        write_count: Arc::clone(&mock_server.write_count),
        response_indices: Arc::clone(&mock_server.response_indices),
    };

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let server_url = format!("http://{}", local_addr);
    info!("Mock server will listen on: {}", server_url);

    tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(ZerobusServer::new(server_clone))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
        {
            error!("Mock server error: {}", e);
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    info!("Mock server started successfully at: {}", server_url);

    Ok((mock_server, server_url))
}
