use std::collections::{HashMap, VecDeque};
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

/// A queued stream entry: either a real stream with responses, or an error during creation.
#[derive(Clone)]
enum StreamEntry {
    Stream {
        stream_id: String,
        create_delay_ms: u64,
        responses: Vec<MockResponse>,
    },
    CreateError {
        status: Status,
        delay_ms: u64,
    },
}

/// Mock gRPC server for testing the Rust SDK.
///
/// Responses are keyed by `(table_name, stream_id)` and tracked per-stream.
/// `inject_responses` splits the flat response list at `CreateStream` boundaries
/// so each stream gets its own independent responses.
pub struct MockZerobusServer {
    /// Per-stream responses keyed by (table_name, stream_id)
    responses: Arc<Mutex<HashMap<(String, String), Vec<MockResponse>>>>,
    /// Per-stream response indices keyed by (table_name, stream_id)
    response_indices: Arc<Mutex<HashMap<(String, String), usize>>>,
    /// Order of streams per table — each connection pops the next entry
    stream_order: Arc<Mutex<HashMap<String, VecDeque<StreamEntry>>>>,
    /// Counter for generating unique stream IDs
    stream_counter: Arc<Mutex<u32>>,
    /// Track the maximum offset sent by clients
    max_offset_sent: Arc<Mutex<i64>>,
    /// Track number of writes received
    write_count: Arc<Mutex<u64>>,
}

impl MockZerobusServer {
    pub fn new() -> Self {
        Self {
            responses: Arc::new(Mutex::new(HashMap::new())),
            response_indices: Arc::new(Mutex::new(HashMap::new())),
            stream_order: Arc::new(Mutex::new(HashMap::new())),
            stream_counter: Arc::new(Mutex::new(0)),
            max_offset_sent: Arc::new(Mutex::new(-1)),
            write_count: Arc::new(Mutex::new(0)),
        }
    }

    /// Inject responses for one or more streams on a table.
    ///
    /// The response list is split at `CreateStream` boundaries. Each
    /// `CreateStream { stream_id }` defines a stream whose responses are
    /// the items following it until the next `CreateStream` (or end).
    /// Errors before any `CreateStream` become connection-creation errors.
    ///
    /// Internally, responses are stored per `(table_name, stream_id)`.
    pub async fn inject_responses(&self, table_name: &str, responses: Vec<MockResponse>) {
        let mut response_map = self.responses.lock().await;
        let mut indices = self.response_indices.lock().await;
        let mut order = self.stream_order.lock().await;
        let mut counter = self.stream_counter.lock().await;

        let queue = order
            .entry(table_name.to_string())
            .or_insert_with(VecDeque::new);

        let mut current_stream: Option<(String, u64, Vec<MockResponse>)> = None;

        for response in responses {
            match response {
                MockResponse::CreateStream {
                    stream_id,
                    delay_ms,
                } => {
                    // Flush previous stream
                    if let Some((sid, delay, resps)) = current_stream.take() {
                        let key = (table_name.to_string(), sid.clone());
                        response_map.insert(key.clone(), resps);
                        indices.insert(key, 0);
                        queue.push_back(StreamEntry::Stream {
                            stream_id: sid,
                            create_delay_ms: delay,
                            responses: Vec::new(), // responses stored in map
                        });
                    }
                    current_stream = Some((stream_id, delay_ms, Vec::new()));
                }
                MockResponse::Error { ref status, delay_ms } if current_stream.is_none() => {
                    // Error before any CreateStream = connection-creation error
                    queue.push_back(StreamEntry::CreateError {
                        status: status.clone(),
                        delay_ms,
                    });
                }
                other => {
                    if let Some((_, _, ref mut resps)) = current_stream {
                        resps.push(other);
                    } else {
                        // Responses before any CreateStream with no error — create default stream
                        *counter += 1;
                        let auto_id = format!("auto_stream_{}", *counter);
                        current_stream = Some((auto_id, 0, vec![other]));
                    }
                }
            }
        }

        // Flush last stream
        if let Some((sid, delay, resps)) = current_stream {
            let key = (table_name.to_string(), sid.clone());
            response_map.insert(key.clone(), resps);
            indices.insert(key, 0);
            queue.push_back(StreamEntry::Stream {
                stream_id: sid,
                create_delay_ms: delay,
                responses: Vec::new(),
            });
        }
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
        self.responses.lock().await.clear();
        self.response_indices.lock().await.clear();
        self.stream_order.lock().await.clear();
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
        let response_indices = Arc::clone(&self.response_indices);
        let stream_order = Arc::clone(&self.stream_order);
        let max_offset_sent = Arc::clone(&self.max_offset_sent);
        let write_count = Arc::clone(&self.write_count);

        tokio::spawn(async move {
            let table_name;
            let stream_responses: Vec<MockResponse>;
            let stream_key: (String, String);

            // Wait for CreateStream request
            if let Some(request_result) = stream.message().await.transpose() {
                match request_result {
                    Ok(request) => {
                        if let Some(RequestPayload::CreateStream(create_request)) = request.payload
                        {
                            table_name = create_request.table_name.unwrap_or_default();
                            info!("Received CreateStream request for table: {}", table_name);

                            // Pop next stream entry for this table
                            let entry = {
                                let mut order = stream_order.lock().await;
                                order
                                    .get_mut(&table_name)
                                    .and_then(|q| q.pop_front())
                            };

                            match entry {
                                Some(StreamEntry::Stream {
                                    stream_id,
                                    create_delay_ms,
                                    ..
                                }) => {
                                    if create_delay_ms > 0 {
                                        sleep(Duration::from_millis(create_delay_ms)).await;
                                    }
                                    info!(
                                        "Sending CreateStream response with stream_id: {}",
                                        stream_id
                                    );
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
                                    stream_key = (table_name.clone(), stream_id);
                                    // Load this stream's responses
                                    let resp_map = responses.lock().await;
                                    stream_responses = resp_map
                                        .get(&stream_key)
                                        .cloned()
                                        .unwrap_or_default();
                                }
                                Some(StreamEntry::CreateError { status, delay_ms }) => {
                                    if delay_ms > 0 {
                                        sleep(Duration::from_millis(delay_ms)).await;
                                    }
                                    info!("Sending error during CreateStream: {:?}", status);
                                    let _ = tx.send(Err(status)).await;
                                    return;
                                }
                                None => {
                                    // No queued streams — auto-create
                                    warn!("No queued stream entries for table: {}", table_name);
                                    let response = EphemeralStreamResponse {
                                        payload: Some(ResponsePayload::CreateStreamResponse(
                                            CreateIngestStreamResponse {
                                                stream_id: Some("auto_stream".to_string()),
                                            },
                                        )),
                                    };
                                    if tx.send(Ok(response)).await.is_err() {
                                        return;
                                    }
                                    stream_key = (table_name.clone(), "auto_stream".to_string());
                                    stream_responses = Vec::new();
                                }
                            }
                        } else {
                            warn!("Expected CreateStream request");
                            return;
                        }
                    }
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        return;
                    }
                }
            } else {
                return;
            }

            // Per-stream response processing
            let mut response_index: usize = {
                let indices = response_indices.lock().await;
                *indices.get(&stream_key).unwrap_or(&0)
            };

            while let Some(request_result) = stream.message().await.transpose() {
                match request_result {
                    Ok(request) => {
                        match request.payload {
                            Some(RequestPayload::IngestRecord(ingest_request)) => {
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
                                    let (should_continue, new_index) = handle_mock_response(
                                        &stream_responses[response_index],
                                        ingest_request.offset_id,
                                        "single record",
                                        &tx,
                                        response_index,
                                    )
                                    .await;
                                    response_index = new_index;

                                    if !should_continue {
                                        return;
                                    }
                                }
                            }
                            Some(RequestPayload::IngestRecordBatch(batch_request)) => {
                                debug!(
                                    "Received IngestRecordBatch request with offset_id: {:?}",
                                    batch_request.offset_id
                                );

                                let record_count = if let Some(batch) = &batch_request.batch {
                                    use databricks::zerobus::ingest_record_batch_request::Batch;
                                    match batch {
                                        Batch::ProtoEncodedBatch(proto_batch) => {
                                            proto_batch.records.len()
                                        }
                                        Batch::JsonBatch(json_batch) => json_batch.records.len(),
                                    }
                                } else {
                                    0
                                };

                                debug!("Batch contains {} records", record_count);

                                if let Some(offset_id) = batch_request.offset_id {
                                    let mut max_offset = max_offset_sent.lock().await;
                                    if offset_id > *max_offset {
                                        *max_offset = offset_id;
                                    }
                                }

                                {
                                    let mut count = write_count.lock().await;
                                    *count += record_count as u64;
                                    debug!(
                                        "Incremented write count by {} to: {}",
                                        record_count, *count
                                    );
                                }

                                if response_index < stream_responses.len() {
                                    let (should_continue, new_index) = handle_mock_response(
                                        &stream_responses[response_index],
                                        batch_request.offset_id,
                                        "batch",
                                        &tx,
                                        response_index,
                                    )
                                    .await;
                                    response_index = new_index;

                                    if !should_continue {
                                        return;
                                    }
                                }
                            }
                            _ => {
                                debug!("Received unknown request type");
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
        response_indices: Arc::clone(&mock_server.response_indices),
        stream_order: Arc::clone(&mock_server.stream_order),
        stream_counter: Arc::clone(&mock_server.stream_counter),
        max_offset_sent: Arc::clone(&mock_server.max_offset_sent),
        write_count: Arc::clone(&mock_server.write_count),
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

/// Helper function to handle mock response processing
/// Returns (should_continue, new_response_index)
async fn handle_mock_response(
    mock_response: &MockResponse,
    offset: Option<i64>,
    request_type: &str,
    tx: &mpsc::Sender<Result<EphemeralStreamResponse, Status>>,
    current_index: usize,
) -> (bool, usize) {
    match mock_response {
        MockResponse::RecordAck {
            ack_up_to_offset,
            delay_ms,
        } => {
            if offset == Some(*ack_up_to_offset) {
                if *delay_ms > 0 {
                    sleep(Duration::from_millis(*delay_ms)).await;
                }
                info!(
                    "Sending RecordAck response for {} with ack_up_to_offset: {}",
                    request_type, ack_up_to_offset
                );
                let response = EphemeralStreamResponse {
                    payload: Some(ResponsePayload::IngestRecordResponse(
                        IngestRecordResponse {
                            durability_ack_up_to_offset: Some(*ack_up_to_offset),
                        },
                    )),
                };
                if tx.send(Ok(response)).await.is_err() {
                    return (false, current_index);
                }
                (true, current_index + 1)
            } else {
                (true, current_index)
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
                payload: Some(ResponsePayload::CloseStreamSignal(CloseStreamSignal {
                    duration: Some(ProtobufDuration {
                        seconds: *duration_seconds,
                        nanos: 0,
                    }),
                })),
            };
            if tx.send(Ok(response)).await.is_err() {
                return (false, current_index);
            }
            (true, current_index + 1)
        }
        MockResponse::Error { status, delay_ms } => {
            if *delay_ms > 0 {
                sleep(Duration::from_millis(*delay_ms)).await;
            }
            let _ = tx.send(Err(status.clone())).await;
            (false, current_index + 1)
        }
        MockResponse::CreateStream { .. } => (true, current_index + 1),
    }
}
