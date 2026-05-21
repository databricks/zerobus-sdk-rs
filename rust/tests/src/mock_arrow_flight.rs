//! Mock Arrow Flight server for testing the Arrow Flight SDK functionality.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_ipc;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use futures::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};
use tokio::time::sleep;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};

/// Metadata sent with each FlightData batch from the client.
/// Must match the SDK's FlightBatchMetadata format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlightBatchMetadata {
    pub offset_id: i64,
}

/// Acknowledgement metadata sent back to the client.
/// Must match the SDK's FlightAckMetadata format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlightAckMetadata {
    pub ack_up_to_offset: i64,
    pub ack_up_to_records: u64,
}

/// Sentinel offset value indicating stream setup is complete but no batches have been acked yet.
pub const STREAM_READY_OFFSET: i64 = -1;

/// Mock response that can be injected into the mock Flight server.
///
/// Responses are processed in order. Each response is consumed once and the mock
/// advances to the next response in the sequence.
#[derive(Debug, Clone)]
pub enum MockFlightResponse {
    /// Successful batch acknowledgment.
    ///
    /// **Trigger semantics**: This ack is sent when a batch with `offset_id >= ack_up_to_offset`
    /// arrives. This allows cumulative acking - e.g., `BatchAck { ack_up_to_offset: 2, .. }`
    /// will trigger when batch 2 (or higher) arrives and acknowledge all batches up to offset 2.
    ///
    /// **Common patterns**:
    /// - Ack each batch individually: `(0..n).map(|i| BatchAck { ack_up_to_offset: i, delay_ms: 0, ack_up_to_records: 100 })`
    /// - Ack in batches: `[BatchAck { ack_up_to_offset: 4, .. }]` acks batches 0-4 when batch 4 arrives
    BatchAck {
        ack_up_to_offset: i64,
        delay_ms: u64,
        /// Cumulative records acknowledged.
        ack_up_to_records: u64,
    },
    /// Error response - sent immediately when a batch arrives.
    Error { status: Status, delay_ms: u64 },
    /// Close stream (drop the connection) - useful for testing recovery.
    CloseStream { delay_ms: u64 },
    /// Graceful close signal - sends a close signal with grace period duration.
    /// Optionally carries ack data in the same message (simulating the server
    /// acking in-flight batches alongside the close signal).
    GracefulClose {
        /// Grace period duration in milliseconds sent to the client.
        duration_ms: u64,
        delay_ms: u64,
        /// Optional ack data to include with the close signal.
        /// When set, the close signal PutResult also carries ack information,
        /// allowing the client to mark batches as acked during the grace period.
        ack_up_to_offset: Option<i64>,
        ack_up_to_records: Option<u64>,
    },
}

/// Mock Arrow Flight server for testing
pub struct MockFlightServer {
    /// Responses to inject for each table
    responses: Arc<Mutex<HashMap<String, Vec<MockFlightResponse>>>>,
    /// Track the maximum offset received from clients
    max_offset_received: Arc<Mutex<i64>>,
    /// Track number of batches received
    batch_count: Arc<Mutex<u64>>,
    /// Track total rows received
    row_count: Arc<Mutex<u64>>,
    /// Track response index across connection attempts
    response_indices: Arc<Mutex<HashMap<String, usize>>>,
    /// Track expected offset per table (must be strictly sequential starting from 0)
    expected_offsets: Arc<Mutex<HashMap<String, i64>>>,
}

impl MockFlightServer {
    pub fn new() -> Self {
        Self {
            responses: Arc::new(Mutex::new(HashMap::new())),
            max_offset_received: Arc::new(Mutex::new(-1)),
            batch_count: Arc::new(Mutex::new(0)),
            row_count: Arc::new(Mutex::new(0)),
            response_indices: Arc::new(Mutex::new(HashMap::new())),
            expected_offsets: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Inject responses for a specific table
    pub async fn inject_responses(&self, table_name: &str, responses: Vec<MockFlightResponse>) {
        let mut response_map = self.responses.lock().await;
        response_map.insert(table_name.to_string(), responses);

        let mut indices = self.response_indices.lock().await;
        indices.insert(table_name.to_string(), 0);
    }

    /// Get the maximum offset received from clients
    pub async fn get_max_offset_received(&self) -> i64 {
        *self.max_offset_received.lock().await
    }

    /// Get the number of batches received
    pub async fn get_batch_count(&self) -> u64 {
        *self.batch_count.lock().await
    }

    /// Get the total number of rows received across all batches
    pub async fn get_total_records_received(&self) -> u64 {
        *self.row_count.lock().await
    }

    /// Reset the server state
    #[allow(dead_code)]
    pub async fn reset(&self) {
        let mut responses = self.responses.lock().await;
        responses.clear();
        let mut indices = self.response_indices.lock().await;
        indices.clear();
        *self.max_offset_received.lock().await = -1;
        *self.batch_count.lock().await = 0;
        *self.row_count.lock().await = 0;
        let mut expected_offsets = self.expected_offsets.lock().await;
        expected_offsets.clear();
    }
}

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for MockFlightServer {
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type ListActionsStream = BoxedFlightStream<ActionType>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Handshake not implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("ListFlights not implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("GetFlightInfo not implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("PollFlightInfo not implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("GetSchema not implemented"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("DoGet not implemented"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        // Extract table name from headers
        let table_name = request
            .metadata()
            .get("x-databricks-zerobus-table-name")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown")
            .to_string();

        info!("Received DoPut request for table: {}", table_name);

        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(100);

        let responses = Arc::clone(&self.responses);
        let max_offset_received = Arc::clone(&self.max_offset_received);
        let batch_count = Arc::clone(&self.batch_count);
        let row_count = Arc::clone(&self.row_count);
        let response_indices = Arc::clone(&self.response_indices);
        let expected_offsets = Arc::clone(&self.expected_offsets);

        tokio::spawn(async move {
            let mut stream_responses: Vec<MockFlightResponse> = Vec::new();
            let mut is_first_message = true;

            // Load configured responses
            {
                let response_map = responses.lock().await;
                if let Some(responses) = response_map.get(&table_name) {
                    stream_responses = responses.clone();
                } else {
                    warn!("No configured responses found for table: {}", table_name);
                }
            }

            let mut response_index = {
                let indices = response_indices.lock().await;
                *indices.get(&table_name).unwrap_or(&0)
            };

            // Reset expected offset to 0 for each new connection
            {
                let mut offsets = expected_offsets.lock().await;
                offsets.insert(table_name.clone(), 0);
            }

            while let Ok(Some(flight_data)) = stream.message().await {
                // Handle schema message (first message has no app_metadata or empty app_metadata)
                if is_first_message {
                    is_first_message = false;
                    if flight_data.app_metadata.is_empty() {
                        debug!("Received schema message, sending ready signal");
                        // Send ready signal to confirm setup succeeded.
                        // This mirrors real server behavior where the server sends this after
                        // successful auth, schema validation, and stream setup.
                        let ready_metadata = FlightAckMetadata {
                            ack_up_to_offset: STREAM_READY_OFFSET,
                            ack_up_to_records: 0,
                        };
                        let ready_bytes = serde_json::to_vec(&ready_metadata).unwrap();
                        let ready_result = PutResult {
                            app_metadata: ready_bytes.into(),
                        };
                        if tx.send(Ok(ready_result)).await.is_err() {
                            warn!("Failed to send ready signal - client disconnected");
                            return;
                        }
                        continue;
                    }
                }

                // Parse batch metadata
                let metadata: Option<FlightBatchMetadata> =
                    serde_json::from_slice(&flight_data.app_metadata).ok();

                if let Some(metadata) = &metadata {
                    debug!("Received batch with offset_id: {}", metadata.offset_id);

                    // Validate offset is strictly sequential
                    let expected = {
                        let offsets = expected_offsets.lock().await;
                        *offsets.get(&table_name).unwrap_or(&0)
                    };
                    if metadata.offset_id != expected {
                        error!(
                            "Non-incremental offset: expected {}, got {}",
                            expected, metadata.offset_id
                        );
                        let _ = tx
                            .send(Err(Status::invalid_argument(format!(
                                "Non-incremental offset: expected {}, actual {}",
                                expected, metadata.offset_id
                            ))))
                            .await;
                        return;
                    }

                    // Update expected offset for next batch
                    {
                        let mut offsets = expected_offsets.lock().await;
                        offsets.insert(table_name.clone(), metadata.offset_id + 1);
                    }

                    // Update max offset
                    {
                        let mut max_offset = max_offset_received.lock().await;
                        if metadata.offset_id > *max_offset {
                            *max_offset = metadata.offset_id;
                        }
                    }

                    // Increment batch count
                    {
                        let mut count = batch_count.lock().await;
                        *count += 1;
                    }

                    // Decode the IPC data_header flatbuffer to get the actual row count.
                    let rows = arrow_ipc::root_as_message(&flight_data.data_header)
                        .ok()
                        .and_then(|msg| msg.header_as_record_batch())
                        .map(|rb| rb.length() as u64)
                        .unwrap_or(0);
                    if rows > 0 {
                        let mut count = row_count.lock().await;
                        *count += rows;
                    }
                }

                // Process mock response
                if response_index < stream_responses.len() {
                    let mock_response = &stream_responses[response_index];
                    match mock_response {
                        MockFlightResponse::BatchAck {
                            ack_up_to_offset,
                            delay_ms,
                            ack_up_to_records,
                        } => {
                            if metadata
                                .as_ref()
                                .map(|m| m.offset_id >= *ack_up_to_offset)
                                .unwrap_or(false)
                            {
                                if *delay_ms > 0 {
                                    sleep(Duration::from_millis(*delay_ms)).await;
                                }

                                let ack_metadata = FlightAckMetadata {
                                    ack_up_to_offset: *ack_up_to_offset,
                                    ack_up_to_records: *ack_up_to_records,
                                };
                                let ack_bytes = serde_json::to_vec(&ack_metadata).unwrap();

                                info!(
                                    "Sending BatchAck for offset: {}, records: {}",
                                    ack_up_to_offset, ack_up_to_records
                                );
                                let put_result = PutResult {
                                    app_metadata: ack_bytes.into(),
                                };

                                if tx.send(Ok(put_result)).await.is_err() {
                                    error!("Failed to send ack - channel closed");
                                    return;
                                }
                                response_index += 1;

                                // Update response index
                                {
                                    let mut indices = response_indices.lock().await;
                                    indices.insert(table_name.clone(), response_index);
                                }
                            }
                        }
                        MockFlightResponse::Error { status, delay_ms } => {
                            // Error responses trigger immediately on first batch
                            if *delay_ms > 0 {
                                sleep(Duration::from_millis(*delay_ms)).await;
                            }
                            info!("Sending error response: {:?}", status);
                            response_index += 1;
                            // Save response index before returning so next connection continues from here
                            {
                                let mut indices = response_indices.lock().await;
                                indices.insert(table_name.clone(), response_index);
                            }
                            let _ = tx.send(Err(status.clone())).await;
                            return;
                        }
                        MockFlightResponse::CloseStream { delay_ms } => {
                            // CloseStream triggers immediately - simulates server closing without ack
                            if *delay_ms > 0 {
                                sleep(Duration::from_millis(*delay_ms)).await;
                            }
                            info!("Closing stream as configured");
                            response_index += 1;
                            // Save response index before returning so next connection continues from here
                            {
                                let mut indices = response_indices.lock().await;
                                indices.insert(table_name.clone(), response_index);
                            }
                            return;
                        }
                        MockFlightResponse::GracefulClose {
                            duration_ms,
                            delay_ms,
                            ack_up_to_offset,
                            ack_up_to_records,
                        } => {
                            if *delay_ms > 0 {
                                sleep(Duration::from_millis(*delay_ms)).await;
                            }
                            info!(
                                "Sending graceful close signal with {}ms grace period",
                                duration_ms
                            );

                            // Send close signal metadata via PutResult.
                            // Optionally includes ack data if provided.
                            let close_metadata = serde_json::json!({
                                "ack_up_to_offset": ack_up_to_offset.unwrap_or(-1),
                                "ack_up_to_records": ack_up_to_records.unwrap_or(0),
                                "close_stream_duration_ms": duration_ms,
                            });
                            let close_bytes = serde_json::to_vec(&close_metadata).unwrap();
                            let put_result = PutResult {
                                app_metadata: close_bytes.into(),
                            };

                            if tx.send(Ok(put_result)).await.is_err() {
                                error!("Failed to send graceful close signal - channel closed");
                                return;
                            }
                            response_index += 1;
                            {
                                let mut indices = response_indices.lock().await;
                                indices.insert(table_name.clone(), response_index);
                            }
                            // Continue processing - the main loop waits for more batches.
                            // During grace period the client won't send new batches,
                            // so this effectively waits until the client disconnects.
                        }
                    }
                } else {
                    // Auto-ack if no more configured responses
                    if let Some(metadata) = metadata {
                        let records = {
                            let count = row_count.lock().await;
                            *count
                        };
                        let ack_metadata = FlightAckMetadata {
                            ack_up_to_offset: metadata.offset_id,
                            ack_up_to_records: records,
                        };
                        let ack_bytes = serde_json::to_vec(&ack_metadata).unwrap();

                        debug!(
                            "Auto-acking offset: {}, records: {}",
                            metadata.offset_id, records
                        );
                        let put_result = PutResult {
                            app_metadata: ack_bytes.into(),
                        };

                        if tx.send(Ok(put_result)).await.is_err() {
                            return;
                        }
                    }
                }
            }

            debug!("Client stream ended");
        });

        let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream)))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("DoExchange not implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("DoAction not implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("ListActions not implemented"))
    }
}

/// Helper function to create a mock Flight server and return its address
pub async fn start_mock_flight_server(
) -> Result<(MockFlightServer, String), Box<dyn std::error::Error>> {
    info!("Starting mock Arrow Flight server");
    let mock_server = MockFlightServer::new();
    let server_clone = MockFlightServer {
        responses: Arc::clone(&mock_server.responses),
        max_offset_received: Arc::clone(&mock_server.max_offset_received),
        batch_count: Arc::clone(&mock_server.batch_count),
        row_count: Arc::clone(&mock_server.row_count),
        response_indices: Arc::clone(&mock_server.response_indices),
        expected_offsets: Arc::clone(&mock_server.expected_offsets),
    };

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let server_url = format!("http://{}", local_addr);
    info!("Mock Flight server will listen on: {}", server_url);

    tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(server_clone))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
        {
            error!("Mock Flight server error: {}", e);
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    info!("Mock Flight server started successfully at: {}", server_url);

    Ok((mock_server, server_url))
}
