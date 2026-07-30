//! Mock Arrow Flight server for testing the Arrow Flight SDK functionality.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::Int64Array;
use arrow_flight::decode::{DecodedFlightData, DecodedPayload, FlightRecordBatchStream};
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use futures::{Stream, StreamExt, TryStreamExt};
use rcgen::{generate_simple_self_signed, CertifiedKey};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};
use tokio::time::sleep;
use tonic::transport::{Identity, ServerTlsConfig};
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
    /// Emits an ordinary ACK followed immediately by a distinct graceful-close signal
    /// for the same received batch. This models both responses already being queued while
    /// the client is between response polls.
    BatchAckThenGracefulClose {
        ack_up_to_offset: i64,
        ack_up_to_records: u64,
        duration_ms: u64,
        hold_response_open: bool,
    },
    /// Acknowledge exactly the records decoded from the current physical chunk and
    /// request graceful close. This is used to verify suffix-only recovery when a
    /// logical RecordBatch is cancelled between Flight chunks.
    GracefulCloseAfterChunk {
        duration_ms: u64,
        hold_response_open: bool,
    },
    /// Successful batch acknowledgment sent only after the client cleanly half-closes
    /// its request stream.
    ///
    /// This explicitly models a genuine acknowledgment that was still in flight when
    /// request `END_STREAM` arrived. Its watermark should advance beyond the preceding
    /// acknowledgment or close signal.
    BatchAckAfterRequestEof {
        ack_up_to_offset: i64,
        /// Cumulative records acknowledged.
        ack_up_to_records: u64,
    },
    /// Acknowledge a batch normally, then keep the response open after request EOF.
    BatchAckAndHoldAfterRequestEof {
        ack_up_to_offset: i64,
        ack_up_to_records: u64,
    },
    /// Terminal peer status sent only after the client cleanly half-closes its request stream.
    ErrorAfterRequestEof { status: Status },
    /// Error response - sent immediately when a batch arrives.
    Error { status: Status, delay_ms: u64 },
    /// Reject a connection's setup: send this error instead of the ready signal on the
    /// first (schema) message, simulating a failed reconnect. Consumes its scripted slot,
    /// so schedule it as the response the target (reconnect) connection reaches.
    FailSetup { status: Status },
    /// Reject a connection's setup after a delay.
    FailSetupAfter { status: Status, delay_ms: u64 },
    /// Delay a connection's ready signal after response headers are already available.
    DelaySetup { delay_ms: u64 },
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
        /// Test-only behavior for exercising the client's bounded drain fallback.
        /// When true, keep the response open after a clean request half-close.
        hold_response_open: bool,
    },
    /// Graceful close whose post-request-EOF response is a terminal peer status.
    GracefulCloseWithFinalError {
        duration_ms: u64,
        delay_ms: u64,
        status: Status,
    },
    /// Graceful close followed by a peer error before request EOF. The handler keeps
    /// polling the request so tests can distinguish a clean client half-close from a reset.
    GracefulCloseWithDelayedError {
        duration_ms: u64,
        error_delay_ms: u64,
        status: Status,
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
    /// Decoded first-column IDs received across all connections.
    received_ids: Arc<Mutex<Vec<i64>>>,
    /// Decoded first-column IDs grouped by DoPut connection, in connection order.
    received_ids_by_connection: Arc<Mutex<Vec<Vec<i64>>>>,
    /// Assigns a stable index to each DoPut connection.
    connection_count: Arc<AtomicU64>,
    /// Track response index across connection attempts
    response_indices: Arc<Mutex<HashMap<String, usize>>>,
    /// Observation of every `ack_up_to_records` value emitted on the auto-ack path,
    /// in emission order. Used by tests to assert acks are connection-relative.
    auto_ack_records: Arc<Mutex<Vec<u64>>>,
    /// Number of DoPut request streams that ended with a clean HTTP/2 half-close.
    request_half_closes: Arc<AtomicU64>,
    /// Number of DoPut request streams that ended with a transport error/reset.
    request_resets: Arc<AtomicU64>,
    /// Number of explicitly scripted acknowledgments delivered after observing a clean
    /// request half-close.
    final_response_deliveries: Arc<AtomicU64>,
    /// Test-only barrier that delays the next `DoPut` response headers after the request
    /// handler has started consuming its body.
    do_put_response_gate: Arc<Mutex<Option<DoPutResponseBarrier>>>,
}

#[derive(Clone)]
struct DoPutResponseBarrier {
    reached: Arc<tokio::sync::Notify>,
    proceed: Arc<tokio::sync::Notify>,
}

impl MockFlightServer {
    pub fn new() -> Self {
        Self {
            responses: Arc::new(Mutex::new(HashMap::new())),
            max_offset_received: Arc::new(Mutex::new(-1)),
            batch_count: Arc::new(Mutex::new(0)),
            row_count: Arc::new(Mutex::new(0)),
            received_ids: Arc::new(Mutex::new(Vec::new())),
            received_ids_by_connection: Arc::new(Mutex::new(Vec::new())),
            connection_count: Arc::new(AtomicU64::new(0)),
            response_indices: Arc::new(Mutex::new(HashMap::new())),
            auto_ack_records: Arc::new(Mutex::new(Vec::new())),
            request_half_closes: Arc::new(AtomicU64::new(0)),
            request_resets: Arc::new(AtomicU64::new(0)),
            final_response_deliveries: Arc::new(AtomicU64::new(0)),
            do_put_response_gate: Arc::new(Mutex::new(None)),
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

    /// Get decoded first-column IDs received across all connections.
    pub async fn get_received_ids(&self) -> Vec<i64> {
        self.received_ids.lock().await.clone()
    }

    /// Get decoded first-column IDs grouped by DoPut connection.
    pub async fn get_received_ids_by_connection(&self) -> Vec<Vec<i64>> {
        self.received_ids_by_connection.lock().await.clone()
    }

    /// Get every `ack_up_to_records` value emitted on the auto-ack path, in order.
    #[allow(dead_code)]
    pub async fn get_auto_ack_records(&self) -> Vec<u64> {
        self.auto_ack_records.lock().await.clone()
    }

    /// Get the number of client request streams that reached a clean END_STREAM.
    pub fn get_request_half_close_count(&self) -> u64 {
        self.request_half_closes.load(Ordering::Relaxed)
    }

    /// Get the number of client request streams that ended with a reset/transport error.
    pub fn get_request_reset_count(&self) -> u64 {
        self.request_resets.load(Ordering::Relaxed)
    }

    /// Get the number of explicitly scripted acknowledgments delivered after request
    /// `END_STREAM`.
    pub fn get_final_response_delivery_count(&self) -> u64 {
        self.final_response_deliveries.load(Ordering::Relaxed)
    }

    /// Delays response headers for the next `DoPut` until `proceed` is notified.
    pub async fn arm_do_put_response_barrier(
        &self,
    ) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        let reached = Arc::new(tokio::sync::Notify::new());
        let proceed = Arc::new(tokio::sync::Notify::new());
        *self.do_put_response_gate.lock().await = Some(DoPutResponseBarrier {
            reached: Arc::clone(&reached),
            proceed: Arc::clone(&proceed),
        });
        (reached, proceed)
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
        self.received_ids.lock().await.clear();
        self.received_ids_by_connection.lock().await.clear();
        self.connection_count.store(0, Ordering::Relaxed);
        self.auto_ack_records.lock().await.clear();
        self.request_half_closes.store(0, Ordering::Relaxed);
        self.request_resets.store(0, Ordering::Relaxed);
        self.final_response_deliveries.store(0, Ordering::Relaxed);
        *self.do_put_response_gate.lock().await = None;
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

        let stream = request.into_inner();
        let mut stream = FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(arrow_flight::error::FlightError::from),
        )
        .into_inner();
        let (tx, rx) = mpsc::channel(100);
        let response_barrier = self.do_put_response_gate.lock().await.take();
        let connection_index = self.connection_count.fetch_add(1, Ordering::Relaxed) as usize;
        {
            let mut ids_by_connection = self.received_ids_by_connection.lock().await;
            ids_by_connection.resize_with(connection_index + 1, Vec::new);
        }

        let responses = Arc::clone(&self.responses);
        let max_offset_received = Arc::clone(&self.max_offset_received);
        let batch_count = Arc::clone(&self.batch_count);
        let row_count = Arc::clone(&self.row_count);
        let received_ids = Arc::clone(&self.received_ids);
        let received_ids_by_connection = Arc::clone(&self.received_ids_by_connection);
        let response_indices = Arc::clone(&self.response_indices);
        let auto_ack_records = Arc::clone(&self.auto_ack_records);
        let request_half_closes = Arc::clone(&self.request_half_closes);
        let request_resets = Arc::clone(&self.request_resets);
        let final_response_deliveries = Arc::clone(&self.final_response_deliveries);

        tokio::spawn(async move {
            let mut stream_responses: Vec<MockFlightResponse> = Vec::new();
            let mut is_first_message = true;
            // Per-connection state, owned as locals like the real server (fresh per
            // DoPut connection). These must not leak across reconnects or concurrent
            // same-table streams: `expected_offset` validates wire ordering, and
            // `connection_record_count` is the cumulative-record tracker that drives
            // auto-acks (the server derives ack_up_to_records per connection).
            let mut expected_offset: i64 = 0;
            let mut connection_record_count: u64 = 0;
            let mut hold_response_open_after_half_close = false;
            let mut final_response_error: Option<Status> = None;

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

            loop {
                let DecodedFlightData {
                    inner: flight_data,
                    payload,
                } = match stream.next().await {
                    Some(Ok(decoded)) => decoded,
                    None => {
                        request_half_closes.fetch_add(1, Ordering::Relaxed);
                        if let Some(status) = final_response_error.take() {
                            let _ = tx.send(Err(status)).await;
                            break;
                        }

                        if let Some(MockFlightResponse::BatchAckAfterRequestEof {
                            ack_up_to_offset,
                            ack_up_to_records,
                        }) = stream_responses.get(response_index)
                        {
                            let metadata = FlightAckMetadata {
                                ack_up_to_offset: *ack_up_to_offset,
                                ack_up_to_records: *ack_up_to_records,
                            };
                            let result = PutResult {
                                app_metadata: serde_json::to_vec(&metadata).unwrap().into(),
                            };
                            let delivered = tx.send(Ok(result)).await.is_ok();
                            response_index += 1;
                            response_indices
                                .lock()
                                .await
                                .insert(table_name.clone(), response_index);
                            if delivered {
                                final_response_deliveries.fetch_add(1, Ordering::Relaxed);
                            }
                        } else if let Some(MockFlightResponse::ErrorAfterRequestEof { status }) =
                            stream_responses.get(response_index)
                        {
                            let _ = tx.send(Err(status.clone())).await;
                            response_index += 1;
                            response_indices
                                .lock()
                                .await
                                .insert(table_name.clone(), response_index);
                        }
                        break;
                    }
                    Some(Err(error)) => {
                        request_resets.fetch_add(1, Ordering::Relaxed);
                        warn!("Client request stream reset or decode failed: {error}");
                        break;
                    }
                };
                // Handle schema message (first message has no app_metadata or empty app_metadata)
                if is_first_message {
                    is_first_message = false;
                    if flight_data.app_metadata.is_empty() {
                        let delayed_setup =
                            if let Some(MockFlightResponse::DelaySetup { delay_ms }) =
                                stream_responses.get(response_index)
                            {
                                let delay_ms = *delay_ms;
                                response_index += 1;
                                response_indices
                                    .lock()
                                    .await
                                    .insert(table_name.clone(), response_index);
                                sleep(Duration::from_millis(delay_ms)).await;
                                true
                            } else {
                                false
                            };
                        // A scripted FailSetup rejects this connection's setup: send the
                        // error instead of the ready signal (simulates a reconnect failure).
                        let setup_failure = if delayed_setup {
                            None
                        } else {
                            match stream_responses.get(response_index) {
                                Some(MockFlightResponse::FailSetup { status }) => {
                                    Some((status.clone(), 0))
                                }
                                Some(MockFlightResponse::FailSetupAfter { status, delay_ms }) => {
                                    Some((status.clone(), *delay_ms))
                                }
                                _ => None,
                            }
                        };
                        if let Some((status, delay_ms)) = setup_failure {
                            response_index += 1;
                            {
                                let mut indices = response_indices.lock().await;
                                indices.insert(table_name.clone(), response_index);
                            }
                            if delay_ms > 0 {
                                sleep(Duration::from_millis(delay_ms)).await;
                            }
                            info!("Rejecting connection setup: {:?}", status);
                            let _ = tx.send(Err(status)).await;
                            return;
                        }

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

                    // Validate offset is strictly sequential (connection-local).
                    if metadata.offset_id != expected_offset {
                        error!(
                            "Non-incremental offset: expected {}, got {}",
                            expected_offset, metadata.offset_id
                        );
                        let _ = tx
                            .send(Err(Status::invalid_argument(format!(
                                "Non-incremental offset: expected {}, actual {}",
                                expected_offset, metadata.offset_id
                            ))))
                            .await;
                        return;
                    }

                    // Update expected offset for next batch.
                    expected_offset = metadata.offset_id + 1;

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

                    // Decode the full Arrow payload so tests can verify row identity,
                    // not only the flatbuffer's declared row count.
                    let rows = match &payload {
                        DecodedPayload::RecordBatch(batch) => {
                            if let Some(id_array) =
                                batch.column(0).as_any().downcast_ref::<Int64Array>()
                            {
                                let ids = id_array.values().to_vec();
                                received_ids.lock().await.extend_from_slice(&ids);
                                received_ids_by_connection.lock().await[connection_index]
                                    .extend_from_slice(&ids);
                            }
                            batch.num_rows() as u64
                        }
                        DecodedPayload::Schema(_) | DecodedPayload::None => 0,
                    };
                    if rows > 0 {
                        // Connection-local counter drives acks (mirrors the server's
                        // per-connection cumulative tracker); the global row_count is
                        // kept only as a cross-connection observation metric.
                        connection_record_count += rows;
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
                        MockFlightResponse::BatchAckThenGracefulClose {
                            ack_up_to_offset,
                            ack_up_to_records,
                            duration_ms,
                            hold_response_open,
                        } => {
                            if metadata
                                .as_ref()
                                .map(|m| m.offset_id >= *ack_up_to_offset)
                                .unwrap_or(false)
                            {
                                let ack = FlightAckMetadata {
                                    ack_up_to_offset: *ack_up_to_offset,
                                    ack_up_to_records: *ack_up_to_records,
                                };
                                if tx
                                    .send(Ok(PutResult {
                                        app_metadata: serde_json::to_vec(&ack).unwrap().into(),
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }

                                let close_metadata = serde_json::json!({
                                    "ack_up_to_offset": -1,
                                    "ack_up_to_records": 0,
                                    "close_stream_duration_ms": duration_ms,
                                });
                                if tx
                                    .send(Ok(PutResult {
                                        app_metadata: serde_json::to_vec(&close_metadata)
                                            .unwrap()
                                            .into(),
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }

                                response_index += 1;
                                hold_response_open_after_half_close = *hold_response_open;
                                response_indices
                                    .lock()
                                    .await
                                    .insert(table_name.clone(), response_index);
                            }
                        }
                        MockFlightResponse::GracefulCloseAfterChunk {
                            duration_ms,
                            hold_response_open,
                        } => {
                            if let Some(metadata) = &metadata {
                                let close_metadata = serde_json::json!({
                                    "ack_up_to_offset": metadata.offset_id,
                                    "ack_up_to_records": connection_record_count,
                                    "close_stream_duration_ms": duration_ms,
                                });
                                if tx
                                    .send(Ok(PutResult {
                                        app_metadata: serde_json::to_vec(&close_metadata)
                                            .unwrap()
                                            .into(),
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                response_index += 1;
                                hold_response_open_after_half_close = *hold_response_open;
                                response_indices
                                    .lock()
                                    .await
                                    .insert(table_name.clone(), response_index);

                                // Let the client observe the close response before the mock
                                // polls another request item. This makes cancellation at a
                                // physical chunk boundary deterministic under local HTTP/2.
                                sleep(Duration::from_millis(100)).await;
                            }
                        }
                        MockFlightResponse::BatchAckAfterRequestEof { .. } => {
                            debug!("Waiting for request EOF before sending scripted ack");
                        }
                        MockFlightResponse::BatchAckAndHoldAfterRequestEof {
                            ack_up_to_offset,
                            ack_up_to_records,
                        } => {
                            if metadata
                                .as_ref()
                                .map(|m| m.offset_id >= *ack_up_to_offset)
                                .unwrap_or(false)
                            {
                                let ack = FlightAckMetadata {
                                    ack_up_to_offset: *ack_up_to_offset,
                                    ack_up_to_records: *ack_up_to_records,
                                };
                                if tx
                                    .send(Ok(PutResult {
                                        app_metadata: serde_json::to_vec(&ack).unwrap().into(),
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                response_index += 1;
                                hold_response_open_after_half_close = true;
                                response_indices
                                    .lock()
                                    .await
                                    .insert(table_name.clone(), response_index);
                            }
                        }
                        MockFlightResponse::ErrorAfterRequestEof { .. } => {
                            debug!("Waiting for request EOF before sending scripted error");
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
                        MockFlightResponse::FailSetup { status } => {
                            // Only meaningful at connection setup; if reached here, the
                            // scripting is off — fail the connection with the error.
                            let status = status.clone();
                            response_index += 1;
                            {
                                let mut indices = response_indices.lock().await;
                                indices.insert(table_name.clone(), response_index);
                            }
                            let _ = tx.send(Err(status)).await;
                            return;
                        }
                        MockFlightResponse::FailSetupAfter { status, delay_ms } => {
                            if *delay_ms > 0 {
                                sleep(Duration::from_millis(*delay_ms)).await;
                            }
                            let status = status.clone();
                            response_index += 1;
                            {
                                let mut indices = response_indices.lock().await;
                                indices.insert(table_name.clone(), response_index);
                            }
                            let _ = tx.send(Err(status)).await;
                            return;
                        }
                        MockFlightResponse::DelaySetup { .. } => {
                            warn!("DelaySetup response reached the batch-processing phase");
                            response_index += 1;
                            response_indices
                                .lock()
                                .await
                                .insert(table_name.clone(), response_index);
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
                            hold_response_open,
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
                            hold_response_open_after_half_close = *hold_response_open;
                            {
                                let mut indices = response_indices.lock().await;
                                indices.insert(table_name.clone(), response_index);
                            }
                            // Continue processing - the main loop waits for more batches.
                            // During grace period the client won't send new batches,
                            // so this effectively waits until the client disconnects.
                        }
                        MockFlightResponse::GracefulCloseWithFinalError {
                            duration_ms,
                            delay_ms,
                            status,
                        } => {
                            if *delay_ms > 0 {
                                sleep(Duration::from_millis(*delay_ms)).await;
                            }
                            let close_metadata = serde_json::json!({
                                "ack_up_to_offset": -1,
                                "ack_up_to_records": 0,
                                "close_stream_duration_ms": duration_ms,
                            });
                            let put_result = PutResult {
                                app_metadata: serde_json::to_vec(&close_metadata).unwrap().into(),
                            };
                            if tx.send(Ok(put_result)).await.is_err() {
                                return;
                            }
                            final_response_error = Some(status.clone());
                            response_index += 1;
                            let mut indices = response_indices.lock().await;
                            indices.insert(table_name.clone(), response_index);
                        }
                        MockFlightResponse::GracefulCloseWithDelayedError {
                            duration_ms,
                            error_delay_ms,
                            status,
                        } => {
                            let close_metadata = serde_json::json!({
                                "ack_up_to_offset": -1,
                                "ack_up_to_records": 0,
                                "close_stream_duration_ms": duration_ms,
                            });
                            let put_result = PutResult {
                                app_metadata: serde_json::to_vec(&close_metadata).unwrap().into(),
                            };
                            if tx.send(Ok(put_result)).await.is_err() {
                                return;
                            }
                            response_index += 1;
                            {
                                let mut indices = response_indices.lock().await;
                                indices.insert(table_name.clone(), response_index);
                            }
                            if *error_delay_ms > 0 {
                                sleep(Duration::from_millis(*error_delay_ms)).await;
                            }
                            if tx.send(Err(status.clone())).await.is_err() {
                                return;
                            }
                            // Continue polling the request. The response has failed, so the
                            // client must still explicitly drive its controlled body to EOF.
                        }
                    }
                } else {
                    // Auto-ack if no more configured responses
                    if let Some(metadata) = metadata {
                        // Use the connection-local record count so acks are
                        // connection-relative, matching the real server.
                        let records = connection_record_count;
                        auto_ack_records.lock().await.push(records);
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

            debug!("Client request stream ended");
            if hold_response_open_after_half_close {
                std::future::pending::<()>().await;
            }
        });

        if let Some(barrier) = response_barrier {
            barrier.reached.notify_one();
            barrier.proceed.notified().await;
        }

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
    start_mock_flight_server_inner(None, "http", "127.0.0.1").await
}

/// Starts the mock Flight server with a runtime-generated TLS identity.
#[allow(dead_code)]
pub async fn start_mock_tls_flight_server(
) -> Result<(MockFlightServer, String, Vec<u8>), Box<dyn std::error::Error>> {
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec!["localhost".to_string()])?;
    let cert_pem = cert.pem();
    let identity = Identity::from_pem(cert_pem.as_bytes(), key_pair.serialize_pem().as_bytes());
    let tls = ServerTlsConfig::new().identity(identity);
    let (server, server_url) =
        start_mock_flight_server_inner(Some(tls), "https", "localhost").await?;
    Ok((server, server_url, cert_pem.into_bytes()))
}

async fn start_mock_flight_server_inner(
    tls: Option<ServerTlsConfig>,
    scheme: &str,
    endpoint_host: &str,
) -> Result<(MockFlightServer, String), Box<dyn std::error::Error>> {
    info!("Starting mock Arrow Flight server");
    let mock_server = MockFlightServer::new();
    let server_clone = MockFlightServer {
        responses: Arc::clone(&mock_server.responses),
        max_offset_received: Arc::clone(&mock_server.max_offset_received),
        batch_count: Arc::clone(&mock_server.batch_count),
        row_count: Arc::clone(&mock_server.row_count),
        received_ids: Arc::clone(&mock_server.received_ids),
        received_ids_by_connection: Arc::clone(&mock_server.received_ids_by_connection),
        connection_count: Arc::clone(&mock_server.connection_count),
        response_indices: Arc::clone(&mock_server.response_indices),
        auto_ack_records: Arc::clone(&mock_server.auto_ack_records),
        request_half_closes: Arc::clone(&mock_server.request_half_closes),
        request_resets: Arc::clone(&mock_server.request_resets),
        final_response_deliveries: Arc::clone(&mock_server.final_response_deliveries),
        do_put_response_gate: Arc::clone(&mock_server.do_put_response_gate),
    };

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let server_url = format!("{}://{}:{}", scheme, endpoint_host, local_addr.port());
    info!("Mock Flight server will listen on: {}", server_url);

    let mut server = tonic::transport::Server::builder();
    if let Some(tls) = tls {
        server = server.tls_config(tls)?;
    }
    let router = server.add_service(FlightServiceServer::new(server_clone));
    tokio::spawn(async move {
        if let Err(e) = router
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
