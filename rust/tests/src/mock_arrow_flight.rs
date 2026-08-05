//! Mock Arrow Flight server for testing the Arrow Flight SDK functionality.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, StringArray};
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use arrow_schema::Schema;
use futures::Stream;
use rcgen::{generate_simple_self_signed, CertifiedKey};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::time::sleep;
use tonic::transport::{Identity, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};

/// Observations extracted from an Arrow IPC message header on the wire.
struct IpcWireObservation {
    dictionary_messages: u64,
    record_batch_messages: u64,
    zstd_record_batches: u64,
    rows: u64,
    schema_has_dictionary_encoding: bool,
}

fn observe_ipc_data_header(data_header: &[u8]) -> IpcWireObservation {
    let mut observation = IpcWireObservation {
        dictionary_messages: 0,
        record_batch_messages: 0,
        zstd_record_batches: 0,
        rows: 0,
        schema_has_dictionary_encoding: false,
    };

    let Some(msg) = arrow_ipc::root_as_message(data_header).ok() else {
        return observation;
    };

    if let Some(schema) = msg.header_as_schema() {
        if let Some(fields) = schema.fields() {
            for idx in 0..fields.len() {
                let field = fields.get(idx);
                if field.dictionary().is_some() {
                    observation.schema_has_dictionary_encoding = true;
                    break;
                }
            }
        }
    }

    if msg.header_as_dictionary_batch().is_some() {
        observation.dictionary_messages = 1;
    }

    if let Some(record_batch) = msg.header_as_record_batch() {
        observation.record_batch_messages = 1;
        if record_batch
            .compression()
            .map(|compression| compression.codec() == arrow_ipc::CompressionType::ZSTD)
            .unwrap_or(false)
        {
            observation.zstd_record_batches = 1;
        }
        observation.rows = record_batch.length() as u64;
    }

    observation
}

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
    /// Acknowledgment emitted only after the client cleanly half-closes its request.
    BatchAckAfterRequestEof {
        ack_up_to_offset: i64,
        ack_up_to_records: u64,
    },
    /// Permanent status emitted only after the client cleanly half-closes its request.
    ErrorAfterRequestEof { status: Status },
    /// Keep the response open after request EOF to exercise the bounded drain fallback.
    HoldResponseAfterRequestEof,
    /// Error response - sent immediately when a batch arrives.
    Error { status: Status, delay_ms: u64 },
    /// Reject a connection's setup: send this error instead of the ready signal on the
    /// first (schema) message, simulating a failed reconnect. Consumes its scripted slot,
    /// so schedule it as the response the target (reconnect) connection reaches.
    FailSetup { status: Status },
    /// Reject a connection's setup after a delay.
    FailSetupAfter { status: Status, delay_ms: u64 },
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
    /// Observation of every `ack_up_to_records` value emitted on the auto-ack path,
    /// in emission order. Used by tests to assert acks are connection-relative.
    auto_ack_records: Arc<Mutex<Vec<u64>>>,
    /// Number of request bodies that ended with a clean client half-close.
    request_half_closes: Arc<AtomicU64>,
    /// Number of request bodies that ended with a transport error/reset.
    request_resets: Arc<AtomicU64>,
    /// One-shot observation that a scripted delayed ACK registered its timer.
    delayed_ack_armed: Arc<Notify>,
    /// Observation that a delayed setup rejection registered its timer.
    delayed_setup_armed: Arc<Notify>,
    /// Record-batch IPC messages whose header declares ZSTD body compression.
    zstd_compressed_record_batch_count: Arc<Mutex<u64>>,
    /// IPC messages carrying a DictionaryBatch header (distinct from record batches).
    dictionary_message_count: Arc<Mutex<u64>>,
    /// IPC messages carrying a RecordBatch header.
    record_batch_message_count: Arc<Mutex<u64>>,
    /// True when any observed IPC schema message declares dictionary encoding.
    wire_ipc_schema_has_dictionary_encoding: Arc<Mutex<bool>>,
    /// Logical UTF-8 column values decoded from received record batches.
    decoded_utf8_columns: Arc<Mutex<Vec<Vec<Option<String>>>>>,
}

impl MockFlightServer {
    pub fn new() -> Self {
        Self {
            responses: Arc::new(Mutex::new(HashMap::new())),
            max_offset_received: Arc::new(Mutex::new(-1)),
            batch_count: Arc::new(Mutex::new(0)),
            row_count: Arc::new(Mutex::new(0)),
            response_indices: Arc::new(Mutex::new(HashMap::new())),
            auto_ack_records: Arc::new(Mutex::new(Vec::new())),
            request_half_closes: Arc::new(AtomicU64::new(0)),
            request_resets: Arc::new(AtomicU64::new(0)),
            delayed_ack_armed: Arc::new(Notify::new()),
            delayed_setup_armed: Arc::new(Notify::new()),
            zstd_compressed_record_batch_count: Arc::new(Mutex::new(0)),
            dictionary_message_count: Arc::new(Mutex::new(0)),
            record_batch_message_count: Arc::new(Mutex::new(0)),
            wire_ipc_schema_has_dictionary_encoding: Arc::new(Mutex::new(false)),
            decoded_utf8_columns: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns a notification that fires immediately before the mock waits on a
    /// scripted delayed ACK.
    pub fn delayed_ack_armed(&self) -> Arc<Notify> {
        Arc::clone(&self.delayed_ack_armed)
    }

    /// Returns a notification that fires immediately before the mock waits on a
    /// scripted delayed setup rejection.
    pub fn delayed_setup_armed(&self) -> Arc<Notify> {
        Arc::clone(&self.delayed_setup_armed)
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

    /// Get every `ack_up_to_records` value emitted on the auto-ack path, in order.
    #[allow(dead_code)]
    pub async fn get_auto_ack_records(&self) -> Vec<u64> {
        self.auto_ack_records.lock().await.clone()
    }

    pub fn get_request_half_close_count(&self) -> u64 {
        self.request_half_closes.load(Ordering::Relaxed)
    }

    pub fn get_request_reset_count(&self) -> u64 {
        self.request_resets.load(Ordering::Relaxed)
    }

    /// Record-batch IPC messages whose header declares ZSTD body compression.
    pub async fn get_zstd_compressed_record_batch_count(&self) -> u64 {
        *self.zstd_compressed_record_batch_count.lock().await
    }

    /// IPC messages carrying a DictionaryBatch header.
    #[allow(dead_code)]
    pub async fn get_dictionary_message_count(&self) -> u64 {
        *self.dictionary_message_count.lock().await
    }

    /// IPC messages carrying a RecordBatch header.
    pub async fn get_record_batch_message_count(&self) -> u64 {
        *self.record_batch_message_count.lock().await
    }

    /// Whether any observed IPC schema message declares dictionary encoding.
    pub async fn wire_ipc_schema_has_dictionary_encoding(&self) -> bool {
        *self.wire_ipc_schema_has_dictionary_encoding.lock().await
    }

    /// Logical UTF-8 column values decoded from received record batches.
    pub async fn get_decoded_utf8_columns(&self) -> Vec<Vec<Option<String>>> {
        self.decoded_utf8_columns.lock().await.clone()
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
        self.auto_ack_records.lock().await.clear();
        self.request_half_closes.store(0, Ordering::Relaxed);
        self.request_resets.store(0, Ordering::Relaxed);
        *self.zstd_compressed_record_batch_count.lock().await = 0;
        *self.dictionary_message_count.lock().await = 0;
        *self.record_batch_message_count.lock().await = 0;
        *self.wire_ipc_schema_has_dictionary_encoding.lock().await = false;
        self.decoded_utf8_columns.lock().await.clear();
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
        let auto_ack_records = Arc::clone(&self.auto_ack_records);
        let request_half_closes = Arc::clone(&self.request_half_closes);
        let request_resets = Arc::clone(&self.request_resets);
        let delayed_ack_armed = Arc::clone(&self.delayed_ack_armed);
        let delayed_setup_armed = Arc::clone(&self.delayed_setup_armed);
        let zstd_compressed_record_batch_count =
            Arc::clone(&self.zstd_compressed_record_batch_count);
        let dictionary_message_count = Arc::clone(&self.dictionary_message_count);
        let record_batch_message_count = Arc::clone(&self.record_batch_message_count);
        let wire_ipc_schema_has_dictionary_encoding =
            Arc::clone(&self.wire_ipc_schema_has_dictionary_encoding);
        let decoded_utf8_columns = Arc::clone(&self.decoded_utf8_columns);

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
            let mut wire_schema: Option<Arc<Schema>> = None;

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

            let clean_request_eof = loop {
                let flight_data = match stream.message().await {
                    Ok(Some(flight_data)) => flight_data,
                    Ok(None) => {
                        request_half_closes.fetch_add(1, Ordering::Relaxed);
                        break true;
                    }
                    Err(error) => {
                        request_resets.fetch_add(1, Ordering::Relaxed);
                        warn!("Client request stream reset: {error}");
                        break false;
                    }
                };
                let observation = if !flight_data.data_header.is_empty() {
                    observe_ipc_data_header(&flight_data.data_header)
                } else {
                    IpcWireObservation {
                        dictionary_messages: 0,
                        record_batch_messages: 0,
                        zstd_record_batches: 0,
                        rows: 0,
                        schema_has_dictionary_encoding: false,
                    }
                };
                if observation.schema_has_dictionary_encoding {
                    *wire_ipc_schema_has_dictionary_encoding.lock().await = true;
                }
                if observation.dictionary_messages > 0 {
                    let mut count = dictionary_message_count.lock().await;
                    *count += observation.dictionary_messages;
                }
                if observation.record_batch_messages > 0 {
                    let mut count = record_batch_message_count.lock().await;
                    *count += observation.record_batch_messages;
                }
                if observation.zstd_record_batches > 0 {
                    let mut count = zstd_compressed_record_batch_count.lock().await;
                    *count += observation.zstd_record_batches;
                }
                // Handle schema message (first message has no app_metadata or empty app_metadata)
                if is_first_message {
                    is_first_message = false;
                    if flight_data.app_metadata.is_empty() {
                        wire_schema = Schema::try_from(&flight_data).ok().map(Arc::new);
                        // A scripted FailSetup rejects this connection's setup: send the
                        // error instead of the ready signal (simulates a reconnect failure).
                        let setup_failure = match stream_responses.get(response_index) {
                            Some(MockFlightResponse::FailSetup { status }) => {
                                Some((status.clone(), 0))
                            }
                            Some(MockFlightResponse::FailSetupAfter { status, delay_ms }) => {
                                Some((status.clone(), *delay_ms))
                            }
                            _ => None,
                        };
                        if let Some((status, delay_ms)) = setup_failure {
                            response_index += 1;
                            {
                                let mut indices = response_indices.lock().await;
                                indices.insert(table_name.clone(), response_index);
                            }
                            if delay_ms > 0 {
                                delayed_setup_armed.notify_one();
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

                    // Decode row counts from record-batch IPC headers when batch metadata is present.
                    if observation.rows > 0 {
                        // Connection-local counter drives acks (mirrors the server's
                        // per-connection cumulative tracker); the global row_count is
                        // kept only as a cross-connection observation metric.
                        connection_record_count += observation.rows;
                        let mut count = row_count.lock().await;
                        *count += observation.rows;
                    }
                    if observation.record_batch_messages > 0 {
                        if let Some(schema) = &wire_schema {
                            match arrow_flight::utils::flight_data_to_arrow_batch(
                                &flight_data,
                                Arc::clone(schema),
                                &HashMap::new(),
                            ) {
                                Ok(batch) => {
                                    let mut decoded = decoded_utf8_columns.lock().await;
                                    for column in batch.columns() {
                                        if let Some(strings) =
                                            column.as_any().downcast_ref::<StringArray>()
                                        {
                                            decoded.push(
                                                strings
                                                    .iter()
                                                    .map(|value| value.map(str::to_string))
                                                    .collect(),
                                            );
                                        }
                                    }
                                }
                                Err(error) => {
                                    warn!("Failed to decode received record batch: {error}");
                                }
                            }
                        }
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
                                    delayed_ack_armed.notify_one();
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
                        MockFlightResponse::BatchAckAfterRequestEof { .. }
                        | MockFlightResponse::ErrorAfterRequestEof { .. }
                        | MockFlightResponse::HoldResponseAfterRequestEof => {
                            debug!("Waiting for request EOF before sending scripted response");
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
                                delayed_setup_armed.notify_one();
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
            };

            if clean_request_eof {
                match stream_responses.get(response_index) {
                    Some(MockFlightResponse::BatchAckAfterRequestEof {
                        ack_up_to_offset,
                        ack_up_to_records,
                    }) => {
                        let metadata = FlightAckMetadata {
                            ack_up_to_offset: *ack_up_to_offset,
                            ack_up_to_records: *ack_up_to_records,
                        };
                        let _ = tx
                            .send(Ok(PutResult {
                                app_metadata: serde_json::to_vec(&metadata).unwrap().into(),
                            }))
                            .await;
                        response_index += 1;
                        response_indices
                            .lock()
                            .await
                            .insert(table_name.clone(), response_index);
                    }
                    Some(MockFlightResponse::ErrorAfterRequestEof { status }) => {
                        let _ = tx.send(Err(status.clone())).await;
                        response_index += 1;
                        response_indices
                            .lock()
                            .await
                            .insert(table_name.clone(), response_index);
                    }
                    Some(MockFlightResponse::HoldResponseAfterRequestEof) => {
                        response_index += 1;
                        response_indices
                            .lock()
                            .await
                            .insert(table_name.clone(), response_index);
                        std::future::pending::<()>().await;
                    }
                    _ => {}
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
        response_indices: Arc::clone(&mock_server.response_indices),
        auto_ack_records: Arc::clone(&mock_server.auto_ack_records),
        request_half_closes: Arc::clone(&mock_server.request_half_closes),
        request_resets: Arc::clone(&mock_server.request_resets),
        delayed_ack_armed: Arc::clone(&mock_server.delayed_ack_armed),
        delayed_setup_armed: Arc::clone(&mock_server.delayed_setup_armed),
        zstd_compressed_record_batch_count: Arc::clone(
            &mock_server.zstd_compressed_record_batch_count,
        ),
        dictionary_message_count: Arc::clone(&mock_server.dictionary_message_count),
        record_batch_message_count: Arc::clone(&mock_server.record_batch_message_count),
        wire_ipc_schema_has_dictionary_encoding: Arc::clone(
            &mock_server.wire_ipc_schema_has_dictionary_encoding,
        ),
        decoded_utf8_columns: Arc::clone(&mock_server.decoded_utf8_columns),
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
