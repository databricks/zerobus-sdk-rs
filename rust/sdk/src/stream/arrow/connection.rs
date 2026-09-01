//! Arrow Flight transport construction and request encoding.
//!
//! A `FlightConnection` owns both halves of one DoPut exchange. Request shutdown
//! is observable so rotation can half-close without dropping the HTTP/2 stream.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::task::Poll;

use arrow_array::Array;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::{FlightClient, FlightData, PutResult};
use futures::{stream::poll_fn, Stream, StreamExt};
use tokio::sync::{mpsc, watch, Mutex};
use tokio::time::{timeout, timeout_at, Duration, Instant};
use tokio_util::sync::CancellationToken;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tracing::{error, info, warn};

use std::sync::Mutex as StdMutex;

use super::batch::make_ipc_write_options;
use super::metadata::{FlightAckMetadata, FlightBatchMetadata};
use super::{
    configured_deadline, ArrowStreamConfigurationOptions, ArrowTableProperties, BatchItem,
    FlightConnectionParameters, RecordBatch, ZerobusArrowStream,
};
use crate::errors::ZerobusError;
use crate::headers_provider::HeadersProvider;
use crate::offset_generator::OffsetId;
use crate::proxy::{self, ConnectorFactory};
use crate::stats::{BatchStats, StatsExporter, StreamStat};
use crate::tls_config::TlsConfig;
use crate::ZerobusResult;

/// Best-effort uncompressed payload size of `batch`: the Arrow data bytes of its
/// columns for the sliced rows (`ArrayData::get_slice_memory_size`). Cheap — no
/// re-encode, and codec-independent (an in-memory `RecordBatch` is never LZ4/ZSTD
/// compressed; that applies only to the IPC wire form). Excludes IPC framing.
fn uncompressed_ipc_bytes(batch: &RecordBatch) -> u64 {
    batch
        .columns()
        .iter()
        .map(|col| col.to_data().get_slice_memory_size().unwrap_or(0) as u64)
        .sum()
}

/// The batch currently being encoded: its offset and row count plus the
/// uncompressed payload size (known at pull) and wire bytes accumulated across the
/// FlightData frames it produces. Held in a single slot (not a per-offset map)
/// because the encoder drains one batch's frames before pulling the next.
struct EncodeInProgress {
    offset: OffsetId,
    records: u64,
    uncompressed: u64,
    wire: u64,
}

/// Emits a `BatchSent` for the in-progress batch (if any) and clears the slot.
/// Called when the next batch is pulled (previous batch's frames are all out), at
/// natural end-of-stream, and on graceful close.
fn flush_sent(exporter: &dyn StatsExporter, pending: &StdMutex<Option<EncodeInProgress>>) {
    let done = pending.lock().unwrap().take();
    if let Some(p) = done {
        exporter.record(StreamStat::BatchSent {
            offset: p.offset,
            stats: BatchStats {
                records: p.records,
                wire_bytes: p.wire,
                uncompressed_bytes: p.uncompressed,
            },
        });
    }
}

pub(super) type FlightResponseStream =
    Pin<Box<dyn Stream<Item = Result<PutResult, FlightError>> + Send>>;
type FlightRequestStream = Pin<Box<dyn Stream<Item = Result<FlightData, FlightError>> + Send>>;

/// Stops one Flight request body without dropping the HTTP/2 stream and reports when
/// tonic has polled that body to EOF.
#[derive(Clone)]
pub(super) struct RequestBodyControl {
    shutdown: CancellationToken,
    /// Set before `shutdown` on a graceful close so the request body flushes the
    /// last batch's `BatchSent`; left false on recovery/rotation aborts (that batch
    /// is replayed and re-emitted on the next connection).
    graceful: Arc<std::sync::atomic::AtomicBool>,
    eof_rx: watch::Receiver<bool>,
}

impl RequestBodyControl {
    #[cfg(test)]
    pub(super) fn completed_for_test() -> Self {
        let (_eof_tx, eof_rx) = watch::channel(true);
        Self {
            shutdown: CancellationToken::new(),
            graceful: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            eof_rx,
        }
    }

    pub(super) fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Graceful shutdown: flush the last batch's telemetry before finishing.
    pub(super) fn shutdown_graceful(&self) {
        self.graceful.store(true, Ordering::Release);
        self.shutdown.cancel();
    }

    pub(super) fn is_finished(&self) -> bool {
        *self.eof_rx.borrow() || self.eof_rx.has_changed().is_err()
    }

    pub(super) async fn wait_for_eof(&self) {
        let mut eof_rx = self.eof_rx.clone();
        loop {
            if *eof_rx.borrow_and_update() {
                return;
            }
            if eof_rx.changed().await.is_err() {
                // RequestBodyStreamState::finish/Drop drops encoded channel state and
                // publishes EOF before dropping this sender, so closure also proves
                // every request-body owner was released.
                return;
            }
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct RequestBodyRegistry {
    controls: Arc<Mutex<Vec<RequestBodyControl>>>,
}

impl RequestBodyRegistry {
    pub(super) async fn register(&self, control: &RequestBodyControl) {
        let mut controls = self.controls.lock().await;
        controls.retain(|registered| !registered.is_finished());
        controls.push(control.clone());
    }

    #[cfg(any(feature = "internal-arrow-c-data", test))]
    pub(super) async fn shutdown_all(&self) {
        let mut controls = self.controls.lock().await;
        controls.retain(|control| !control.is_finished());
        for control in controls.iter() {
            control.shutdown();
        }
    }

    pub(super) fn try_shutdown_all(&self) {
        if let Ok(mut controls) = self.controls.try_lock() {
            controls.retain(|control| !control.is_finished());
            for control in controls.iter() {
                control.shutdown();
            }
        }
    }

    #[cfg(any(feature = "internal-arrow-c-data", test))]
    pub(super) async fn wait_for_all_eof(&self) {
        loop {
            let controls = {
                let mut registered = self.controls.lock().await;
                registered.retain(|control| !control.is_finished());
                registered.clone()
            };
            if controls.is_empty() {
                return;
            }
            for control in controls {
                control.wait_for_eof().await;
            }
        }
    }

    #[cfg(test)]
    async fn registered_count(&self) -> usize {
        self.controls.lock().await.len()
    }
}

#[cfg(feature = "test-hooks")]
struct RequestBodyTestState {
    hooks: Arc<super::TestHooks>,
    shutdown_barrier: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    before_batch_poll_barrier: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

#[cfg(feature = "test-hooks")]
impl RequestBodyTestState {
    fn new(hooks: Arc<super::TestHooks>) -> Self {
        Self {
            hooks,
            shutdown_barrier: None,
            before_batch_poll_barrier: None,
        }
    }

    fn poll_shutdown_barrier(&mut self, cx: &mut std::task::Context<'_>) -> Poll<()> {
        let barrier = self.shutdown_barrier.get_or_insert_with(|| {
            let hooks = Arc::clone(&self.hooks);
            Box::pin(async move {
                let barrier = hooks.request_body_shutdown.lock().await.take();
                if let Some(barrier) = barrier {
                    barrier.reached.notify_one();
                    barrier.proceed.notified().await;
                }
            })
        });
        barrier.as_mut().poll(cx)
    }

    fn poll_before_batch_barrier(&mut self, cx: &mut std::task::Context<'_>) -> Poll<()> {
        if self.before_batch_poll_barrier.is_none() {
            let barrier = self
                .hooks
                .request_body_before_batch_poll
                .try_lock()
                .ok()
                .and_then(|mut gate| gate.take());
            if let Some(barrier) = barrier {
                self.before_batch_poll_barrier = Some(Box::pin(async move {
                    barrier.reached.notify_one();
                    barrier.proceed.notified().await;
                }));
            }
        }
        if let Some(barrier) = self.before_batch_poll_barrier.as_mut() {
            if barrier.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            self.before_batch_poll_barrier = None;
        }
        Poll::Ready(())
    }
}

struct RequestBodyStreamState {
    encoded: Option<FlightRequestStream>,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
    eof_tx: Option<watch::Sender<bool>>,
    #[cfg(feature = "test-hooks")]
    test: RequestBodyTestState,
}

impl RequestBodyStreamState {
    fn finish(&mut self) {
        drop(self.encoded.take());
        if let Some(eof_tx) = self.eof_tx.take() {
            eof_tx.send_replace(true);
        }
    }
}

impl Drop for RequestBodyStreamState {
    fn drop(&mut self) {
        self.finish();
    }
}

/// State owned by a single active DoPut connection.
pub(super) struct FlightConnection {
    response_stream: FlightResponseStream,
    batch_tx: mpsc::Sender<BatchItem>,
    request_body: RequestBodyControl,
}

impl FlightConnection {
    pub(super) fn sender(&self) -> mpsc::Sender<BatchItem> {
        self.batch_tx.clone()
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        FlightResponseStream,
        mpsc::Sender<BatchItem>,
        RequestBodyControl,
    ) {
        (self.response_stream, self.batch_tx, self.request_body)
    }

    pub(super) fn into_supervisor_io(self) -> (FlightResponseStream, RequestBodyControl) {
        let (response_stream, batch_tx, request_body) = self.into_parts();
        // The public sender slot owns the sender used for normal ingestion.
        drop(batch_tx);
        (response_stream, request_body)
    }
}

impl ZerobusArrowStream {
    /// Attempts to establish a Flight connection.
    /// Returns the complete connection on success.
    pub(super) async fn try_connect(
        parameters: &FlightConnectionParameters<'_>,
    ) -> ZerobusResult<FlightConnection> {
        // Share one deadline across connection setup and auth-rejection invalidation.
        // This preserves the original auth error if a custom provider stalls instead of
        // reclassifying the attempt as a retryable setup timeout.
        let attempt_timeout = Duration::from_millis(parameters.options.recovery_timeout_ms);
        let attempt_started = Instant::now();
        let attempt_deadline =
            configured_deadline(attempt_started, attempt_timeout, "recovery_timeout_ms")?;
        let result = timeout_at(attempt_deadline, async {
            let client = Self::create_flight_client(
                parameters.endpoint,
                parameters.tls_config,
                parameters.connector_factory,
                parameters.table_properties,
                parameters.options,
                parameters.headers_provider,
                parameters.sdk_identifier,
            )
            .await?;

            Self::start_stream_connection(
                client,
                parameters.table_properties,
                parameters.options,
                parameters.request_bodies,
                parameters.stats_exporter.clone(),
                #[cfg(feature = "test-hooks")]
                Arc::clone(parameters.test_hooks),
            )
            .await
        })
        .await
        .map_err(|_| {
            ZerobusError::CreateStreamError(tonic::Status::deadline_exceeded(
                "Stream creation timed out",
            ))
        })?;

        match result {
            Ok(connection) => Ok(connection),
            Err(error) => {
                // Drop the rejected token so the next attempt re-mints. A provider must
                // not be able to turn a known auth rejection into repeated generic
                // timeout retries by stalling here.
                if error.is_auth_rejection()
                    && timeout_at(attempt_deadline, parameters.headers_provider.invalidate())
                        .await
                        .is_err()
                {
                    warn!(target: super::LOG_TARGET,
                        timeout_ms = parameters.options.recovery_timeout_ms,
                        "Initial headers provider invalidation timed out; preserving auth rejection"
                    );
                }
                Err(error)
            }
        }
    }

    /// Creates a Flight client connected to the endpoint.
    async fn create_flight_client(
        endpoint: &str,
        tls_config: &Arc<dyn TlsConfig>,
        connector_factory: Option<&ConnectorFactory>,
        table_properties: &ArrowTableProperties,
        options: &ArrowStreamConfigurationOptions,
        headers_provider: &Arc<dyn HeadersProvider>,
        sdk_identifier: &str,
    ) -> ZerobusResult<FlightClient> {
        let connection_timeout = Duration::from_millis(options.connection_timeout_ms);

        let base_endpoint = Channel::from_shared(endpoint.to_string())
            .map_err(|e| ZerobusError::ChannelCreationError(e.to_string()))?
            .user_agent(sdk_identifier)
            .map_err(|e| ZerobusError::ChannelCreationError(e.to_string()))?
            .connect_timeout(connection_timeout)
            .timeout(connection_timeout);

        let configured_endpoint = tls_config.configure_endpoint(base_endpoint)?;
        let host = configured_endpoint.uri().host().unwrap_or_default();
        let channel = match proxy::resolve_connector(host, connector_factory)? {
            Some(proxy_connector) => {
                configured_endpoint.connect_with_connector_lazy(proxy_connector)
            }
            None => configured_endpoint.connect_lazy(),
        };

        let mut client = FlightClient::new(channel);

        // Add headers from the provider first, filtering out reserved headers.
        // The table name header is authoritative and must not be overridden.
        const TABLE_NAME_HEADER: &str = "x-databricks-zerobus-table-name";
        const AUTHORIZATION_HEADER: &str = "authorization";
        let headers = headers_provider.get_headers().await?;
        for (key, value) in headers {
            if key.eq_ignore_ascii_case(TABLE_NAME_HEADER) {
                warn!(target: super::LOG_TARGET,
                    "HeadersProvider attempted to set reserved header '{}', ignoring",
                    TABLE_NAME_HEADER
                );
                continue;
            }
            if key.eq_ignore_ascii_case(AUTHORIZATION_HEADER) {
                let mut auth_value = MetadataValue::try_from(value.as_str()).map_err(|_| {
                    error!(target: super::LOG_TARGET, table_name = %table_properties.table_name, "authorization token is not a valid HTTP header value");
                    ZerobusError::InvalidUCTokenError(
                        "authorization token is not a valid HTTP header value".to_string(),
                    )
                })?;
                auth_value.set_sensitive(true);
                client
                    .metadata_mut()
                    .insert(AUTHORIZATION_HEADER, auth_value);
                continue;
            }
            client.add_header(key, &value).map_err(|e| {
                ZerobusError::InvalidArgument(format!("Failed to add header '{}': {}", key, e))
            })?;
        }

        // Add the required table name header (authoritative, added last to ensure it's set).
        client
            .add_header(TABLE_NAME_HEADER, &table_properties.table_name)
            .map_err(|e| {
                ZerobusError::InvalidArgument(format!("Failed to add table name header: {}", e))
            })?;

        Ok(client)
    }

    /// Builds a request body that can be stopped at a FlightData boundary and observed
    /// reaching EOF without dropping the surrounding HTTP/2 stream.
    fn make_request_stream(
        batch_rx: mpsc::Receiver<BatchItem>,
        table_properties: &ArrowTableProperties,
        options: &ArrowStreamConfigurationOptions,
        stats_exporter: Option<Arc<dyn StatsExporter>>,
        #[cfg(feature = "test-hooks")] test_hooks: Arc<super::TestHooks>,
    ) -> ZerobusResult<(FlightRequestStream, RequestBodyControl)> {
        let ipc_write_options = make_ipc_write_options(options.ipc_compression)?;
        let schema = Arc::clone(&table_properties.schema);
        // When an exporter is registered, track the batch being encoded in a single
        // slot: measure its uncompressed size at pull, sum wire bytes across the frames
        // it produces, and emit `BatchSent` once the whole batch is out — when the next
        // batch is pulled (the encoder drains one batch's frames before pulling the
        // next), at natural end-of-stream, or on graceful close. A batch cut off by a
        // recovery/rotation cancel is dropped and re-emitted on the next connection.
        let pending: Arc<StdMutex<Option<EncodeInProgress>>> = Arc::new(StdMutex::new(None));

        let stats_in = stats_exporter.clone();
        let pending_in = Arc::clone(&pending);
        let batch_stream =
            tokio_stream::wrappers::ReceiverStream::new(batch_rx).map(move |result| {
                result.map(|(offset, batch)| {
                    if let Some(exporter) = &stats_in {
                        // The previous batch's frames are all out — emit it, then start
                        // accounting for this one.
                        flush_sent(exporter.as_ref(), &pending_in);
                        *pending_in.lock().unwrap() = Some(EncodeInProgress {
                            offset,
                            records: batch.num_rows() as u64,
                            uncompressed: uncompressed_ipc_bytes(&batch),
                            wire: 0,
                        });
                    }
                    batch
                })
            });
        let offset_counter = Arc::new(AtomicI64::new(0));
        let offset_counter_clone = Arc::clone(&offset_counter);
        let stats_out = stats_exporter.clone();
        let pending_out = Arc::clone(&pending);
        let encoded: FlightRequestStream = Box::pin(
            FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .with_options(ipc_write_options)
                .build(batch_stream)
                .enumerate()
                .map(move |(idx, result)| {
                    result.map(|mut flight_data| {
                        if idx > 0 {
                            let offset = offset_counter_clone.fetch_add(1, Ordering::Relaxed);
                            let metadata = FlightBatchMetadata::new(offset);
                            if let Ok(bytes) = metadata.to_bytes() {
                                flight_data.app_metadata = bytes.into();
                            }
                            if stats_out.is_some() {
                                let wire = (flight_data.data_header.len()
                                    + flight_data.data_body.len()
                                    + flight_data.app_metadata.len())
                                    as u64;
                                if let Some(p) = pending_out.lock().unwrap().as_mut() {
                                    p.wire += wire;
                                }
                            }
                        }
                        flight_data
                    })
                }),
        );

        let shutdown = CancellationToken::new();
        let graceful = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (eof_tx, eof_rx) = watch::channel(false);
        let mut state = RequestBodyStreamState {
            encoded: Some(encoded),
            cancelled: Box::pin(shutdown.clone().cancelled_owned()),
            eof_tx: Some(eof_tx),
            #[cfg(feature = "test-hooks")]
            test: RequestBodyTestState::new(test_hooks),
        };
        let stats_eof = stats_exporter;
        let pending_eof = pending;
        let graceful_poll = Arc::clone(&graceful);
        let controlled = poll_fn(move |cx| {
            if state.encoded.is_none() {
                return Poll::Ready(None);
            }
            if state.cancelled.as_mut().poll(cx).is_ready() {
                #[cfg(feature = "test-hooks")]
                if state.test.poll_shutdown_barrier(cx).is_pending() {
                    return Poll::Pending;
                }
                // Graceful close: the last batch was fully sent + acked and will not be
                // replayed, so flush its BatchSent. Recovery/rotation aborts leave
                // `graceful` false and drop the batch — it is re-sent (and re-emitted)
                // on the next connection.
                if graceful_poll.load(Ordering::Acquire) {
                    if let Some(exporter) = &stats_eof {
                        flush_sent(exporter.as_ref(), &pending_eof);
                    }
                }
                state.finish();
                return Poll::Ready(None);
            }
            #[cfg(feature = "test-hooks")]
            if state.test.poll_before_batch_barrier(cx).is_pending() {
                return Poll::Pending;
            }
            let encoded = state
                .encoded
                .as_mut()
                .expect("request body completion checked before polling");
            match encoded.as_mut().poll_next(cx) {
                Poll::Ready(None) => {
                    // Natural end-of-stream: emit the last batch's BatchSent.
                    if let Some(exporter) = &stats_eof {
                        flush_sent(exporter.as_ref(), &pending_eof);
                    }
                    state.finish();
                    Poll::Ready(None)
                }
                result => result,
            }
        });

        Ok((
            Box::pin(controlled),
            RequestBodyControl {
                shutdown,
                graceful,
                eof_rx,
            },
        ))
    }

    /// Starts the Flight stream with the given client.
    /// Returns the complete active connection for use by the supervisor.
    ///
    /// This method waits for the server's "ready" signal (ack_up_to_offset = -1)
    /// to confirm that stream setup succeeded (auth, schema validation, table access).
    /// This allows setup errors to be detected during stream creation rather than
    /// later during batch ingestion.
    async fn start_stream_connection(
        mut client: FlightClient,
        table_properties: &ArrowTableProperties,
        options: &ArrowStreamConfigurationOptions,
        request_bodies: &RequestBodyRegistry,
        stats_exporter: Option<Arc<dyn StatsExporter>>,
        #[cfg(feature = "test-hooks")] test_hooks: Arc<super::TestHooks>,
    ) -> ZerobusResult<FlightConnection> {
        // Create channel for sending RecordBatches.
        let (batch_tx, batch_rx) = mpsc::channel::<BatchItem>(options.max_inflight_batches);

        let (flight_data_stream, request_body) = Self::make_request_stream(
            batch_rx,
            table_properties,
            options,
            stats_exporter,
            #[cfg(feature = "test-hooks")]
            test_hooks,
        )?;
        // Register before tonic takes ownership so every do_put/setup exit remains
        // observable by destructive free and cannot outlive the C Data owner guarantee.
        request_bodies.register(&request_body).await;

        // Start the DoPut stream.
        let mut response_stream = client
            .do_put(flight_data_stream)
            .await
            // `.into()` preserves the inner gRPC code; `Status::from_error` would
            // flatten it to `Unknown` and break auth/retry classification.
            .map_err(|e| ZerobusError::CreateStreamError(e.into()))?;

        // Wait for server's "ready" signal to confirm setup succeeded.
        // The server sends ack_up_to_offset = -1 after successful auth, schema validation,
        // and stream setup. This allows us to detect setup errors early.
        let setup_timeout = Duration::from_millis(options.connection_timeout_ms);
        match timeout(setup_timeout, response_stream.next()).await {
            Ok(Some(Ok(put_result))) => {
                // Parse the ack metadata to verify it's the ready signal.
                match FlightAckMetadata::from_bytes(&put_result.app_metadata) {
                    Ok(metadata) if metadata.is_stream_ready() => {
                        info!(target: super::LOG_TARGET, "Stream setup confirmed by server (ready signal received)");
                    }
                    Ok(metadata) => {
                        // Unexpected: got a real ack before sending any batches - protocol error.
                        error!(target: super::LOG_TARGET,
                            "Unexpected ack during setup (offset {}), expected ready signal",
                            metadata.ack_up_to_offset
                        );
                        return Err(ZerobusError::UnexpectedStreamResponseError(format!(
                            "Expected ready signal, got ack for offset {}",
                            metadata.ack_up_to_offset
                        )));
                    }
                    Err(e) => {
                        // Malformed metadata - protocol error.
                        error!(target: super::LOG_TARGET, "Failed to parse setup response metadata: {}", e);
                        return Err(ZerobusError::UnexpectedStreamResponseError(format!(
                            "Malformed setup response metadata: {}",
                            e
                        )));
                    }
                }
            }
            Ok(Some(Err(flight_error))) => {
                // Server sent an error during setup (auth failed, schema mismatch, blocked table, etc.)
                error!(target: super::LOG_TARGET, "Stream setup failed: {:?}", flight_error);
                // Classify so a schema mismatch surfaces as ZerobusError::InvalidSchema
                // rather than a generic CreateStreamError.
                return Err(ZerobusError::from_setup_status(flight_error.into()));
            }
            Ok(None) => {
                // Server closed the stream without sending anything.
                error!(target: super::LOG_TARGET, "Server closed stream during setup without response");
                return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                    "Server closed stream during setup",
                )));
            }
            Err(_timeout) => {
                // Timeout waiting for server response.
                error!(target: super::LOG_TARGET,
                    "Timed out waiting for server setup confirmation ({}ms)",
                    options.connection_timeout_ms
                );
                return Err(ZerobusError::ConnectionTimeout(format!(
                    "Timed out waiting for server setup confirmation ({}ms)",
                    options.connection_timeout_ms
                )));
            }
        }

        Ok(FlightConnection {
            response_stream: Box::pin(response_stream),
            batch_tx,
            request_body,
        })
    }

    /// Establishes a replacement DoPut transport and validates its ready signal.
    pub(super) async fn reconnect_transport(
        parameters: &FlightConnectionParameters<'_>,
    ) -> ZerobusResult<FlightConnection> {
        let client = Self::create_flight_client(
            parameters.endpoint,
            parameters.tls_config,
            parameters.connector_factory,
            parameters.table_properties,
            parameters.options,
            parameters.headers_provider,
            parameters.sdk_identifier,
        )
        .await?;

        let (batch_tx, batch_rx) =
            mpsc::channel::<BatchItem>(parameters.options.max_inflight_batches);
        let (flight_data_stream, request_body) = Self::make_request_stream(
            batch_rx,
            parameters.table_properties,
            parameters.options,
            parameters.stats_exporter.clone(),
            #[cfg(feature = "test-hooks")]
            Arc::clone(parameters.test_hooks),
        )?;
        // Register before tonic takes ownership so replay can never target an
        // untracked request body, including when do_put or READY setup fails.
        parameters.request_bodies.register(&request_body).await;

        let mut flight_client = client;
        let mut response_stream = flight_client
            .do_put(flight_data_stream)
            .await
            // `.into()` preserves the inner gRPC code; `Status::from_error` would
            // flatten it to `Unknown` and break auth/retry classification.
            .map_err(|e| ZerobusError::CreateStreamError(e.into()))?;

        let setup_timeout = Duration::from_millis(parameters.options.connection_timeout_ms);
        match timeout(setup_timeout, response_stream.next()).await {
            Ok(Some(Ok(put_result))) => {
                match FlightAckMetadata::from_bytes(&put_result.app_metadata) {
                    Ok(metadata) if metadata.is_stream_ready() => {
                        info!(target: super::LOG_TARGET, "Reconnection confirmed by server (ready signal received)");
                    }
                    Ok(metadata) => {
                        error!(target: super::LOG_TARGET,
                            "Unexpected ack during reconnect (offset {}), expected ready signal",
                            metadata.ack_up_to_offset
                        );
                        return Err(ZerobusError::UnexpectedStreamResponseError(format!(
                            "Expected ready signal, got ack for offset {}",
                            metadata.ack_up_to_offset
                        )));
                    }
                    Err(e) => {
                        error!(target: super::LOG_TARGET, "Failed to parse reconnect response metadata: {}", e);
                        return Err(ZerobusError::UnexpectedStreamResponseError(format!(
                            "Malformed reconnect response metadata: {}",
                            e
                        )));
                    }
                }
            }
            Ok(Some(Err(flight_error))) => {
                error!(target: super::LOG_TARGET, "Reconnection setup failed: {:?}", flight_error);
                return Err(ZerobusError::from_setup_status(flight_error.into()));
            }
            Ok(None) => {
                error!(target: super::LOG_TARGET, "Server closed stream during reconnect without response");
                return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                    "Server closed stream during reconnect",
                )));
            }
            Err(_timeout) => {
                error!(target: super::LOG_TARGET,
                    "Timed out waiting for server reconnect confirmation ({}ms)",
                    parameters.options.connection_timeout_ms
                );
                return Err(ZerobusError::ConnectionTimeout(format!(
                    "Timed out waiting for server reconnect confirmation ({}ms)",
                    parameters.options.connection_timeout_ms
                )));
            }
        }

        Ok(FlightConnection {
            response_stream: Box::pin(response_stream),
            batch_tx,
            request_body,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::StreamExt;
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};

    use super::super::ArrowSchema;
    use super::{
        ArrowStreamConfigurationOptions, ArrowTableProperties, BatchItem, FlightClient,
        FlightConnection, FlightResponseStream, HeadersProvider, RequestBodyControl,
        RequestBodyRegistry, StatsExporter, TlsConfig, ZerobusArrowStream, ZerobusResult,
    };

    struct PassthroughTlsConfig;

    impl TlsConfig for PassthroughTlsConfig {
        fn configure_endpoint(
            &self,
            endpoint: tonic::transport::Endpoint,
        ) -> ZerobusResult<tonic::transport::Endpoint> {
            Ok(endpoint)
        }
    }

    struct TestHeadersProvider;

    #[async_trait]
    impl HeadersProvider for TestHeadersProvider {
        async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
            Ok(HashMap::from([
                ("authorization", "Bearer secret-token".to_string()),
                ("x-custom-header", "custom-value".to_string()),
            ]))
        }
    }

    #[tokio::test]
    async fn supervisor_handoff_does_not_retain_redundant_sender() {
        let (batch_tx, mut batch_rx) = mpsc::channel::<BatchItem>(1);
        let response_stream: FlightResponseStream = Box::pin(futures::stream::pending());
        let connection = FlightConnection {
            response_stream,
            batch_tx,
            request_body: RequestBodyControl::completed_for_test(),
        };

        let public_sender = connection.sender();
        let (_response_stream, _request_body) = connection.into_supervisor_io();
        drop(public_sender);

        let received = timeout(Duration::from_secs(1), batch_rx.recv())
            .await
            .expect("receiver should observe sender closure");
        assert!(received.is_none());
    }

    #[tokio::test]
    async fn request_body_control_clone_stops_body_and_reports_owner_drop() {
        let table_properties = ArrowTableProperties {
            table_name: "catalog.schema.table".to_string(),
            schema: Arc::new(ArrowSchema::empty()),
        };
        let (_batch_tx, batch_rx) = mpsc::channel(1);
        let (mut request_body, control) = ZerobusArrowStream::make_request_stream(
            batch_rx,
            &table_properties,
            &ArrowStreamConfigurationOptions::default(),
            None,
            #[cfg(feature = "test-hooks")]
            Arc::new(super::super::TestHooks::default()),
        )
        .unwrap();
        let cloned = control.clone();

        cloned.shutdown();
        assert!(request_body.next().await.is_none());
        timeout(Duration::from_secs(1), control.wait_for_eof())
            .await
            .expect("request body completion must reach every control clone");
    }

    #[tokio::test]
    async fn make_request_stream_emits_batch_sent() {
        use arrow_array::Int32Array;
        use arrow_schema::{DataType, Field};

        use crate::stats::{channel_exporter, StreamStat};

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let table_properties = ArrowTableProperties {
            table_name: "catalog.schema.table".to_string(),
            schema: Arc::clone(&schema),
        };
        let make_batch = |rows: i32| {
            super::super::RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from((0..rows).collect::<Vec<_>>()))],
            )
            .unwrap()
        };

        let (exporter, mut rx) = channel_exporter(8);
        let (batch_tx, batch_rx) = mpsc::channel::<BatchItem>(4);
        let (mut request_body, _control) = ZerobusArrowStream::make_request_stream(
            batch_rx,
            &table_properties,
            &ArrowStreamConfigurationOptions::default(),
            Some(exporter as Arc<dyn StatsExporter>),
            #[cfg(feature = "test-hooks")]
            Arc::new(super::super::TestHooks::default()),
        )
        .unwrap();

        // Two batches at non-contiguous offsets: the first is flushed when the second
        // is pulled, the second (last) is flushed at end-of-stream.
        batch_tx.send(Ok((7, make_batch(100)))).await.unwrap();
        batch_tx.send(Ok((9, make_batch(200)))).await.unwrap();
        drop(batch_tx);
        while request_body.next().await.is_some() {}

        let mut sent = Vec::new();
        while let Ok(stat) = rx.try_recv() {
            if let StreamStat::BatchSent { offset, stats } = stat {
                sent.push((offset, stats));
            }
        }
        assert_eq!(
            sent.len(),
            2,
            "one BatchSent per batch, incl. the last at EOF"
        );
        let (o0, s0) = sent[0];
        let (o1, s1) = sent[1];
        assert_eq!(o0, 7);
        assert_eq!(s0.records, 100);
        assert_eq!(o1, 9);
        assert_eq!(s1.records, 200);
        for (_, s) in &sent {
            assert!(s.uncompressed_bytes > 0, "uncompressed payload recorded");
            assert!(
                s.wire_bytes >= s.uncompressed_bytes,
                "no compression: wire frame (incl. framing) >= payload: wire={} uncompressed={}",
                s.wire_bytes,
                s.uncompressed_bytes
            );
        }
    }

    /// A batch fully sent but not yet flushed (no successor, no EOF) is emitted on a
    /// graceful close, but dropped on a recovery/rotation cancel.
    #[cfg(feature = "arrow-flight")]
    #[tokio::test]
    async fn make_request_stream_last_batch_flush_depends_on_graceful() {
        use arrow_array::Int32Array;
        use arrow_schema::{DataType, Field};

        use crate::offset_generator::OffsetId;
        use crate::stats::{channel_exporter, StreamStat};

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let table_properties = ArrowTableProperties {
            table_name: "catalog.schema.table".to_string(),
            schema: Arc::clone(&schema),
        };
        let batch = super::super::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from((0..50).collect::<Vec<_>>()))],
        )
        .unwrap();

        // Drive one batch fully through the encoder (schema + data frames) while
        // keeping batch_tx open (no EOF), then shut down and observe.
        async fn drive_then_shutdown(
            table_properties: &ArrowTableProperties,
            batch: super::super::RecordBatch,
            graceful: bool,
        ) -> Vec<OffsetId> {
            let (exporter, mut rx) = channel_exporter(8);
            let (batch_tx, batch_rx) = mpsc::channel::<BatchItem>(4);
            let (mut request_body, control) = ZerobusArrowStream::make_request_stream(
                batch_rx,
                table_properties,
                &ArrowStreamConfigurationOptions::default(),
                Some(exporter as Arc<dyn StatsExporter>),
                #[cfg(feature = "test-hooks")]
                Arc::new(super::super::TestHooks::default()),
            )
            .unwrap();
            batch_tx.send(Ok((7, batch))).await.unwrap();
            // Pull the batch's frames (schema + record batch) so it is fully sent, but
            // do NOT drop batch_tx — no natural EOF, so its BatchSent stays pending.
            request_body.next().await;
            request_body.next().await;
            if graceful {
                control.shutdown_graceful();
            } else {
                control.shutdown();
            }
            drop(batch_tx);
            while request_body.next().await.is_some() {}
            let mut sent = Vec::new();
            while let Ok(StreamStat::BatchSent { offset, .. }) = rx.try_recv() {
                sent.push(offset);
            }
            sent
        }

        assert_eq!(
            drive_then_shutdown(&table_properties, batch.clone(), true).await,
            vec![7],
            "graceful close flushes the last batch"
        );
        assert_eq!(
            drive_then_shutdown(&table_properties, batch, false).await,
            Vec::<OffsetId>::new(),
            "recovery/rotation cancel drops the last batch (re-sent on next connection)"
        );
    }

    #[tokio::test]
    async fn request_body_remains_finished_after_natural_eof() {
        let table_properties = ArrowTableProperties {
            table_name: "catalog.schema.table".to_string(),
            schema: Arc::new(ArrowSchema::empty()),
        };
        let (batch_tx, batch_rx) = mpsc::channel(1);
        let (mut request_body, control) = ZerobusArrowStream::make_request_stream(
            batch_rx,
            &table_properties,
            &ArrowStreamConfigurationOptions::default(),
            None,
            #[cfg(feature = "test-hooks")]
            Arc::new(super::super::TestHooks::default()),
        )
        .unwrap();
        drop(batch_tx);

        while request_body.next().await.is_some() {}
        assert!(request_body.next().await.is_none());
        assert!(control.is_finished());
    }

    #[cfg(feature = "test-hooks")]
    #[tokio::test]
    async fn request_body_ignores_shutdown_hooks_after_natural_eof() {
        let table_properties = ArrowTableProperties {
            table_name: "catalog.schema.table".to_string(),
            schema: Arc::new(ArrowSchema::empty()),
        };
        let (batch_tx, batch_rx) = mpsc::channel(1);
        let test_hooks = Arc::new(super::super::TestHooks::default());
        let (mut request_body, control) = ZerobusArrowStream::make_request_stream(
            batch_rx,
            &table_properties,
            &ArrowStreamConfigurationOptions::default(),
            None,
            Arc::clone(&test_hooks),
        )
        .unwrap();
        drop(batch_tx);
        while request_body.next().await.is_some() {}

        let reached = Arc::new(tokio::sync::Notify::new());
        let proceed = Arc::new(tokio::sync::Notify::new());
        *test_hooks.request_body_shutdown.lock().await = Some(super::super::TestBarrier {
            reached,
            proceed: Arc::clone(&proceed),
        });
        control.shutdown();

        for _ in 0..2 {
            let next = timeout(Duration::from_millis(100), request_body.next()).await;
            proceed.notify_one();
            assert!(next
                .expect("completed request body must not enter shutdown hooks")
                .is_none());
        }
    }

    #[test]
    fn closed_eof_watch_is_finished() {
        let (eof_tx, eof_rx) = tokio::sync::watch::channel(false);
        drop(eof_tx);
        let control = RequestBodyControl {
            shutdown: tokio_util::sync::CancellationToken::new(),
            graceful: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            eof_rx,
        };

        assert!(control.is_finished());
    }

    #[tokio::test]
    async fn request_body_registry_prunes_dropped_bodies() {
        let table_properties = ArrowTableProperties {
            table_name: "catalog.schema.table".to_string(),
            schema: Arc::new(ArrowSchema::empty()),
        };
        let (_batch_tx, batch_rx) = mpsc::channel(1);
        let (request_body, control) = ZerobusArrowStream::make_request_stream(
            batch_rx,
            &table_properties,
            &ArrowStreamConfigurationOptions::default(),
            None,
            #[cfg(feature = "test-hooks")]
            Arc::new(super::super::TestHooks::default()),
        )
        .unwrap();
        let registry = RequestBodyRegistry::default();
        registry.register(&control).await;
        assert_eq!(registry.registered_count().await, 1);

        drop(request_body);
        registry.wait_for_all_eof().await;
        assert_eq!(registry.registered_count().await, 0);
    }

    #[tokio::test]
    async fn request_body_is_registered_before_do_put_completes() {
        let table_properties = ArrowTableProperties {
            table_name: "catalog.schema.table".to_string(),
            schema: Arc::new(ArrowSchema::empty()),
        };
        let registry = RequestBodyRegistry::default();
        let client = FlightClient::new(
            tonic::transport::Channel::from_static("http://127.0.0.1:1").connect_lazy(),
        );
        let options = ArrowStreamConfigurationOptions::default();

        {
            let attempt = ZerobusArrowStream::start_stream_connection(
                client,
                &table_properties,
                &options,
                &registry,
                None,
                #[cfg(feature = "test-hooks")]
                Arc::new(super::super::TestHooks::default()),
            );
            tokio::pin!(attempt);
            let _ = futures::poll!(attempt.as_mut());
            assert_eq!(
                registry.registered_count().await,
                1,
                "request body must be registered before do_put can complete"
            );
        }

        timeout(Duration::from_secs(1), registry.wait_for_all_eof())
            .await
            .expect("dropping the setup attempt must release its request body");
        assert_eq!(registry.registered_count().await, 0);
    }

    #[tokio::test]
    async fn authorization_metadata_is_sensitive() {
        let table_properties = ArrowTableProperties {
            table_name: "catalog.schema.table".to_string(),
            schema: Arc::new(ArrowSchema::empty()),
        };
        let tls_config: Arc<dyn TlsConfig> = Arc::new(PassthroughTlsConfig);
        let headers_provider: Arc<dyn HeadersProvider> = Arc::new(TestHeadersProvider);
        let client = ZerobusArrowStream::create_flight_client(
            "http://127.0.0.1:1",
            &tls_config,
            None,
            &table_properties,
            &ArrowStreamConfigurationOptions::default(),
            &headers_provider,
            "test-sdk",
        )
        .await
        .unwrap();

        let authorization = client.metadata().get("authorization").unwrap();
        assert!(authorization.is_sensitive());
        assert_eq!(authorization.to_str().unwrap(), "Bearer secret-token");

        let custom_header = client.metadata().get("x-custom-header").unwrap();
        assert!(!custom_header.is_sensitive());
        assert_eq!(custom_header.to_str().unwrap(), "custom-value");
        assert_eq!(
            client
                .metadata()
                .get("x-databricks-zerobus-table-name")
                .unwrap()
                .to_str()
                .unwrap(),
            "catalog.schema.table"
        );
    }
}
