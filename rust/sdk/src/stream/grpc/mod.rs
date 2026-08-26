//! Ingestion stream over the gRPC transport (proto / JSON records).
//!
//! `ZerobusStream` is the main user-facing type for streaming records into a
//! Delta table. This module owns the struct, the constructor that wires
//! together the supervisor + IO tasks, and the `Drop` impl. Behavior is
//! split across sibling files by concern; the boundary column marks which
//! pieces are transport-agnostic (candidates for a shared `StreamCore`) vs
//! gRPC-specific.
//!
//! | File                  | Concern                                            | Boundary           |
//! |-----------------------|----------------------------------------------------|--------------------|
//! | `types.rs`            | Internal types (`IngestRequest`, channel messages) | Transport-agnostic |
//! | `ingest.rs`           | Public `ingest_*` methods                          | Transport-agnostic |
//! | `acks.rs`             | `flush`, `wait_for_offset`, unacked queries        | Transport-agnostic |
//! | `close.rs`            | `close`, `is_closed`, task shutdown                | Transport-agnostic |
//! | `callback_handler.rs` | User-callback dispatch task                        | Transport-agnostic |
//! | `connection.rs`       | gRPC bidirectional stream setup                    | gRPC-specific      |
//! | `transport.rs`        | Ephemeral/persistent RPC seam (`eos`)              | gRPC-specific      |
//! | `sender.rs`           | Outbound gRPC sender task                          | gRPC-specific      |
//! | `receiver.rs`         | Inbound gRPC receiver task                         | gRPC-specific      |
//! | `supervisor.rs`       | Create → spawn → recover loop                      | gRPC-specific      |

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::instrument;

use crate::databricks::zerobus::zerobus_client::ZerobusClient;
use crate::landing_zone::LandingZone;
use crate::{
    DynamicRecord, HeadersProvider, MessageDescriptor, OffsetId, OffsetIdGenerator,
    StreamConfigurationOptions, StreamType, TableProperties, ZerobusError, ZerobusResult,
};

mod acks;
mod callback_handler;
mod close;
mod connection;
mod ingest;
mod receiver;
mod sender;
mod supervisor;
mod transport;
mod types;

use transport::GrpcConnectionMode;
use types::{IngestRequest, OneshotMap, RecordLandingZone};

#[cfg(feature = "testing")]
pub use callback_handler::CallbackHandlerHarness;

/// Maximum time to wait for the receiver/sender tasks to finish during stream
/// teardown.
pub(super) const STREAM_TEARDOWN_DRAIN_TIMEOUT_MS: u64 = 500;

/// Represents an active ingestion stream to a Databricks Delta table.
///
/// A `ZerobusStream` manages a bidirectional gRPC stream for ingesting records into
/// a Unity Catalog table. It handles authentication, automatic recovery, acknowledgment
/// tracking, and graceful shutdown.
///
/// # Lifecycle
///
/// 1. Create a stream via `ZerobusSdk::stream_builder()`
/// 2. Ingest records in a loop with `ingest_record_offset()`
/// 3. Call `flush()` to confirm all queued records are acknowledged
/// 4. Close the stream with `close()` to release resources
///
/// # Examples
///
/// ```no_run
/// # use databricks_zerobus_ingest_sdk::*;
/// # async fn example(mut stream: ZerobusStream, records: Vec<Vec<u8>>) -> Result<(), ZerobusError> {
/// // Ingest records in a loop (queue only)
/// for data in records {
///     stream.ingest_record_offset(data).await?;
/// }
///
/// // Confirm all queued records at once
/// stream.flush().await?;
///
/// // Close the stream gracefully
/// stream.close().await?;
/// # Ok(())
/// # }
/// ```
#[non_exhaustive]
pub struct ZerobusStream {
    /// This is a 128-bit UUID that is unique across all streams in the system,
    /// not just within a single table. The server returns this ID in the CreateStreamResponse
    /// after validating the table properties and establishing the gRPC connection.
    pub(crate) stream_id: Option<String>,
    /// Type of gRPC stream that is used when sending records.
    pub stream_type: StreamType,
    /// For a persistent (Eos) stream resumed from a prior session, the offset
    /// the server had durably committed at resume time (`last_committed_offset`
    /// from the resume response). `None` for ephemeral streams and for a
    /// freshly created persistent stream (nothing committed yet). Only read
    /// through the `eos`-gated accessor.
    pub(crate) last_committed_offset: Option<OffsetId>,
    /// Gets headers which are used in the first request to establish connection with the server.
    pub headers_provider: Arc<dyn HeadersProvider>,
    /// The stream configuration options related to recovery, fetching OAuth tokens, etc.
    pub options: StreamConfigurationOptions,
    /// The table properties - table name and descriptor of the table.
    pub(crate) table_properties: TableProperties,
    // The fields below have no visibility modifier: they are private to this
    // module, but accessible from descendant modules (the sibling files under
    // `stream/grpc/`) per Rust's visibility rules. That is what we want — no
    // external crate or other module should reach into stream internals.
    /// Logical landing zone that stores records the user has submitted but the sender task has not yet placed on the wire.
    landing_zone: RecordLandingZone,
    /// Map of logical offset to oneshot sender.
    oneshot_map: Arc<tokio::sync::Mutex<OneshotMap>>,
    /// Supervisor task that manages the stream lifecycle such as stream creation, recovery, etc.
    /// It orchestrates the receiver and sender tasks.
    supervisor_task: tokio::task::JoinHandle<Result<(), ZerobusError>>,
    /// The generator of logical offset IDs. Used to generate monotonically increasing offset IDs, even if the stream recovers.
    logical_offset_id_generator: OffsetIdGenerator,
    /// Signal that the stream is caught up to the given offset.
    logical_last_received_offset_id_tx: tokio::sync::watch::Sender<Option<OffsetId>>,
    /// Persistent offset ID receiver to ensure at least one receiver exists, preventing SendError
    _logical_last_received_offset_id_rx: tokio::sync::watch::Receiver<Option<OffsetId>>,
    /// A vector of records that have failed to be acknowledged.
    failed_records: Arc<RwLock<Vec<crate::EncodedBatch>>>,
    /// Flag indicating if the stream has been closed.
    is_closed: Arc<AtomicBool>,
    /// Sync mutex to ensure that offset generation and record ingestion happen atomically.
    sync_mutex: Arc<tokio::sync::Mutex<()>>,
    /// Watch channel for last error received from the server.
    server_error_rx: tokio::sync::watch::Receiver<Option<ZerobusError>>,
    /// Cancellation token to signal receiver and sender tasks to abort. It is sent either when stream is closed or dropped.
    cancellation_token: CancellationToken,
    /// Callback handler task that executes callbacks in a separate thread.
    callback_handler_task: Option<tokio::task::JoinHandle<()>>,
    /// Resolved message descriptor for building dynamic-proto records, supplied by
    /// the builder. `None` for JSON and compiled-proto streams.
    dynamic_message_descriptor: Option<MessageDescriptor>,
}

impl ZerobusStream {
    /// Creates a new ephemeral stream for ingesting records.
    #[instrument(level = "debug", skip_all)]
    pub(crate) async fn new_stream(
        channel: ZerobusClient<Channel>,
        table_properties: TableProperties,
        headers_provider: Arc<dyn HeadersProvider>,
        options: StreamConfigurationOptions,
    ) -> ZerobusResult<Self> {
        Self::new_with_kind(
            channel,
            table_properties,
            headers_provider,
            options,
            StreamType::Ephemeral,
            GrpcConnectionMode::Ephemeral,
        )
        .await
    }

    /// Creates or resumes a persistent (Eos) stream.
    ///
    /// With `resume_stream_id = None` the server mints a new stream and its id
    /// is available via [`stream_id`](Self::stream_id). With `Some(id)` the SDK
    /// reconnects to an existing persistent stream, reseeds its offset generator
    /// to continue after the server's committed offset, and re-sends only the
    /// records the server has not yet durably stored.
    #[instrument(level = "debug", skip_all)]
    pub(crate) async fn new_persistent_stream(
        channel: ZerobusClient<Channel>,
        table_properties: TableProperties,
        headers_provider: Arc<dyn HeadersProvider>,
        options: StreamConfigurationOptions,
        resume_stream_id: Option<String>,
    ) -> ZerobusResult<Self> {
        Self::new_with_kind(
            channel,
            table_properties,
            headers_provider,
            options,
            StreamType::Persistent,
            GrpcConnectionMode::Persistent { resume_stream_id },
        )
        .await
    }

    /// Shared constructor for both stream kinds. Wires the supervisor + IO
    /// tasks, waits for the first open to complete, and — for a persistent
    /// resume — reseeds the logical offset generator so records ingested after
    /// resume continue past the server's committed offset.
    async fn new_with_kind(
        channel: ZerobusClient<Channel>,
        table_properties: TableProperties,
        headers_provider: Arc<dyn HeadersProvider>,
        options: StreamConfigurationOptions,
        stream_type: StreamType,
        kind: GrpcConnectionMode,
    ) -> ZerobusResult<Self> {
        let (stream_init_result_tx, stream_init_result_rx) =
            tokio::sync::oneshot::channel::<ZerobusResult<supervisor::StreamInitInfo>>();

        let (logical_last_received_offset_id_tx, _logical_last_received_offset_id_rx) =
            tokio::sync::watch::channel(None);
        let landing_zone = Arc::new(LandingZone::<Box<IngestRequest>>::new(
            options.max_inflight_requests,
        ));

        let oneshot_map = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let is_closed = Arc::new(AtomicBool::new(false));
        let failed_records = Arc::new(RwLock::new(Vec::new()));
        let logical_offset_id_generator = OffsetIdGenerator::default();

        let (server_error_tx, server_error_rx) = tokio::sync::watch::channel(None);
        let cancellation_token = CancellationToken::new();
        // Create callback channel and spawn callback handler task only if callback is defined
        let (callback_tx, callback_handler_task) = if options.ack_callback.is_some() {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let task = Self::spawn_callback_handler_task(
                rx,
                options.ack_callback.clone(),
                cancellation_token.clone(),
            );
            (Some(tx), Some(task))
        } else {
            (None, None)
        };

        let supervisor_task = tokio::task::spawn(Self::supervisor_task(
            channel,
            table_properties.clone(),
            Arc::clone(&headers_provider),
            options.clone(),
            kind,
            Arc::clone(&landing_zone),
            Arc::clone(&oneshot_map),
            logical_last_received_offset_id_tx.clone(),
            Arc::clone(&is_closed),
            Arc::clone(&failed_records),
            stream_init_result_tx,
            server_error_tx,
            cancellation_token.clone(),
            callback_tx.clone(),
        ));
        let init_info = stream_init_result_rx.await.map_err(|_| {
            ZerobusError::UnexpectedStreamResponseError(
                "Supervisor task died before stream creation".to_string(),
            )
        })??;

        // On a persistent resume, continue offset generation past the server's
        // committed offset so newly ingested records get the next durable
        // offsets rather than restarting at 0. Also seed the last-received-offset
        // watch to the watermark: everything up to it is already durable, so a
        // `flush()` / `wait_for_offset()` targeting an already-committed offset
        // must resolve immediately. Without this the watch stays `None` and, on a
        // fresh-process resume with no new ingests, `flush()` (and the `close()`
        // that calls it) would block until the flush timeout and then error,
        // since the server never re-acks offsets it committed in a prior session.
        // Safe because the constructor returns before any user ingest, so no real
        // ack can race this initial value.
        if let Some(watermark) = init_info.last_committed_offset {
            let next_offset = watermark.checked_add(1).ok_or_else(|| {
                ZerobusError::UnexpectedStreamResponseError(
                    "Persistent stream offset space is exhausted".to_string(),
                )
            })?;
            logical_offset_id_generator.set_next(next_offset);
            let _ = logical_last_received_offset_id_tx.send(Some(watermark));
        }

        // Cloned out before `table_properties` is moved into the struct below.
        let dynamic_message_descriptor = table_properties.message_descriptor.clone();
        let stream = Self {
            stream_type,
            headers_provider,
            options: options.clone(),
            table_properties,
            stream_id: Some(init_info.stream_id),
            last_committed_offset: init_info.last_committed_offset,
            landing_zone,
            oneshot_map,
            supervisor_task,
            logical_offset_id_generator,
            logical_last_received_offset_id_tx,
            _logical_last_received_offset_id_rx,
            failed_records,
            is_closed,
            sync_mutex: Arc::new(tokio::sync::Mutex::new(())),
            server_error_rx,
            cancellation_token,
            callback_handler_task,
            dynamic_message_descriptor,
        };

        Ok(stream)
    }

    /// The [`MessageDescriptor`] for this stream's schema, for building records
    /// with [`crate::DynamicRecord`]. Supplied at build time; the
    /// returned clone is cheap (Arc-backed).
    ///
    /// # Errors
    ///
    /// [`ZerobusError::InvalidArgument`] if this isn't a dynamic-proto stream
    /// (i.e. not built with [`dynamic_proto`](crate::StreamBuilder::dynamic_proto)).
    pub fn message_descriptor(&self) -> ZerobusResult<MessageDescriptor> {
        self.dynamic_message_descriptor.clone().ok_or_else(|| {
            ZerobusError::InvalidArgument(
                "stream was not built with .dynamic_proto(); no message descriptor available"
                    .into(),
            )
        })
    }

    /// Create an empty [`DynamicRecord`] bound to this stream's schema.
    ///
    /// Convenience over [`message_descriptor`](Self::message_descriptor); same errors.
    pub fn new_record(&self) -> ZerobusResult<DynamicRecord> {
        Ok(DynamicRecord::new(self.message_descriptor()?))
    }
}

impl Drop for ZerobusStream {
    fn drop(&mut self) {
        self.is_closed.store(true, Ordering::Relaxed);
        self.cancellation_token.cancel();
        self.supervisor_task.abort();
        if let Some(callback_handler_task) = self.callback_handler_task.take() {
            callback_handler_task.abort();
        }
    }
}
