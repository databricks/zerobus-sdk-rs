//! JSON and Protocol Buffer ingestion stream.
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
mod types;

use types::{IngestRequest, OneshotMap, RecordLandingZone};

#[cfg(feature = "testing")]
pub use callback_handler::CallbackHandlerHarness;
#[cfg(feature = "testing")]
pub(crate) use close::StreamShutdownHandle;

/// Maximum time to wait for the receiver/sender tasks to finish during stream
/// teardown.
pub(super) const STREAM_TEARDOWN_DRAIN_TIMEOUT_MS: u64 = 500;

/// Represents an active ingestion stream to a Databricks Delta table.
///
/// A `ZerobusStream` manages JSON or Protocol Buffer ingestion into
/// a Unity Catalog table over a bidirectional gRPC stream. It handles
/// authentication, automatic recovery, acknowledgment tracking, and
/// graceful shutdown.
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
    /// Kind of stream used when sending records.
    pub stream_type: StreamType,
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
    /// Persistent signal that wakes capacity waiters when the stream becomes terminal.
    terminal_token: CancellationToken,
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
        let (stream_init_result_tx, stream_init_result_rx) =
            tokio::sync::oneshot::channel::<ZerobusResult<String>>();

        let (logical_last_received_offset_id_tx, _logical_last_received_offset_id_rx) =
            tokio::sync::watch::channel(None);
        let landing_zone = Arc::new(LandingZone::<Box<IngestRequest>>::new(
            options.max_inflight_requests,
        ));

        let oneshot_map = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let is_closed = Arc::new(AtomicBool::new(false));
        let sync_mutex = Arc::new(tokio::sync::Mutex::new(()));
        let failed_records = Arc::new(RwLock::new(Vec::new()));
        let logical_offset_id_generator = OffsetIdGenerator::default();

        let (server_error_tx, server_error_rx) = tokio::sync::watch::channel(None);
        let cancellation_token = CancellationToken::new();
        let terminal_token = CancellationToken::new();
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
            Arc::clone(&landing_zone),
            Arc::clone(&oneshot_map),
            logical_last_received_offset_id_tx.clone(),
            Arc::clone(&is_closed),
            Arc::clone(&sync_mutex),
            terminal_token.clone(),
            Arc::clone(&failed_records),
            stream_init_result_tx,
            server_error_tx,
            cancellation_token.clone(),
            callback_tx.clone(),
        ));
        let stream_id = Some(stream_init_result_rx.await.map_err(|_| {
            ZerobusError::UnexpectedStreamResponseError(
                "Supervisor task died before stream creation".to_string(),
            )
        })??);

        // Cloned out before `table_properties` is moved into the struct below.
        let dynamic_message_descriptor = table_properties.message_descriptor.clone();
        let stream = Self {
            stream_type: StreamType::Ephemeral,
            headers_provider,
            options: options.clone(),
            table_properties,
            stream_id,
            landing_zone,
            oneshot_map,
            supervisor_task,
            logical_offset_id_generator,
            logical_last_received_offset_id_tx,
            _logical_last_received_offset_id_rx,
            failed_records,
            is_closed,
            sync_mutex,
            terminal_token,
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
        self.terminal_token.cancel();
        self.cancellation_token.cancel();
        self.supervisor_task.abort();
        if let Some(callback_handler_task) = self.callback_handler_task.take() {
            callback_handler_task.abort();
        }
    }
}
