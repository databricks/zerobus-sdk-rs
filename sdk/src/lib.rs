pub mod databricks {
    pub mod zerobus {
        include!(concat!(env!("OUT_DIR"), "/databricks.zerobus.rs"));
    }
}
use databricks::zerobus as proto_zerobus;

pub use default_token_factory::DefaultTokenFactory;
pub use errors::ZerobusError;
pub use headers_provider::HeadersProvider;
use headers_provider::OAuthHeadersProvider;
use landing_zone::LandingZone;
pub use offset_generator::{OffsetId, OffsetIdGenerator};
pub use stream_configuration::StreamConfigurationOptions;

mod default_token_factory;
mod errors;
mod headers_provider;
mod landing_zone;
mod offset_generator;
mod stream_configuration;

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use prost::Message;
use proto_zerobus::ephemeral_stream_request::Payload as RequestPayload;
use proto_zerobus::ephemeral_stream_response::Payload as ResponsePayload;
use proto_zerobus::ingest_record_request::Record;
use proto_zerobus::zerobus_client::ZerobusClient;
use proto_zerobus::{
    CloseStreamSignal, CreateIngestStreamRequest, EphemeralStreamRequest, EphemeralStreamResponse,
    IngestRecordRequest, IngestRecordResponse, RecordType,
};
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};
use tokio_retry::strategy::FixedInterval;
use tokio_retry::RetryIf;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig};
use tracing::{debug, error, info, instrument, span, warn, Level};

/// The type of the stream connection created with the server.
/// Currently we only support ephemeral streams on the server side, so we support only that in the SDK as well.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamType {
    /// Ephemeral streams exist only for the duration of the connection.
    /// They are not persisted and are not recoverable.
    Ephemeral,
    /// UNSUPPORTED: Persistent streams are durable and recoverable.
    Persistent,
}

/// The properties of the table to ingest to.
///
/// Used when creating streams via `ZerobusSdk::create_stream()` to specify
/// which table to write to and the schema of records being ingested.
///
/// # Common errors:
/// -`InvalidTableName`: table_name contains invalid characters or doesn't exist
/// -`PermissionDenied`: insufficient permissions to write to the specified table
/// -`InvalidArgument`: invalid or missing descriptor_proto or auth token
#[derive(Debug, Clone)]
pub struct TableProperties {
    pub table_name: String,
    pub descriptor_proto: Option<prost_types::DescriptorProto>,
}

pub type ZerobusResult<T> = Result<T, ZerobusError>;

/// A type alias for a protobuf-encoded record.
pub type ProtoEncodedRecord = Vec<u8>;
/// A type alias for a JSON-encoded record.
pub type JsonEncodedRecord = String;

impl From<Vec<u8>> for RecordPayload {
    fn from(v: Vec<u8>) -> Self {
        RecordPayload::Proto(v)
    }
}

impl From<String> for RecordPayload {
    fn from(s: String) -> Self {
        RecordPayload::Json(s)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RecordPayload {
    Json(JsonEncodedRecord),
    Proto(ProtoEncodedRecord),
}

pub trait IntoRecord {
    fn into_record(self) -> Record;
}

impl IntoRecord for RecordPayload {
    fn into_record(self) -> Record {
        match self {
            RecordPayload::Json(json) => Record::JsonRecord(json),
            RecordPayload::Proto(proto) => Record::ProtoEncodedRecord(proto),
        }
    }
}

/// Logical representation of a record to be ingested.
/// Contains the payload and the offset on which the record was sent.
#[derive(Debug, Clone)]
struct IngestRecord<RecordPayload> {
    payload: RecordPayload,
    offset_id: OffsetId,
}

/// Map of logical offset to oneshot sender used to send acknowledgments back to the client.
type OneshotMap = HashMap<OffsetId, tokio::sync::oneshot::Sender<ZerobusResult<OffsetId>>>;
/// Landing zone for ingest records.
type RecordLandingZone<RecordPayload> = Arc<LandingZone<Box<IngestRecord<RecordPayload>>>>;

/// Represents an active ingestion stream to a Databricks Delta table.
///
/// A `ZerobusStream` manages a bidirectional gRPC stream for ingesting records into
/// a Unity Catalog table. It handles authentication, automatic recovery, acknowledgment
/// tracking, and graceful shutdown.
///
/// # Lifecycle
///
/// 1. Create a stream via `ZerobusSdk::create_stream()`
/// 2. Ingest records with `ingest_record()` and await acknowledgments
/// 3. Optionally call `flush()` to ensure all records are persisted
/// 4. Close the stream with `close()` to release resources
///
/// # Examples
///
/// ```no_run
/// # use databricks_zerobus_ingest_sdk::*;
/// # async fn example(mut stream: ZerobusStream, data: Vec<u8>) -> Result<(), ZerobusError> {
/// // Ingest a single record
/// let ack = stream.ingest_record(data).await?;
/// let offset = ack.await?;
/// println!("Record acknowledged at offset: {}", offset);
///
/// // Close the stream gracefully
/// stream.close().await?;
/// # Ok(())
/// # }
/// ```
pub struct ZerobusStream {
    /// This is a 128-bit UUID that is unique across all streams in the system,
    /// not just within a single table. The server returns this ID in the CreateStreamResponse
    /// after validating the table properties and establishing the gRPC connection.
    stream_id: Option<String>,
    /// Type of gRPC stream that is used when sending records.
    pub stream_type: StreamType,
    /// Gets headers which are used in the first request to establish connection with the server.
    pub headers_provider: Arc<dyn HeadersProvider>,
    /// The stream configuration options related to recovery, fetching OAuth tokens, etc.
    pub options: StreamConfigurationOptions,
    /// The table properties - table name and descriptor of the table.
    pub table_properties: TableProperties,
    /// Logical landing zone that is used to store records that have been sent by user but not yet sent over the network.
    landing_zone: RecordLandingZone<RecordPayload>,
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
    failed_records: Arc<RwLock<Vec<RecordPayload>>>,
    /// Flag indicating if the stream has been closed.
    is_closed: Arc<AtomicBool>,
    /// Sync mutex to ensure that offset generation and record ingestion happen atomically.
    sync_mutex: Arc<tokio::sync::Mutex<()>>,
}

/// The main interface for interacting with the Zerobus API.
/// # Examples
/// ```no_run
/// # use std::error::Error;
/// # use std::sync::Arc;
/// # use databricks_zerobus_ingest_sdk::{ZerobusSdk, StreamConfigurationOptions, TableProperties, ZerobusError, ZerobusResult};
/// #
/// # async fn write_single_row(row: impl prost::Message) -> Result<(), ZerobusError> {
///
/// // Open SDK with the Zerobus API endpoint.
/// let sdk = ZerobusSdk::new("https://your-workspace.zerobus.region.cloud.databricks.com".to_string(),"https://your-workspace.cloud.databricks.com".to_string())?;
///
/// // Define the arguments for the ephemeral stream.
/// let table_properties = TableProperties {
///     table_name: "test_table".to_string(),
///     descriptor_proto: Default::default(),
/// };
/// let options = StreamConfigurationOptions {
///     max_inflight_records: 100,
///     ..Default::default()
/// };
/// let client_id = "your-client-id".to_string();
/// let client_secret = "your-client-secret".to_string();
///
/// // Create a stream
/// let stream = sdk.create_stream(table_properties, client_id, client_secret, Some(options)).await?;
///
/// // Ingest a single record and await its acknowledgment
/// let ack_future = stream.ingest_record(row.encode_to_vec()).await?;
///
/// // At this point we know that the record has been sent to the server.
/// // Let's block on the acknowledgment.
/// let offset_id = ack_future.await?;
/// println!("Record acknowledged with offset Id: {}", offset_id);
/// # Ok(())
/// # }
/// ```
pub struct ZerobusSdk {
    pub zerobus_endpoint: String,
    pub use_tls: bool,
    pub unity_catalog_url: String,
    workspace_id: String,
}

impl ZerobusSdk {
    /// Creates a new Zerobus SDK instance.
    ///
    /// This initializes the SDK with the required endpoints. The workspace ID is automatically
    /// extracted from the Zerobus endpoint URL.
    ///
    /// # Arguments
    ///
    /// * `zerobus_endpoint` - The Zerobus API endpoint URL (e.g., "https://workspace-id.cloud.databricks.com")
    /// * `unity_catalog_url` - The Unity Catalog endpoint URL (e.g., "https://workspace.cloud.databricks.com")
    ///
    /// # Returns
    ///
    /// A new `ZerobusSdk` instance configured to use TLS.
    ///
    /// # Errors
    ///
    /// * `ChannelCreationError` - If the workspace ID cannot be extracted from the Zerobus endpoint
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// let sdk = ZerobusSdk::new(
    ///     "https://<your-zerobus-endpoint>".to_string(),
    ///     "https://<your-uc-endpoint>".to_string(),
    /// )?;
    /// # Ok::<(), ZerobusError>(())
    /// ```
    #[allow(clippy::result_large_err)]
    pub fn new(zerobus_endpoint: String, unity_catalog_url: String) -> ZerobusResult<Self> {
        let workspace_id = zerobus_endpoint
            .strip_prefix("https://")
            .or_else(|| zerobus_endpoint.strip_prefix("http://"))
            .and_then(|s| s.split('.').next())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                ZerobusError::ChannelCreationError(
                    "Failed to extract workspace_id from zerobus_endpoint".to_string(),
                )
            })?;

        Ok(ZerobusSdk {
            zerobus_endpoint,
            use_tls: true,
            unity_catalog_url,
            workspace_id,
        })
    }

    /// Creates a new ingestion stream to a Unity Catalog table.
    ///
    /// This establishes a bidirectional gRPC stream for ingesting records. Authentication
    /// is handled automatically using the provided OAuth credentials.
    ///
    /// # Arguments
    ///
    /// * `table_properties` - Table name and protobuf descriptor
    /// * `client_id` - OAuth client ID for authentication
    /// * `client_secret` - OAuth client secret for authentication
    /// * `options` - Optional stream configuration (uses defaults if `None`)
    ///
    /// # Returns
    ///
    /// A `ZerobusStream` ready for ingesting records.
    ///
    /// # Errors
    ///
    /// * `CreateStreamError` - If stream creation fails
    /// * `InvalidTableName` - If the table name is invalid or table doesn't exist
    /// * `InvalidUCTokenError` - If OAuth authentication fails
    /// * `PermissionDenied` - If credentials lack required permissions
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # async fn example(sdk: ZerobusSdk) -> Result<(), ZerobusError> {
    /// let table_props = TableProperties {
    ///     table_name: "catalog.schema.table".to_string(),
    ///     descriptor_proto: Default::default(), // Load from generated files
    /// };
    ///
    /// let stream = sdk.create_stream(
    ///     table_props,
    ///     "client-id".to_string(),
    ///     "client-secret".to_string(),
    ///     None,
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(level = "debug", skip_all)]
    pub async fn create_stream(
        &self,
        table_properties: TableProperties,
        client_id: String,
        client_secret: String,
        options: Option<StreamConfigurationOptions>,
    ) -> ZerobusResult<ZerobusStream> {
        let headers_provider = OAuthHeadersProvider::new(
            client_id,
            client_secret,
            table_properties.table_name.clone(),
            self.workspace_id.clone(),
            self.unity_catalog_url.clone(),
        );
        self.create_stream_with_headers_provider(
            table_properties,
            Arc::new(headers_provider),
            options,
        )
        .await
    }

    /// Creates a new ingestion stream with a custom headers provider.
    ///
    /// This is an advanced method that allows you to implement your own authentication
    /// logic by providing a custom implementation of the `HeadersProvider` trait.
    ///
    /// # Arguments
    ///
    /// * `table_properties` - Table name and protobuf descriptor
    /// * `headers_provider` - An `Arc` holding your custom `HeadersProvider` implementation
    /// * `options` - Optional stream configuration (uses defaults if `None`)
    ///
    /// # Returns
    ///
    /// A `ZerobusStream` ready for ingesting records.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # use std::collections::HashMap;
    /// # use std::sync::Arc;
    /// # use async_trait::async_trait;
    /// #
    /// # struct MyHeadersProvider;
    /// #
    /// # #[async_trait]
    /// # impl HeadersProvider for MyHeadersProvider {
    /// #     async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
    /// #         let mut headers = HashMap::new();
    /// #         headers.insert("some_key", "some_value".to_string());
    /// #         Ok(headers)
    /// #     }
    /// # }
    /// #
    /// # async fn example(sdk: ZerobusSdk) -> Result<(), ZerobusError> {
    /// let table_props = TableProperties {
    ///     table_name: "catalog.schema.table".to_string(),
    ///     descriptor_proto: Default::default(),
    /// };
    ///
    /// let headers_provider = Arc::new(MyHeadersProvider);
    ///
    /// let stream = sdk.create_stream_with_headers_provider(
    ///     table_props,
    ///     headers_provider,
    ///     None,
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(level = "debug", skip_all)]
    pub async fn create_stream_with_headers_provider(
        &self,
        table_properties: TableProperties,
        headers_provider: Arc<dyn HeadersProvider>,
        options: Option<StreamConfigurationOptions>,
    ) -> ZerobusResult<ZerobusStream> {
        let options = options.unwrap_or_default();

        match options.record_type {
            RecordType::Proto => {
                if table_properties.descriptor_proto.is_none() {
                    return Err(ZerobusError::InvalidArgument(
                        "Proto descriptor is required for Proto record type".to_string(),
                    ));
                }
            }
            RecordType::Json => {
                if table_properties.descriptor_proto.is_some() {
                    warn!("JSON descriptor is not supported for Proto record type");
                }
            }
            RecordType::Unspecified => {
                return Err(ZerobusError::InvalidArgument(
                    "Record type is not specified".to_string(),
                ));
            }
        }

        // TODO: For now we are opening a new channel for each stream.
        // In the future we should consider reusing the channel.
        let channel = if self.use_tls {
            self.create_secure_channel_zerobus_client().await?
        } else {
            let endpoint = Channel::from_shared(self.zerobus_endpoint.clone())
                .map_err(|err| ZerobusError::ChannelCreationError(err.to_string()))?;
            ZerobusClient::new(endpoint.connect_lazy())
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX)
        };
        let stream = ZerobusStream::new_stream(
            channel,
            table_properties,
            Arc::clone(&headers_provider),
            options,
        )
        .await;
        match stream {
            Ok(stream) => {
                if let Some(stream_id) = stream.stream_id.as_ref() {
                    info!(stream_id = %stream_id, "Successfully created new ephemeral stream");
                } else {
                    error!("Successfully created a stream but stream_id is None");
                }
                return Ok(stream);
            }
            Err(e) => {
                error!("Stream initialization failed with error: {}", e);
                return Err(e);
            }
        }
    }

    /// Recreates a failed stream and re-ingests unacknowledged records.
    ///
    /// This is useful when a stream encounters an error and you want to preserve
    /// unacknowledged records. The method creates a new stream with the same
    /// configuration and automatically re-ingests all records that weren't acknowledged.
    ///
    /// # Arguments
    ///
    /// * `stream` - The failed stream to recreate
    ///
    /// # Returns
    ///
    /// A new `ZerobusStream` with unacknowledged records already submitted.
    ///
    /// # Errors
    ///
    /// Returns any errors from stream creation or re-ingestion.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # async fn example(sdk: ZerobusSdk, mut stream: ZerobusStream) -> Result<(), ZerobusError> {
    /// match stream.close().await {
    ///     Err(_) => {
    ///         // Stream failed, recreate it
    ///         let new_stream = sdk.recreate_stream(stream).await?;
    ///         // Continue using new_stream
    ///     }
    ///     Ok(_) => println!("Stream closed successfully"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(level = "debug", skip_all)]
    pub async fn recreate_stream(&self, stream: ZerobusStream) -> ZerobusResult<ZerobusStream> {
        let records = stream.get_unacked_records().await?;
        let new_stream = self
            .create_stream_with_headers_provider(
                stream.table_properties,
                Arc::clone(&stream.headers_provider),
                Some(stream.options),
            )
            .await?;
        for record in records {
            let ack = new_stream.ingest_record(record).await?;
            tokio::spawn(ack);
        }
        return Ok(new_stream);
    }

    async fn create_secure_channel_zerobus_client(&self) -> ZerobusResult<ZerobusClient<Channel>> {
        // Use native OS certificate store (works on Windows, macOS, and Linux)
        let tls_config = ClientTlsConfig::new().with_native_roots();

        let channel = Channel::from_shared(self.zerobus_endpoint.clone())
            .map_err(|_| ZerobusError::InvalidZerobusEndpointError(self.zerobus_endpoint.clone()))?
            .tls_config(tls_config)
            .map_err(|_| ZerobusError::FailedToEstablishTlsConnectionError)?
            .connect_lazy();

        // Set unlimited message sizes (equivalent to -1 in Python gRPC)
        let client = ZerobusClient::new(channel)
            .max_decoding_message_size(usize::MAX) // Max receive message length
            .max_encoding_message_size(usize::MAX); // Max send message length

        Ok(client)
    }
}

impl ZerobusStream {
    /// Creates a new ephemeral stream for ingesting records.
    #[instrument(level = "debug", skip_all)]
    async fn new_stream(
        channel: ZerobusClient<Channel>,
        table_properties: TableProperties,
        headers_provider: Arc<dyn HeadersProvider>,
        options: StreamConfigurationOptions,
    ) -> ZerobusResult<Self> {
        let (stream_init_result_tx, stream_init_result_rx) =
            tokio::sync::oneshot::channel::<ZerobusResult<String>>();

        let (logical_last_received_offset_id_tx, _logical_last_received_offset_id_rx) =
            tokio::sync::watch::channel(None);
        let landing_zone = Arc::new(LandingZone::<Box<IngestRecord<RecordPayload>>>::new(
            options.max_inflight_records,
        ));

        let oneshot_map = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let is_closed = Arc::new(AtomicBool::new(false));
        let failed_records = Arc::new(RwLock::new(Vec::new()));
        let logical_offset_id_generator = OffsetIdGenerator::default();
        let supervisor_task = tokio::task::spawn(Self::supervisor_task(
            channel,
            table_properties.clone(),
            Arc::clone(&headers_provider),
            options.clone(),
            Arc::clone(&landing_zone),
            Arc::clone(&oneshot_map),
            logical_last_received_offset_id_tx.clone(),
            Arc::clone(&is_closed),
            Arc::clone(&failed_records),
            stream_init_result_tx,
        ));
        let stream_id = Some(stream_init_result_rx.await.map_err(|_| {
            ZerobusError::UnexpectedStreamResponseError(
                "Supervisor task died before stream creation".to_string(),
            )
        })??);

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
            sync_mutex: Arc::new(tokio::sync::Mutex::new(())),
        };

        Ok(stream)
    }

    /// Supervisor task is responsible for managing the stream lifecycle.
    /// It handles stream creation, recovery, and error handling.
    #[allow(clippy::too_many_arguments)]
    #[instrument(level = "debug", skip_all, fields(table_name = %table_properties.table_name))]
    async fn supervisor_task(
        channel: ZerobusClient<Channel>,
        table_properties: TableProperties,
        headers_provider: Arc<dyn HeadersProvider>,
        options: StreamConfigurationOptions,
        landing_zone: RecordLandingZone<RecordPayload>,
        oneshot_map: Arc<tokio::sync::Mutex<OneshotMap>>,
        logical_last_received_offset_id_tx: tokio::sync::watch::Sender<Option<OffsetId>>,
        is_closed: Arc<AtomicBool>,
        failed_records: Arc<RwLock<Vec<RecordPayload>>>,
        stream_init_result_tx: tokio::sync::oneshot::Sender<ZerobusResult<String>>,
    ) -> ZerobusResult<()> {
        let mut initial_stream_creation = true;
        let mut stream_init_result_tx = Some(stream_init_result_tx);

        loop {
            debug!("Supervisor task loop");
            if is_closed.load(Ordering::Relaxed) {
                return Ok(());
            }

            let landing_zone_sender = Arc::clone(&landing_zone);
            let landing_zone_receiver = Arc::clone(&landing_zone);
            let landing_zone_recovery = Arc::clone(&landing_zone);

            // 1. Create a stream.
            let strategy = FixedInterval::from_millis(options.recovery_backoff_ms)
                .take(options.recovery_retries as usize);

            let create_attempt = || {
                let channel = channel.clone();
                let table_properties = table_properties.clone();
                let headers_provider = Arc::clone(&headers_provider);
                let record_type = options.record_type;

                async move {
                    tokio::time::timeout(
                        Duration::from_millis(options.recovery_timeout_ms),
                        Self::create_stream_connection(
                            channel,
                            &table_properties,
                            &headers_provider,
                            record_type,
                        ),
                    )
                    .await
                    .map_err(|_| {
                        ZerobusError::CreateStreamError(tonic::Status::deadline_exceeded(
                            "Stream creation timed out",
                        ))
                    })?
                }
            };
            let should_retry = |e: &ZerobusError| options.recovery && e.is_retryable();
            let creation = RetryIf::spawn(strategy, create_attempt, should_retry).await;

            let (tx, response_grpc_stream, stream_id) = match creation {
                Ok((tx, response_grpc_stream, stream_id)) => (tx, response_grpc_stream, stream_id),
                Err(e) => {
                    if initial_stream_creation {
                        if let Some(tx) = stream_init_result_tx.take() {
                            let _ = tx.send(Err(e.clone()));
                        }
                    } else {
                        is_closed.store(true, Ordering::Relaxed);
                        Self::fail_all_pending_records(
                            landing_zone.clone(),
                            oneshot_map.clone(),
                            failed_records.clone(),
                            &e,
                        )
                        .await;
                    }
                    return Err(e);
                }
            };
            if initial_stream_creation {
                if let Some(stream_init_result_tx_inner) = stream_init_result_tx.take() {
                    let _ = stream_init_result_tx_inner.send(Ok(stream_id.clone()));
                }
                initial_stream_creation = false;
            }
            info!(stream_id = %stream_id, "Successfully created stream");

            // 2. Reset landing zone.
            landing_zone_recovery.reset_observe();

            // 3. Spawn receiver and sender task.
            let mut recv_task = Self::spawn_receiver_task(
                response_grpc_stream,
                logical_last_received_offset_id_tx.clone(),
                options.server_lack_of_ack_timeout_ms,
                landing_zone_receiver,
                oneshot_map.clone(),
                options.recovery,
            );
            let mut send_task = Self::spawn_sender_task(tx, landing_zone_sender);

            // 4. Wait for any of the two tasks to end.
            let result = tokio::select! {
                recv_result = &mut recv_task => {
                    send_task.abort();
                    match recv_result {
                        Ok(Err(e)) => Err(e),
                        Err(e) => Err(ZerobusError::UnexpectedStreamResponseError(
                            format!("Receiver task panicked: {}", e)
                        )),
                        Ok(Ok(())) => Ok(()),
                    }
                }
                send_result = &mut send_task => {
                    recv_task.abort();
                    match send_result {
                        Ok(Err(e)) => Err(e),
                        Err(e) => Err(ZerobusError::UnexpectedStreamResponseError(
                            format!("Sender task panicked: {}", e)
                        )),
                        Ok(Ok(())) => unreachable!("Sender task should never complete successfully"),
                    }
                }
            };

            // 5. Handle errors.
            if result.is_err() {
                let error = result.err().unwrap();
                error!(stream_id = %stream_id, "Stream failure detected: {}", error);
                let error = match &error {
                    // Mapping this to pass certain e2e tests.
                    // TODO: Remove this once we fix tests.
                    ZerobusError::StreamClosedError(status)
                        if status.code() == tonic::Code::InvalidArgument =>
                    {
                        ZerobusError::InvalidArgument(status.message().to_string())
                    }
                    _ => error,
                };
                if !error.is_retryable() || !options.recovery {
                    is_closed.store(true, Ordering::Relaxed);
                    Self::fail_all_pending_records(
                        landing_zone.clone(),
                        oneshot_map.clone(),
                        failed_records.clone(),
                        &error,
                    )
                    .await;
                    return Err(error);
                }
            }
        }
    }

    /// Creates a stream connection to the Zerobus API.
    /// Returns a tuple containing the sender, response gRPC stream, and stream ID.
    /// If the stream creation fails, it returns an error.
    #[instrument(level = "debug", skip_all, fields(table_name = %table_properties.table_name))]
    async fn create_stream_connection(
        mut channel: ZerobusClient<Channel>,
        table_properties: &TableProperties,
        headers_provider: &Arc<dyn HeadersProvider>,
        record_type: RecordType,
    ) -> ZerobusResult<(
        tokio::sync::mpsc::Sender<EphemeralStreamRequest>,
        tonic::Streaming<EphemeralStreamResponse>,
        String,
    )> {
        const CHANNEL_BUFFER_SIZE: usize = 2048;
        let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_BUFFER_SIZE);
        let mut request_stream = tonic::Request::new(ReceiverStream::new(rx));

        let stream_metadata = request_stream.metadata_mut();
        let headers = headers_provider.get_headers().await?;

        for (key, value) in headers {
            match key {
                "x-databricks-zerobus-table-name" => {
                    let table_name = MetadataValue::try_from(value.as_str())
                        .map_err(|_| ZerobusError::InvalidTableName(key.to_string()))?;
                    stream_metadata.insert("x-databricks-zerobus-table-name", table_name);
                }
                "authorization" => {
                    let mut auth_value = MetadataValue::try_from(value.as_str()).map_err(|_| {
                        error!(table_name = %table_properties.table_name, "Invalid token: {}", value);
                        ZerobusError::InvalidUCTokenError(value)
                    })?;
                    auth_value.set_sensitive(true);
                    stream_metadata.insert("authorization", auth_value);
                }
                other_key => {
                    let header_value = MetadataValue::try_from(value.as_str())
                        .map_err(|_| ZerobusError::InvalidArgument(other_key.to_string()))?;
                    stream_metadata.insert(other_key, header_value);
                }
            }
        }

        let mut response_grpc_stream = channel
            .ephemeral_stream(request_stream)
            .await
            .map_err(ZerobusError::CreateStreamError)?
            .into_inner();

        let descriptor_proto = if record_type == RecordType::Proto {
            Some(
                table_properties
                    .descriptor_proto
                    .as_ref()
                    .unwrap()
                    .encode_to_vec(),
            )
        } else {
            None
        };

        let create_stream_request = RequestPayload::CreateStream(CreateIngestStreamRequest {
            table_name: Some(table_properties.table_name.to_string()),
            descriptor_proto,
            record_type: Some(record_type.into()),
        });

        debug!("Sending CreateStream request.");
        tx.send(EphemeralStreamRequest {
            payload: Some(create_stream_request),
        })
        .await
        .map_err(|_| {
            error!(table_name = %table_properties.table_name, "Failed to send CreateStream request");
            ZerobusError::StreamClosedError(tonic::Status::internal(
                "Failed to send CreateStream request",
            ))
        })?;
        debug!("Waiting for CreateStream response.");
        let create_stream_response = response_grpc_stream.message().await;

        match create_stream_response {
            Ok(Some(create_stream_response)) => match create_stream_response.payload {
                Some(ResponsePayload::CreateStreamResponse(resp)) => {
                    if let Some(stream_id) = resp.stream_id {
                        info!(stream_id = %stream_id, "Successfully created stream");
                        Ok((tx, response_grpc_stream, stream_id))
                    } else {
                        error!("Successfully created a stream but stream_id is None");
                        Err(ZerobusError::CreateStreamError(tonic::Status::internal(
                            "Successfully created a stream but stream_id is None",
                        )))
                    }
                }
                unexpected_message => {
                    error!("Unexpected response from server {unexpected_message:?}");
                    Err(ZerobusError::CreateStreamError(tonic::Status::internal(
                        "Unexpected response from server",
                    )))
                }
            },
            Ok(None) => {
                info!("Server closed the stream gracefully before sending CreateStream response");
                Err(ZerobusError::CreateStreamError(tonic::Status::ok(
                    "Stream closed gracefully by server",
                )))
            }
            Err(status) => {
                error!("CreateStream RPC failed: {status:?}");
                Err(ZerobusError::CreateStreamError(status))
            }
        }
    }

    /// Ingests a protobuf-encoded record into the stream.
    ///
    /// This method is non-blocking and returns immediately with a future. The record is
    /// queued for transmission and the returned future resolves when the server acknowledges
    /// the record has been durably written.
    ///
    /// # Arguments
    ///
    /// * `payload` - Protobuf-encoded record bytes (use `prost::Message::encode_to_vec()`)
    ///
    /// # Returns
    ///
    /// A future that resolves to the logical offset ID of the acknowledged record.
    ///
    /// # Errors
    ///
    /// * `StreamClosedError` - If the stream has been closed
    /// * Other errors may be returned via the acknowledgment future
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # use prost::Message;
    /// # async fn example(stream: ZerobusStream) -> Result<(), ZerobusError> {
    /// # let my_record = vec![1, 2, 3]; // Example protobuf-encoded data
    /// // Ingest and immediately await acknowledgment
    /// let ack = stream.ingest_record(my_record).await?;
    /// let offset = ack.await?;
    /// println!("Record written at offset: {}", offset);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ingest_record(
        &self,
        payload: impl Into<RecordPayload>,
    ) -> ZerobusResult<impl Future<Output = ZerobusResult<i64>>> {
        // Convert to RecordPayload first
        let payload: RecordPayload = payload.into();
        if self.is_closed.load(Ordering::Relaxed) {
            error!(table_name = %self.table_properties.table_name, "Stream closed");
            return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                "Stream closed",
            )));
        }
        match (&payload, self.options.record_type) {
            (RecordPayload::Json(_), RecordType::Proto) => {
                return Err(ZerobusError::InvalidArgument(
                    "Json record type is not supported for proto stream".to_string(),
                ));
            }
            (RecordPayload::Proto(_), RecordType::Json) => {
                return Err(ZerobusError::InvalidArgument(
                    "Proto record type is not supported for json stream".to_string(),
                ));
            }
            _ => {}
        }
        let _guard = self.sync_mutex.lock().await;

        let offset_id = self.logical_offset_id_generator.next();
        debug!(offset_id = offset_id, "Ingesting record");

        if let Some(stream_id) = self.stream_id.as_ref() {
            let (tx, rx) = tokio::sync::oneshot::channel();
            {
                let mut map = self.oneshot_map.lock().await;
                map.insert(offset_id, tx);
            }
            self.landing_zone
                .add(Box::new(IngestRecord { payload, offset_id }))
                .await;
            let stream_id = stream_id.to_string();
            Ok(async move {
                rx.await.map_err(|err| {
                    error!(stream_id = %stream_id, "Failed to receive ack: {}", err);
                    ZerobusError::StreamClosedError(tonic::Status::internal(
                        "Failed to receive ack",
                    ))
                })?
            })
        } else {
            error!("Stream ID is None");
            Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                "Stream ID is None",
            )))
        }
    }

    /// Spawns a task that continuously reads from `response_grpc_stream`
    /// and propagates the received durability acknowledgements to the
    /// corresponding pending acks promises.
    #[instrument(level = "debug", skip_all)]
    fn spawn_receiver_task(
        mut response_grpc_stream: tonic::Streaming<EphemeralStreamResponse>,
        last_received_offset_id_tx: tokio::sync::watch::Sender<Option<OffsetId>>,
        ack_timeout_ms: u64,
        landing_zone: RecordLandingZone<RecordPayload>,
        oneshot_map: Arc<tokio::sync::Mutex<OneshotMap>>,
        recovery_enabled: bool,
    ) -> tokio::task::JoinHandle<ZerobusResult<()>> {
        tokio::spawn(async move {
            let span = span!(Level::DEBUG, "inbound_stream_processor");
            let _guard = span.enter();
            let mut last_acked_offset = -1;

            loop {
                let message_result = tokio::time::timeout(
                    Duration::from_millis(ack_timeout_ms),
                    response_grpc_stream.message(),
                )
                .await;
                match message_result {
                    Ok(Ok(Some(ingest_record_response))) => match ingest_record_response.payload {
                        Some(ResponsePayload::IngestRecordResponse(IngestRecordResponse {
                            durability_ack_up_to_offset,
                        })) => {
                            let durability_ack_up_to_offset = match durability_ack_up_to_offset {
                                Some(offset) => offset,
                                None => {
                                    error!("Missing ack offset in server response");
                                    return Err(ZerobusError::StreamClosedError(
                                        tonic::Status::internal(
                                            "Missing ack offset in server response",
                                        ),
                                    ));
                                }
                            };
                            let mut last_logical_acked_offset = -2;
                            for _offset_to_ack in
                                (last_acked_offset + 1)..=durability_ack_up_to_offset
                            {
                                if let Ok(record) = landing_zone.remove_observed() {
                                    let logical_offset = record.offset_id;
                                    last_logical_acked_offset = logical_offset;

                                    let mut map = oneshot_map.lock().await;
                                    if let Some(sender) = map.remove(&logical_offset) {
                                        let _ = sender.send(Ok(logical_offset));
                                    }
                                }
                            }
                            last_acked_offset = durability_ack_up_to_offset;
                            if last_logical_acked_offset != -2 {
                                let _ignore_on_channel_break = last_received_offset_id_tx
                                    .send(Some(last_logical_acked_offset));
                            }
                        }
                        Some(ResponsePayload::CloseStreamSignal(CloseStreamSignal {
                            duration,
                        })) => {
                            if recovery_enabled {
                                let duration_ms = duration
                                    .as_ref()
                                    .map(|d| {
                                        d.seconds as f64 * 1000.0 + d.nanos as f64 / 1_000_000.0
                                    })
                                    .unwrap_or(0.0);

                                info!("Server will close the stream in {:.3}ms. Triggering stream recovery.", duration_ms);
                                return Ok(());
                            }
                        }
                        unexpected_message => {
                            error!("Unexpected response from server {unexpected_message:?}");
                            return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                                "Unexpected response from server",
                            )));
                        }
                    },
                    Ok(Ok(None)) => {
                        info!("Server closed the stream without errors.");
                        return Err(ZerobusError::StreamClosedError(tonic::Status::ok(
                            "Stream closed by server without errors.",
                        )));
                    }
                    Ok(Err(status)) => {
                        error!("Unexpected response from server {status:?}");
                        return Err(ZerobusError::StreamClosedError(status));
                    }
                    Err(_timeout) => {
                        // No message received for ack_timeout_ms
                        if !landing_zone.is_observed_empty() {
                            error!("Server ack timeout: no response for {}ms", ack_timeout_ms);
                            return Err(ZerobusError::StreamClosedError(
                                tonic::Status::deadline_exceeded("Server ack timeout"),
                            ));
                        }
                    }
                }
            }
        })
    }

    /// Spawns a task that continuously sends records to the Ingest API by observing the landing zone
    /// to get records and sending them through the outbound stream to the gRPC stream.
    fn spawn_sender_task(
        outbound_stream: tokio::sync::mpsc::Sender<EphemeralStreamRequest>,
        landing_zone: RecordLandingZone<RecordPayload>,
    ) -> tokio::task::JoinHandle<ZerobusResult<()>> {
        tokio::spawn(async move {
            let physical_offset_id_generator = OffsetIdGenerator::default();
            loop {
                let item = landing_zone.observe().await;
                let send_result = outbound_stream
                    .send(EphemeralStreamRequest {
                        payload: Some(RequestPayload::IngestRecord(IngestRecordRequest {
                            record: Some(item.payload.clone().into_record()),
                            offset_id: Some(physical_offset_id_generator.next()),
                        })),
                    })
                    .await;

                if let Err(err) = send_result {
                    error!("Failed to send record: {}", err);
                    return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                        "Failed to send record",
                    )));
                }
            }
        })
    }

    /// Fails all pending records by removing them from the landing zone and sending error to all pending acks promises.
    async fn fail_all_pending_records(
        landing_zone: RecordLandingZone<RecordPayload>,
        oneshot_map: Arc<tokio::sync::Mutex<OneshotMap>>,
        failed_records: Arc<RwLock<Vec<RecordPayload>>>,
        error: &ZerobusError,
    ) {
        let mut failed_payloads = Vec::with_capacity(landing_zone.len());
        let records = landing_zone.remove_all();
        let mut map = oneshot_map.lock().await;
        for record in records {
            failed_payloads.push(record.payload);
            if let Some(sender) = map.remove(&record.offset_id) {
                let _ = sender.send(Err(error.clone()));
            }
        }
        *failed_records.write().await = failed_payloads;
    }

    /// Flushes all currently pending records and waits for their acknowledgments.
    ///
    /// This method captures the current highest offset and waits until all records up to
    /// that offset have been acknowledged by the server. Records ingested during the flush
    /// operation are not included in this flush.
    ///
    /// # Returns
    ///
    /// `Ok(())` when all pending records at the time of the call have been acknowledged.
    ///
    /// # Errors
    ///
    /// * `StreamClosedError` - If the stream is closed or times out
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # async fn example(stream: ZerobusStream) -> Result<(), ZerobusError> {
    /// // Ingest many records
    /// for i in 0..1000 {
    ///     let ack = stream.ingest_record(vec![i as u8]).await?;
    ///     tokio::spawn(ack); // Fire and forget
    /// }
    ///
    /// // Wait for all to be acknowledged
    /// stream.flush().await?;
    /// println!("All 1000 records have been acknowledged");
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(level = "debug", skip_all, fields(table_name = %self.table_properties.table_name))]
    pub async fn flush(&self) -> ZerobusResult<()> {
        let flush_operation = async {
            loop {
                if self.is_closed.load(Ordering::Relaxed) {
                    return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                        "Stream closed during flush",
                    )));
                }
                let offset_to_wait = match self.logical_offset_id_generator.last() {
                    Some(offset) => offset,
                    None => return Ok(()),
                };
                let mut offset_receiver = self.logical_last_received_offset_id_tx.subscribe();
                loop {
                    let offset = *offset_receiver.borrow_and_update();

                    let stream_id = match self.stream_id.as_deref() {
                        Some(stream_id) => stream_id,
                        None => {
                            error!("Stream ID is None during flush");
                            "None"
                        }
                    };
                    if let Some(offset) = offset {
                        if offset >= offset_to_wait {
                            info!(stream_id = %stream_id, "Stream is caught up to the given offset. Flushing complete.");
                            return Ok(());
                        } else {
                            info!(
                                stream_id = %stream_id,
                                "Stream is caught up to offset {}. Waiting for offset {}.",
                                offset, offset_to_wait
                            );
                        }
                    } else {
                        info!(
                            stream_id = %stream_id,
                            "Stream is not caught up to any offset yet. Waiting for the first offset."
                        );
                    }
                    // If offset_receiver channel is closed, break the loop.
                    if offset_receiver.changed().await.is_err() {
                        break;
                    }
                }

                sleep(Duration::from_millis(self.options.recovery_timeout_ms)).await;
                // TODO Add a watch channel to alert on is_closed change, this causes unnecessary wakeups.
            }
        };

        match tokio::time::timeout(
            Duration::from_millis(self.options.flush_timeout_ms),
            flush_operation,
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => {
                if let Some(stream_id) = self.stream_id.as_deref() {
                    error!(stream_id = %stream_id, table_name = %self.table_properties.table_name, "Flush timed out");
                } else {
                    error!(table_name = %self.table_properties.table_name, "Flush timed out");
                }
                Err(ZerobusError::StreamClosedError(
                    tonic::Status::deadline_exceeded("Flush timed out"),
                ))
            }
        }
    }

    /// Closes the stream gracefully after flushing all pending records.
    ///
    /// This method first calls `flush()` to ensure all pending records are acknowledged,
    /// then shuts down the stream and releases all resources. Always call this method
    /// when you're done with a stream to ensure data integrity.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the stream was closed successfully after flushing all records.
    ///
    /// # Errors
    ///
    /// Returns any errors from the flush operation. If flush fails, some records
    /// may not have been acknowledged. Use `get_unacked_records()` to retrieve them.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # async fn example(mut stream: ZerobusStream) -> Result<(), ZerobusError> {
    /// // After ingesting records...
    /// stream.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn close(&mut self) -> ZerobusResult<()> {
        if self.is_closed.load(Ordering::Relaxed) {
            return Ok(());
        }
        if let Some(stream_id) = self.stream_id.as_deref() {
            info!(stream_id = %stream_id, "Closing stream");
        } else {
            error!("Stream ID is None during closing");
        }
        self.flush().await?;
        self.is_closed.store(true, Ordering::Relaxed);
        self.supervisor_task.abort();
        Ok(())
    }

    /// Returns all records that were ingested but not acknowledged by the server.
    ///
    /// This method should only be called after a stream has failed or been closed.
    /// It's useful for implementing retry logic or persisting failed records.
    ///
    /// # Returns
    ///
    /// A vector of protobuf-encoded record payloads that weren't acknowledged.
    ///
    /// # Errors
    ///
    /// * `InvalidStateError` - If called on an active (not closed) stream
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # async fn example(sdk: ZerobusSdk, mut stream: ZerobusStream) -> Result<(), ZerobusError> {
    /// match stream.close().await {
    ///     Err(e) => {
    ///         // Stream failed, get unacked records
    ///         let unacked = stream.get_unacked_records().await?;
    ///         println!("Failed to acknowledge {} records", unacked.len());
    ///         
    ///         // Recreate stream with unacked records
    ///         let new_stream = sdk.recreate_stream(stream).await?;
    ///     }
    ///     Ok(_) => println!("All records acknowledged"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_unacked_records(&self) -> ZerobusResult<Vec<RecordPayload>> {
        if self.is_closed.load(Ordering::Relaxed) {
            let failed = self.failed_records.read().await.clone();
            return Ok(failed);
        }
        if let Some(stream_id) = self.stream_id.as_deref() {
            error!(stream_id = %stream_id, "Cannot get unacked records from an active stream. Stream must be closed first.");
        } else {
            error!(
                "Cannot get unacked records from an active stream. Stream must be closed first."
            );
        }
        Err(ZerobusError::InvalidStateError(
            "Cannot get unacked records from an active stream. Stream must be closed first."
                .to_string(),
        ))
    }
}
