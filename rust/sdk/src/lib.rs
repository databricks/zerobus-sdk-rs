//! # Databricks Zerobus Ingest SDK
//!
//! A high-performance Rust client for streaming data ingestion into Databricks Delta tables.
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use databricks_zerobus_ingest_sdk::{ZerobusSdk, TableProperties, ProtoMessage};
//!
//! let sdk = ZerobusSdk::builder()
//!     .endpoint(zerobus_endpoint)
//!     .unity_catalog_url(uc_endpoint)
//!     .build()?;
//! let stream = sdk.create_stream(table_properties, client_id, client_secret, None).await?;
//!
//! // Ingest a record and wait for acknowledgment
//! let offset = stream.ingest_record_offset(ProtoMessage(my_message)).await?;
//! stream.wait_for_offset(offset).await?;
//!
//! stream.close().await?;
//! ```
//!
//! See the `examples/` directory for complete working examples.

pub mod databricks {
    pub mod zerobus {
        include!(concat!(env!("OUT_DIR"), "/databricks.zerobus.rs"));
    }
}

#[cfg(feature = "arrow-flight")]
mod arrow_configuration;
#[cfg(feature = "arrow-flight")]
mod arrow_metadata;
#[cfg(feature = "arrow-flight")]
mod arrow_stream;
mod builder;
mod callbacks;
mod default_token_factory;
mod errors;
mod headers_provider;
mod landing_zone;
mod offset_generator;
mod proxy;
mod record_types;
mod stream_configuration;
mod stream_options;
mod tls_config;

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use prost::Message;
use tokio::sync::RwLock;
use tokio::time::Duration;
use tokio_retry::strategy::FixedInterval;
use tokio_retry::RetryIf;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, error, info, instrument, span, warn, Level};

use databricks::zerobus::ephemeral_stream_request::Payload as RequestPayload;
use databricks::zerobus::ephemeral_stream_response::Payload as ResponsePayload;
use databricks::zerobus::zerobus_client::ZerobusClient;
use databricks::zerobus::{
    CloseStreamSignal, CreateIngestStreamRequest, EphemeralStreamRequest, EphemeralStreamResponse,
    IngestRecordResponse, RecordType,
};
use landing_zone::LandingZone;

/// **Experimental/Unsupported**: Arrow Flight ingestion is experimental and not yet
/// supported for production use. The API may change in future releases.
#[cfg(feature = "arrow-flight")]
pub use arrow_configuration::ArrowStreamConfigurationOptions;
#[cfg(feature = "arrow-flight")]
pub use arrow_stream::{
    ArrowSchema, ArrowTableProperties, DataType, Field, RecordBatch, ZerobusArrowStream,
};
pub use builder::ZerobusSdkBuilder;
pub use callbacks::AckCallback;
pub use default_token_factory::DefaultTokenFactory;
pub use errors::ZerobusError;
pub use headers_provider::{HeadersProvider, OAuthHeadersProvider, DEFAULT_X_ZEROBUS_SDK};
pub use offset_generator::{OffsetId, OffsetIdGenerator};
pub use record_types::{
    EncodedBatch, EncodedBatchIter, EncodedRecord, JsonEncodedRecord, JsonString, JsonValue,
    ProtoBytes, ProtoEncodedRecord, ProtoMessage,
};
pub use stream_configuration::StreamConfigurationOptions;
#[cfg(feature = "testing")]
pub use tls_config::NoTlsConfig;
pub use tls_config::{SecureTlsConfig, TlsConfig};

const SHUTDOWN_TIMEOUT_SECS: u64 = 2;

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

#[derive(Debug, Clone)]
struct IngestRequest {
    payload: EncodedBatch,
    offset_id: OffsetId,
}

/// Map of logical offset to oneshot sender used to send acknowledgments back to the client.
type OneshotMap = HashMap<OffsetId, tokio::sync::oneshot::Sender<ZerobusResult<OffsetId>>>;
/// Landing zone for ingest records.
type RecordLandingZone = Arc<LandingZone<Box<IngestRequest>>>;

/// Messages sent to the callback handler task.
#[derive(Debug, Clone)]
enum CallbackMessage {
    /// Acknowledgment callback with logical offset ID.
    Ack(OffsetId),
    /// Error callback with logical offset ID and error message.
    Error(OffsetId, String),
}

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
/// let offset = stream.ingest_record_offset(data).await?;
/// println!("Record sent with offset: {}", offset);
///
/// // Wait for acknowledgment
/// stream.wait_for_offset(offset).await?;
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
    failed_records: Arc<RwLock<Vec<EncodedBatch>>>,
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
/// // Create SDK using the builder
/// let sdk = ZerobusSdk::builder()
///     .endpoint("https://your-workspace.zerobus.region.cloud.databricks.com")
///     .unity_catalog_url("https://your-workspace.cloud.databricks.com")
///     .build()?;
///
/// // Define the arguments for the ephemeral stream.
/// let table_properties = TableProperties {
///     table_name: "test_table".to_string(),
///     descriptor_proto: Default::default(),
/// };
/// let options = StreamConfigurationOptions {
///     max_inflight_requests: 100,
///     ..Default::default()
/// };
/// let client_id = "your-client-id".to_string();
/// let client_secret = "your-client-secret".to_string();
///
/// // Create a stream
/// let stream = sdk.create_stream(table_properties, client_id, client_secret, Some(options)).await?;
///
/// // Ingest a single record
/// let offset_id = stream.ingest_record_offset(row.encode_to_vec()).await?;
/// println!("Record sent with offset Id: {}", offset_id);
///
/// // Wait for acknowledgment
/// stream.wait_for_offset(offset_id).await?;
/// println!("Record acknowledged with offset Id: {}", offset_id);
/// # Ok(())
/// # }
/// ```
pub struct ZerobusSdk {
    pub zerobus_endpoint: String,
    /// Deprecated: This field is no longer used. TLS is now controlled via `tls_config`.
    #[deprecated(
        since = "0.5.0",
        note = "This field is no longer used. TLS is controlled via tls_config."
    )]
    pub use_tls: bool,
    pub unity_catalog_url: String,
    shared_channel: tokio::sync::Mutex<Option<ZerobusClient<Channel>>>,
    workspace_id: String,
    tls_config: Arc<dyn TlsConfig>,
}

impl ZerobusSdk {
    /// Creates a new SDK builder for fluent configuration.
    ///
    /// This is the recommended way to create a `ZerobusSdk` instance.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use databricks_zerobus_ingest_sdk::ZerobusSdk;
    ///
    /// let sdk = ZerobusSdk::builder()
    ///     .endpoint("https://workspace.zerobus.databricks.com")
    ///     .unity_catalog_url("https://workspace.cloud.databricks.com")
    ///     .build()?;
    /// # Ok::<(), databricks_zerobus_ingest_sdk::ZerobusError>(())
    /// ```
    pub fn builder() -> ZerobusSdkBuilder {
        ZerobusSdkBuilder::new()
    }

    /// Creates a new Zerobus SDK instance.
    ///
    /// # Deprecated
    ///
    /// Use [`ZerobusSdk::builder()`] instead for more flexible configuration:
    ///
    /// ```no_run
    /// use databricks_zerobus_ingest_sdk::ZerobusSdk;
    ///
    /// let sdk = ZerobusSdk::builder()
    ///     .endpoint("https://workspace.zerobus.databricks.com")
    ///     .unity_catalog_url("https://workspace.cloud.databricks.com")
    ///     .build()?;
    /// # Ok::<(), databricks_zerobus_ingest_sdk::ZerobusError>(())
    /// ```
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
    #[deprecated(since = "0.5.0", note = "Use ZerobusSdk::builder() instead")]
    #[allow(clippy::result_large_err)]
    pub fn new(zerobus_endpoint: String, unity_catalog_url: String) -> ZerobusResult<Self> {
        let zerobus_endpoint = if !zerobus_endpoint.starts_with("https://")
            && !zerobus_endpoint.starts_with("http://")
        {
            format!("https://{}", zerobus_endpoint)
        } else {
            zerobus_endpoint
        };

        let workspace_id = zerobus_endpoint
            .strip_prefix("https://")
            .or_else(|| zerobus_endpoint.strip_prefix("http://"))
            .and_then(|s| s.split('.').next())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                ZerobusError::InvalidArgument(
                    "Failed to extract workspace ID from zerobus_endpoint".to_string(),
                )
            })?;

        #[allow(deprecated)]
        Ok(ZerobusSdk {
            zerobus_endpoint,
            use_tls: true,
            unity_catalog_url,
            workspace_id,
            shared_channel: tokio::sync::Mutex::new(None),
            tls_config: Arc::new(SecureTlsConfig::new()),
        })
    }

    /// Creates a new SDK instance with explicit configuration.
    ///
    /// This is used internally by the builder pattern.
    pub(crate) fn new_with_config(
        zerobus_endpoint: String,
        unity_catalog_url: String,
        workspace_id: String,
        tls_config: Arc<dyn TlsConfig>,
    ) -> Self {
        #[allow(deprecated)]
        ZerobusSdk {
            zerobus_endpoint,
            use_tls: true,
            unity_catalog_url,
            workspace_id,
            shared_channel: tokio::sync::Mutex::new(None),
            tls_config,
        }
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

        let channel = self.get_or_create_channel_zerobus_client().await?;
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
    ///         let new_stream = sdk.recreate_stream(&stream).await?;
    ///         // Continue using new_stream
    ///     }
    ///     Ok(_) => println!("Stream closed successfully"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(level = "debug", skip_all)]
    pub async fn recreate_stream(&self, stream: &ZerobusStream) -> ZerobusResult<ZerobusStream> {
        let batches = stream.get_unacked_batches().await?;
        let new_stream = self
            .create_stream_with_headers_provider(
                stream.table_properties.clone(),
                Arc::clone(&stream.headers_provider),
                Some(stream.options.clone()),
            )
            .await?;
        for batch in batches {
            let ack = new_stream.ingest_internal(batch).await?;
            tokio::spawn(ack);
        }
        return Ok(new_stream);
    }

    /// Creates a new Arrow Flight ingestion stream to a Unity Catalog table.
    ///
    /// This establishes an Arrow Flight stream for high-performance ingestion of
    /// Arrow RecordBatches. Authentication is handled automatically using the
    /// provided OAuth credentials.
    ///
    /// # Arguments
    ///
    /// * `table_properties` - Table name and Arrow schema
    /// * `client_id` - OAuth client ID for authentication
    /// * `client_secret` - OAuth client secret for authentication
    /// * `options` - Optional Arrow stream configuration (uses defaults if `None`)
    ///
    /// # Returns
    ///
    /// A `ZerobusArrowStream` ready for ingesting Arrow RecordBatches.
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
    /// # use std::sync::Arc;
    /// # use arrow_schema::{Schema as ArrowSchema, Field, DataType};
    /// # async fn example(sdk: ZerobusSdk) -> Result<(), ZerobusError> {
    /// let schema = Arc::new(ArrowSchema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    ///     Field::new("name", DataType::Utf8, true),
    /// ]));
    ///
    /// let table_props = ArrowTableProperties {
    ///     table_name: "catalog.schema.table".to_string(),
    ///     schema,
    /// };
    ///
    /// let stream = sdk.create_arrow_stream(
    ///     table_props,
    ///     "client-id".to_string(),
    ///     "client-secret".to_string(),
    ///     None,
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "arrow-flight")]
    #[instrument(level = "debug", skip_all)]
    pub async fn create_arrow_stream(
        &self,
        table_properties: ArrowTableProperties,
        client_id: String,
        client_secret: String,
        options: Option<ArrowStreamConfigurationOptions>,
    ) -> ZerobusResult<ZerobusArrowStream> {
        let headers_provider = OAuthHeadersProvider::new(
            client_id,
            client_secret,
            table_properties.table_name.clone(),
            self.workspace_id.clone(),
            self.unity_catalog_url.clone(),
        );
        self.create_arrow_stream_with_headers_provider(
            table_properties,
            Arc::new(headers_provider),
            options,
        )
        .await
    }

    /// Creates a new Arrow Flight stream with a custom headers provider.
    ///
    /// This is an advanced method that allows you to implement your own authentication
    /// logic by providing a custom implementation of the `HeadersProvider` trait.
    ///
    /// # Arguments
    ///
    /// * `table_properties` - Table name and Arrow schema
    /// * `headers_provider` - An `Arc` holding your custom `HeadersProvider` implementation
    /// * `options` - Optional Arrow stream configuration (uses defaults if `None`)
    ///
    /// # Returns
    ///
    /// A `ZerobusArrowStream` ready for ingesting Arrow RecordBatches.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # use std::collections::HashMap;
    /// # use std::sync::Arc;
    /// # use async_trait::async_trait;
    /// # use arrow_schema::{Schema as ArrowSchema, Field, DataType};
    /// #
    /// # struct MyHeadersProvider;
    /// #
    /// # #[async_trait]
    /// # impl HeadersProvider for MyHeadersProvider {
    /// #     async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
    /// #         let mut headers = HashMap::new();
    /// #         headers.insert("authorization", "Bearer my-token".to_string());
    /// #         Ok(headers)
    /// #     }
    /// # }
    /// #
    /// # async fn example(sdk: ZerobusSdk) -> Result<(), ZerobusError> {
    /// let schema = Arc::new(ArrowSchema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]));
    ///
    /// let table_props = ArrowTableProperties {
    ///     table_name: "catalog.schema.table".to_string(),
    ///     schema,
    /// };
    ///
    /// let headers_provider = Arc::new(MyHeadersProvider);
    ///
    /// let stream = sdk.create_arrow_stream_with_headers_provider(
    ///     table_props,
    ///     headers_provider,
    ///     None,
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "arrow-flight")]
    #[instrument(level = "debug", skip_all)]
    pub async fn create_arrow_stream_with_headers_provider(
        &self,
        table_properties: ArrowTableProperties,
        headers_provider: Arc<dyn HeadersProvider>,
        options: Option<ArrowStreamConfigurationOptions>,
    ) -> ZerobusResult<ZerobusArrowStream> {
        let options = options.unwrap_or_default();

        let stream = ZerobusArrowStream::new(
            &self.zerobus_endpoint,
            Arc::clone(&self.tls_config),
            table_properties,
            headers_provider,
            options,
        )
        .await;

        match stream {
            Ok(stream) => {
                info!(
                    table_name = %stream.table_name(),
                    "Successfully created new Arrow Flight stream"
                );
                Ok(stream)
            }
            Err(e) => {
                error!("Arrow Flight stream initialization failed: {}", e);
                Err(e)
            }
        }
    }

    /// Recreates an Arrow Flight stream from a failed or closed stream, replaying any
    /// unacknowledged batches.
    ///
    /// This method is useful when you want to manually recover from a stream failure
    /// or continue ingestion after closing a stream with unacknowledged batches.
    /// It creates a new stream with the same configuration and automatically ingests
    /// any batches that were not acknowledged in the original stream.
    ///
    /// # Arguments
    ///
    /// * `stream` - A reference to the failed or closed Arrow Flight stream
    ///
    /// # Returns
    ///
    /// A new `ZerobusArrowStream` with the same configuration, with unacked batches
    /// already queued for ingestion.
    ///
    /// # Errors
    ///
    /// * `InvalidStateError` - If the source stream is still active
    /// * `CreateStreamError` - If stream creation fails
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # use arrow_array::RecordBatch;
    /// # async fn example(sdk: ZerobusSdk, mut stream: ZerobusArrowStream) -> Result<(), ZerobusError> {
    /// // Ingest some batches
    /// // ...
    ///
    /// // Stream fails for some reason
    /// match stream.flush().await {
    ///     Err(_) => {
    ///         // Close the failed stream
    ///         stream.close().await.ok();
    ///
    ///         // Recreate and retry
    ///         let new_stream = sdk.recreate_arrow_stream(&stream).await?;
    ///         new_stream.flush().await?;
    ///     }
    ///     Ok(_) => {}
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "arrow-flight")]
    #[instrument(level = "debug", skip_all)]
    pub async fn recreate_arrow_stream(
        &self,
        stream: &ZerobusArrowStream,
    ) -> ZerobusResult<ZerobusArrowStream> {
        let batches = stream.get_unacked_batches().await?;

        let new_stream = self
            .create_arrow_stream_with_headers_provider(
                stream.table_properties().clone(),
                stream.headers_provider(),
                Some(stream.options().clone()),
            )
            .await?;

        // Replay unacked batches.
        for batch in batches {
            let _offset = new_stream.ingest_batch(batch).await?;
        }

        info!(
            table_name = %new_stream.table_name(),
            "Successfully recreated Arrow Flight stream"
        );

        Ok(new_stream)
    }

    /// Gets or creates the shared Channel for all streams.
    /// The first call creates the Channel, subsequent calls clone it.
    /// All clones share the same underlying TCP connection via HTTP/2 multiplexing.
    async fn get_or_create_channel_zerobus_client(&self) -> ZerobusResult<ZerobusClient<Channel>> {
        let mut guard = self.shared_channel.lock().await;

        if guard.is_none() {
            // Create the channel for the first time.
            let endpoint = Endpoint::from_shared(self.zerobus_endpoint.clone())
                .map_err(|err| ZerobusError::ChannelCreationError(err.to_string()))?;

            let endpoint = self.tls_config.configure_endpoint(endpoint)?;

            // Check for HTTP proxy env vars (https_proxy, HTTPS_PROXY, etc.)
            // and use a proxy connector if one is configured.
            let host = endpoint.uri().host().unwrap_or_default().to_string();

            let channel = if !proxy::is_no_proxy(&host) {
                if let Some(proxy_connector) = proxy::create_proxy_connector() {
                    endpoint.connect_with_connector_lazy(proxy_connector)
                } else {
                    endpoint.connect_lazy()
                }
            } else {
                endpoint.connect_lazy()
            };

            let client = ZerobusClient::new(channel)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX);

            *guard = Some(client);
        }

        Ok(guard
            .as_ref()
            .expect("Channel was just initialized")
            .clone())
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
            server_error_rx,
            cancellation_token,
            callback_handler_task,
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
        landing_zone: RecordLandingZone,
        oneshot_map: Arc<tokio::sync::Mutex<OneshotMap>>,
        logical_last_received_offset_id_tx: tokio::sync::watch::Sender<Option<OffsetId>>,
        is_closed: Arc<AtomicBool>,
        failed_records: Arc<RwLock<Vec<EncodedBatch>>>,
        stream_init_result_tx: tokio::sync::oneshot::Sender<ZerobusResult<String>>,
        server_error_tx: tokio::sync::watch::Sender<Option<ZerobusError>>,
        cancellation_token: CancellationToken,
        callback_tx: Option<tokio::sync::mpsc::UnboundedSender<CallbackMessage>>,
    ) -> ZerobusResult<()> {
        let mut initial_stream_creation = true;
        let mut stream_init_result_tx = Some(stream_init_result_tx);

        loop {
            debug!("Supervisor task loop");

            if cancellation_token.is_cancelled() {
                debug!("Supervisor task cancelled, exiting");
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
                            &callback_tx,
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
                info!(stream_id = %stream_id, "Successfully created stream");
            } else {
                info!(stream_id = %stream_id, "Successfully recovered stream");
                let _ = server_error_tx.send(None);
            }

            // 2. Reset landing zone.
            landing_zone_recovery.reset_observe();

            // 3. Spawn receiver and sender task.
            let is_paused = Arc::new(AtomicBool::new(false));
            let mut recv_task = Self::spawn_receiver_task(
                response_grpc_stream,
                logical_last_received_offset_id_tx.clone(),
                landing_zone_receiver,
                oneshot_map.clone(),
                Arc::clone(&is_paused),
                options.clone(),
                server_error_tx.clone(),
                cancellation_token.clone(),
                callback_tx.clone(),
            );
            let mut send_task = Self::spawn_sender_task(
                tx,
                landing_zone_sender,
                Arc::clone(&is_paused),
                server_error_tx.clone(),
                cancellation_token.clone(),
            );

            // 4. Wait for any of the two tasks to end.
            let result = tokio::select! {
                recv_result = &mut recv_task => {
                    send_task.abort();
                    match recv_result {
                        Ok(Err(e)) => Err(e),
                        Err(e) => Err(ZerobusError::UnexpectedStreamResponseError(
                            format!("Receiver task panicked: {}", e)
                        )),
                        Ok(Ok(())) => {
                            info!("Receiver task completed successfully");
                            Ok(())
                        }
                    }
                }
                send_result = &mut send_task => {
                    recv_task.abort();
                    match send_result {
                        Ok(Err(e)) => Err(e),
                        Err(e) => Err(ZerobusError::UnexpectedStreamResponseError(
                            format!("Sender task panicked: {}", e)
                        )),
                        Ok(Ok(())) => Ok(()) // This only happens when the sender task receives a cancellation signal.
                    }
                }
            };

            // 5. Handle errors.
            if let Err(error) = result {
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
                let _ = server_error_tx.send(Some(error.clone()));
                if !error.is_retryable() || !options.recovery {
                    is_closed.store(true, Ordering::Relaxed);
                    Self::fail_all_pending_records(
                        landing_zone.clone(),
                        oneshot_map.clone(),
                        failed_records.clone(),
                        &error,
                        &callback_tx,
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
                        .map_err(|e| ZerobusError::InvalidTableName(e.to_string()))?;
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
                    .ok_or_else(|| {
                        ZerobusError::InvalidArgument(
                            "Descriptor proto is required for Proto record type".to_string(),
                        )
                    })?
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

    /// Ingests a single record into the stream.
    ///
    /// This method is non-blocking and returns immediately with a future. The record is
    /// queued for transmission and the returned future resolves when the server acknowledges
    /// the record has been durably written.
    ///
    /// # Arguments
    ///
    /// * `payload` - A record that can be converted to `EncodedRecord` (either JSON string or protobuf bytes)
    ///
    /// # Returns
    ///
    /// A future that resolves to the logical offset ID of the acknowledged record.
    ///
    /// # Errors
    ///
    /// * `InvalidArgument` - If the record type doesn't match stream configuration
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
    ///
    /// # Deprecation Note
    ///
    /// This method is deprecated. Use [`ingest_record_offset()`](Self::ingest_record_offset) instead,
    /// which returns the offset directly (after queuing) without Future wrapping. You can then use
    /// [`wait_for_offset()`](Self::wait_for_offset) to explicitly wait for acknowledgment when needed.
    #[deprecated(
        since = "0.4.0",
        note = "Use `ingest_record_offset()` instead which returns the offset directly after queuing"
    )]
    pub async fn ingest_record(
        &self,
        payload: impl Into<EncodedRecord>,
    ) -> ZerobusResult<impl Future<Output = ZerobusResult<OffsetId>>> {
        let encoded_batch = EncodedBatch::try_from_record(payload, self.options.record_type)
            .ok_or_else(|| {
                ZerobusError::InvalidArgument(
                    "Record type does not match stream configuration".to_string(),
                )
            })?;

        self.ingest_internal(encoded_batch).await
    }

    /// Ingests a single record and returns its logical offset directly.
    ///
    /// This is an alternative to `ingest_record()` that returns the logical offset directly
    /// as an integer (after queuing) instead of wrapping it in a Future. Use `wait_for_offset()`
    /// to explicitly wait for server acknowledgment of this offset when needed.
    ///
    /// # Arguments
    ///
    /// * `payload` - A record that can be converted to `EncodedRecord` (either JSON string or protobuf bytes)
    ///
    /// # Returns
    ///
    /// The logical offset ID assigned to this record.
    ///
    /// # Errors
    ///
    /// * `InvalidArgument` - If the record type doesn't match stream configuration
    /// * `StreamClosedError` - If the stream has been closed
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # use prost::Message;
    /// # async fn example(stream: ZerobusStream) -> Result<(), ZerobusError> {
    /// # let my_record = vec![1, 2, 3]; // Example protobuf-encoded data
    /// // Ingest and get offset immediately
    /// let offset = stream.ingest_record_offset(my_record).await?;
    ///
    /// // Later, wait for acknowledgment
    /// stream.wait_for_offset(offset).await?;
    /// println!("Record at offset {} has been acknowledged", offset);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ingest_record_offset(
        &self,
        payload: impl Into<EncodedRecord>,
    ) -> ZerobusResult<OffsetId> {
        let encoded_batch = EncodedBatch::try_from_record(payload, self.options.record_type)
            .ok_or_else(|| {
                ZerobusError::InvalidArgument(
                    "Record type does not match stream configuration".to_string(),
                )
            })?;

        self.ingest_internal_v2(encoded_batch).await
    }

    /// Ingests a batch of records into the stream.
    ///
    /// This method is non-blocking and returns immediately with a future. The records are
    /// queued for transmission and the returned future resolves when the server acknowledges
    /// the entire batch has been durably written.
    ///
    /// # Arguments
    ///
    /// * `payload` - An iterator of protobuf-encoded records (each item should be convertible to `EncodedRecord`)
    ///
    /// # Returns
    ///
    /// A future that resolves to the logical offset ID of the last acknowledged batch.
    /// If the batch is empty, the future resoles to None.
    ///
    /// # Errors
    ///
    /// * `InvalidArgument` - If record types don't match stream configuration
    /// * `StreamClosedError` - If the stream has been closed
    /// * Other errors may be returned via the acknowledgment future
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # use prost::Message;
    /// # async fn example(stream: ZerobusStream) -> Result<(), ZerobusError> {
    /// let records = vec![vec![1, 2, 3], vec![4, 5, 6]]; // Example protobuf-encoded data
    /// // Ingest batch and await acknowledgment
    /// let ack = stream.ingest_records(records).await?;
    /// let offset = ack.await?;
    /// match offset {
    ///     Some(offset) => println!("Batch written at offset: {}", offset),
    ///     None => println!("Empty batch - no records written"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Deprecation Note
    ///
    /// This method is deprecated. Use [`ingest_records_offset()`](Self::ingest_records_offset) instead,
    /// which returns the offset directly (after queuing) without Future wrapping. You can then use
    /// [`wait_for_offset()`](Self::wait_for_offset) to explicitly wait for acknowledgment when needed.
    #[deprecated(
        since = "0.4.0",
        note = "Use `ingest_records_offset()` instead which returns the offset directly after queuing"
    )]
    pub async fn ingest_records<I, T>(
        &self,
        payload: I,
    ) -> ZerobusResult<impl Future<Output = ZerobusResult<Option<OffsetId>>>>
    where
        I: IntoIterator<Item = T>,
        T: Into<EncodedRecord>,
    {
        let encoded_batch = EncodedBatch::try_from_batch(payload, self.options.record_type)
            .ok_or_else(|| {
                ZerobusError::InvalidArgument(
                    "Record type does not match stream configuration".to_string(),
                )
            })?;

        // For non-empty batches, get the future from ingest_internal
        let ingest_future = if encoded_batch.is_empty() {
            None
        } else {
            Some(self.ingest_internal(encoded_batch).await?)
        };

        Ok(async move {
            match ingest_future {
                Some(fut) => fut.await.map(Option::Some),
                None => Ok(None),
            }
        })
    }

    /// Ingests a batch of records and returns the logical offset directly.
    ///
    /// This is an alternative to `ingest_records()` that returns the logical offset directly
    /// (after queuing) instead of wrapping it in a Future. Use `wait_for_offset()` to explicitly
    /// wait for server acknowledgment when needed.
    ///
    /// # Arguments
    ///
    /// * `payload` - An iterator of records (each item should be convertible to `EncodedRecord`)
    ///
    /// # Returns
    ///
    /// `Some(offset_id)` for non-empty batches, or `None` if the batch is empty.
    ///
    /// # Errors
    ///
    /// * `InvalidArgument` - If record types don't match stream configuration
    /// * `StreamClosedError` - If the stream has been closed
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # use prost::Message;
    /// # async fn example(stream: ZerobusStream) -> Result<(), ZerobusError> {
    /// let records = vec![vec![1, 2, 3], vec![4, 5, 6]]; // Example protobuf-encoded data
    ///
    /// // Ingest batch and get offset immediately
    /// if let Some(offset) = stream.ingest_records_offset(records).await? {
    ///     // Later, wait for batch acknowledgment
    ///     stream.wait_for_offset(offset).await?;
    ///     println!("Batch at offset {} has been acknowledged", offset);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ingest_records_offset<I, T>(&self, payload: I) -> ZerobusResult<Option<OffsetId>>
    where
        I: IntoIterator<Item = T>,
        T: Into<EncodedRecord>,
    {
        let encoded_batch = EncodedBatch::try_from_batch(payload, self.options.record_type)
            .ok_or_else(|| {
                ZerobusError::InvalidArgument(
                    "Record type does not match stream configuration".to_string(),
                )
            })?;

        if encoded_batch.is_empty() {
            Ok(None)
        } else {
            self.ingest_internal_v2(encoded_batch)
                .await
                .map(Option::Some)
        }
    }
    /// Internal unified method for ingesting records and batches
    async fn ingest_internal(
        &self,
        encoded_batch: EncodedBatch,
    ) -> ZerobusResult<impl Future<Output = ZerobusResult<OffsetId>>> {
        if self.is_closed.load(Ordering::Relaxed) {
            error!(table_name = %self.table_properties.table_name, "Stream closed");
            return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                "Stream closed",
            )));
        }

        let _guard = self.sync_mutex.lock().await;

        let offset_id = self.logical_offset_id_generator.next();
        debug!(
            offset_id = offset_id,
            record_count = encoded_batch.get_record_count(),
            "Ingesting record(s)"
        );

        if let Some(stream_id) = self.stream_id.as_ref() {
            let (tx, rx) = tokio::sync::oneshot::channel();
            {
                let mut map = self.oneshot_map.lock().await;
                map.insert(offset_id, tx);
            }
            self.landing_zone
                .add(Box::new(IngestRequest {
                    payload: encoded_batch,
                    offset_id,
                }))
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

    /// Internal unified method for ingesting records and batches
    async fn ingest_internal_v2(&self, encoded_batch: EncodedBatch) -> ZerobusResult<OffsetId> {
        if self.is_closed.load(Ordering::Relaxed) {
            error!(table_name = %self.table_properties.table_name, "Stream closed");
            return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                "Stream closed",
            )));
        }

        let _guard = self.sync_mutex.lock().await;

        let offset_id = self.logical_offset_id_generator.next();
        debug!(
            offset_id = offset_id,
            record_count = encoded_batch.get_record_count(),
            "Ingesting record(s)"
        );
        self.landing_zone
            .add(Box::new(IngestRequest {
                payload: encoded_batch,
                offset_id,
            }))
            .await;
        Ok(offset_id)
    }

    /// Spawns a task that handles callback execution in a separate thread.
    /// This task receives callback messages via a channel and executes them
    /// without blocking the receiver task.
    #[instrument(level = "debug", skip_all)]
    fn spawn_callback_handler_task(
        mut callback_rx: tokio::sync::mpsc::UnboundedReceiver<CallbackMessage>,
        ack_callback: Option<Arc<dyn AckCallback>>,
        cancellation_token: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let span = span!(Level::DEBUG, "callback_handler");
            let _guard = span.enter();
            loop {
                tokio::select! {
                    biased;
                    message = callback_rx.recv() => {
                        match message {
                            Some(message) => {
                                match message {
                                    CallbackMessage::Ack(logical_offset) => {
                                        if let Some(ref callback) = ack_callback {
                                            callback.on_ack(logical_offset);
                                        }
                                    }
                                    CallbackMessage::Error(logical_offset, error_message) => {
                                        if let Some(ref callback) = ack_callback {
                                            callback.on_error(logical_offset, &error_message);
                                        }
                                    }
                                }
                            }
                            None => { // This happens when all senders are dropped.
                                debug!("Callback handler task shutting down");
                                return;
                            }
                        }
                    }
                    _ = cancellation_token.cancelled() => {
                        debug!("Callback handler task cancelled");
                        return;
                    }

                }
            }
        })
    }

    /// Spawns a task that continuously reads from `response_grpc_stream`
    /// and propagates the received durability acknowledgements to the
    /// corresponding pending acks promises.
    #[instrument(level = "debug", skip_all)]
    #[allow(clippy::too_many_arguments)]
    fn spawn_receiver_task(
        mut response_grpc_stream: tonic::Streaming<EphemeralStreamResponse>,
        last_received_offset_id_tx: tokio::sync::watch::Sender<Option<OffsetId>>,
        landing_zone: RecordLandingZone,
        oneshot_map: Arc<tokio::sync::Mutex<OneshotMap>>,
        is_paused: Arc<AtomicBool>,
        options: StreamConfigurationOptions,
        server_error_tx: tokio::sync::watch::Sender<Option<ZerobusError>>,
        cancellation_token: CancellationToken,
        callback_tx: Option<tokio::sync::mpsc::UnboundedSender<CallbackMessage>>,
    ) -> tokio::task::JoinHandle<ZerobusResult<()>> {
        tokio::spawn(async move {
            let span = span!(Level::DEBUG, "inbound_stream_processor");
            let _guard = span.enter();
            let mut last_acked_offset = -1;
            let mut pause_deadline: Option<tokio::time::Instant> = None;

            loop {
                if let Some(deadline) = pause_deadline {
                    let now = tokio::time::Instant::now();
                    let all_acked = landing_zone.is_observed_empty();

                    if now >= deadline {
                        info!("Graceful close timeout reached. Triggering recovery.");
                        return Ok(());
                    } else if all_acked {
                        info!("All in-flight records acknowledged during graceful close. Triggering recovery.");
                        return Ok(());
                    }
                }

                let message_result = if let Some(deadline) = pause_deadline {
                    tokio::select! {
                        biased;
                        _ = cancellation_token.cancelled() => return Ok(()),
                        _ = tokio::time::sleep_until(deadline) => {
                            continue;
                        }
                        res = tokio::time::timeout(
                            Duration::from_millis(options.server_lack_of_ack_timeout_ms),
                            response_grpc_stream.message(),
                        ) => res,
                    }
                } else {
                    tokio::select! {
                        biased;
                        _ = cancellation_token.cancelled() => return Ok(()),
                        res = tokio::time::timeout(
                            Duration::from_millis(options.server_lack_of_ack_timeout_ms),
                            response_grpc_stream.message(),
                        ) => res,
                    }
                };

                match message_result {
                    Ok(Ok(Some(ingest_record_response))) => match ingest_record_response.payload {
                        Some(ResponsePayload::IngestRecordResponse(IngestRecordResponse {
                            durability_ack_up_to_offset,
                        })) => {
                            let durability_ack_up_to_offset = match durability_ack_up_to_offset {
                                Some(offset) => offset,
                                None => {
                                    error!("Missing ack offset in server response");
                                    let error =
                                        ZerobusError::StreamClosedError(tonic::Status::internal(
                                            "Missing ack offset in server response",
                                        ));
                                    let _ = server_error_tx.send(Some(error.clone()));
                                    return Err(error);
                                }
                            };
                            let mut last_logical_acked_offset = -2;
                            let mut map = oneshot_map.lock().await;
                            for _offset_to_ack in
                                (last_acked_offset + 1)..=durability_ack_up_to_offset
                            {
                                if let Ok(record) = landing_zone.remove_observed() {
                                    let logical_offset = record.offset_id;
                                    last_logical_acked_offset = logical_offset;

                                    if let Some(sender) = map.remove(&logical_offset) {
                                        let _ = sender.send(Ok(logical_offset));
                                    }

                                    if let Some(ref tx) = callback_tx {
                                        let _ = tx.send(CallbackMessage::Ack(logical_offset));
                                    }
                                }
                            }
                            drop(map);
                            last_acked_offset = durability_ack_up_to_offset;
                            if last_logical_acked_offset != -2 {
                                let _ignore_on_channel_break = last_received_offset_id_tx
                                    .send(Some(last_logical_acked_offset));
                            }
                        }
                        Some(ResponsePayload::CloseStreamSignal(CloseStreamSignal {
                            duration,
                        })) => {
                            if options.recovery {
                                let server_duration_ms = duration
                                    .as_ref()
                                    .map(|d| d.seconds as u64 * 1000 + d.nanos as u64 / 1_000_000)
                                    .unwrap_or(0);

                                let wait_duration_ms = match options.stream_paused_max_wait_time_ms
                                {
                                    None => server_duration_ms,
                                    Some(0) => {
                                        // Immediate recovery
                                        info!("Server will close the stream in {}ms. Triggering stream recovery.", server_duration_ms);
                                        return Ok(());
                                    }
                                    Some(max_wait) => std::cmp::min(max_wait, server_duration_ms),
                                };

                                if wait_duration_ms == 0 {
                                    info!("Server will close the stream. Triggering immediate recovery.");
                                    return Ok(());
                                }

                                is_paused.store(true, Ordering::Relaxed);
                                pause_deadline = Some(
                                    tokio::time::Instant::now()
                                        + Duration::from_millis(wait_duration_ms),
                                );
                                info!(
                                    "Server will close the stream in {}ms. Entering graceful close period (waiting up to {}ms for in-flight acks).",
                                    server_duration_ms, wait_duration_ms
                                );
                            }
                        }
                        unexpected_message => {
                            error!("Unexpected response from server {unexpected_message:?}");
                            let error = ZerobusError::StreamClosedError(tonic::Status::internal(
                                "Unexpected response from server",
                            ));
                            let _ = server_error_tx.send(Some(error.clone()));
                            return Err(error);
                        }
                    },
                    Ok(Ok(None)) => {
                        info!("Server closed the stream without errors.");
                        let error = ZerobusError::StreamClosedError(tonic::Status::ok(
                            "Stream closed by server without errors.",
                        ));
                        let _ = server_error_tx.send(Some(error.clone()));
                        return Err(error);
                    }
                    Ok(Err(status)) => {
                        error!("Unexpected response from server {status:?}");
                        let error = ZerobusError::StreamClosedError(status);
                        let _ = server_error_tx.send(Some(error.clone()));
                        return Err(error);
                    }
                    Err(_timeout) => {
                        // No message received for server_lack_of_ack_timeout_ms.
                        if pause_deadline.is_none() && !landing_zone.is_observed_empty() {
                            error!(
                                "Server ack timeout: no response for {}ms",
                                options.server_lack_of_ack_timeout_ms
                            );
                            let error = ZerobusError::StreamClosedError(
                                tonic::Status::deadline_exceeded("Server ack timeout"),
                            );
                            let _ = server_error_tx.send(Some(error.clone()));
                            return Err(error);
                        }
                    }
                }
            }
        })
    }

    /// Spawns a task that continuously sends records to the Zerobus API by observing the landing zone
    /// to get records and sending them through the outbound stream to the gRPC stream.
    fn spawn_sender_task(
        outbound_stream: tokio::sync::mpsc::Sender<EphemeralStreamRequest>,
        landing_zone: RecordLandingZone,
        is_paused: Arc<AtomicBool>,
        server_error_tx: tokio::sync::watch::Sender<Option<ZerobusError>>,
        cancellation_token: CancellationToken,
    ) -> tokio::task::JoinHandle<ZerobusResult<()>> {
        tokio::spawn(async move {
            let physical_offset_id_generator = OffsetIdGenerator::default();
            loop {
                let item = tokio::select! {
                    biased;
                    _ = cancellation_token.cancelled() => return Ok(()),
                    item = async {
                        if is_paused.load(Ordering::Relaxed) {
                            std::future::pending().await // Wait until supervisor task aborts this task.
                        } else {
                            landing_zone.observe().await
                        }
                    } => item.clone(),
                };
                let offset_id = physical_offset_id_generator.next();
                let request_payload = item.payload.into_request_payload(offset_id);

                let send_result = outbound_stream
                    .send(EphemeralStreamRequest {
                        payload: Some(request_payload),
                    })
                    .await;

                if let Err(err) = send_result {
                    error!("Failed to send record: {}", err);
                    let error = ZerobusError::StreamClosedError(tonic::Status::internal(
                        "Failed to send record",
                    ));
                    let _ = server_error_tx.send(Some(error.clone()));
                    return Err(error);
                }
            }
        })
    }

    /// Fails all pending records by removing them from the landing zone and sending error to all pending acks promises.
    async fn fail_all_pending_records(
        landing_zone: RecordLandingZone,
        oneshot_map: Arc<tokio::sync::Mutex<OneshotMap>>,
        failed_records: Arc<RwLock<Vec<EncodedBatch>>>,
        error: &ZerobusError,
        callback_tx: &Option<tokio::sync::mpsc::UnboundedSender<CallbackMessage>>,
    ) {
        let mut failed_payloads = Vec::with_capacity(landing_zone.len());
        let records = landing_zone.remove_all();
        let mut map = oneshot_map.lock().await;
        let error_message = error.to_string();
        for record in records {
            failed_payloads.push(record.payload);
            if let Some(sender) = map.remove(&record.offset_id) {
                let _ = sender.send(Err(error.clone()));
            }
            if let Some(tx) = callback_tx {
                let _ = tx.send(CallbackMessage::Error(
                    record.offset_id,
                    error_message.clone(),
                ));
            }
        }
        *failed_records.write().await = failed_payloads;
    }

    /// Internal method to wait for a specific offset to be acknowledged.
    /// Used by both `flush()` and `wait_for_offset()`.
    async fn wait_for_offset_internal(
        &self,
        offset_to_wait: OffsetId,
        operation_name: &str,
    ) -> ZerobusResult<()> {
        let wait_operation = async {
            let mut offset_receiver = self.logical_last_received_offset_id_tx.subscribe();
            let mut error_rx = self.server_error_rx.clone();

            loop {
                let offset = *offset_receiver.borrow_and_update();

                let stream_id = match self.stream_id.as_deref() {
                    Some(stream_id) => stream_id,
                    None => {
                        error!("Stream ID is None during {}", operation_name.to_lowercase());
                        "None"
                    }
                };
                if let Some(offset) = offset {
                    if offset >= offset_to_wait {
                        info!(stream_id = %stream_id, "Stream is caught up to the given offset. {} completed.", operation_name);
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
                if self.is_closed.load(Ordering::Relaxed) {
                    // Re-check offset before failing, it might have been updated.
                    let offset = *offset_receiver.borrow_and_update();
                    if let Some(offset) = offset {
                        if offset >= offset_to_wait {
                            return Ok(());
                        }
                    }
                    // The supervisor always sends the real error to server_error_tx
                    // before setting is_closed=true, so check error_rx first to
                    // return the actual error instead of a generic one.
                    if let Some(server_error) = error_rx.borrow().clone() {
                        return Err(server_error);
                    }
                    return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                        format!("Stream closed during {}", operation_name.to_lowercase()),
                    )));
                }
                // Race between offset updates and server errors.
                tokio::select! {
                    result = offset_receiver.changed() => {
                        // If offset_receiver channel is closed, break the loop.
                        if result.is_err() {
                            break;
                        }
                        // Loop continues to check new offset value.
                    }
                    _ = error_rx.changed() => {
                        // Server error occurred, return it immediately if stream is closed.
                        if let Some(server_error) = error_rx.borrow().clone() {
                            if self.is_closed.load(Ordering::Relaxed) {
                                // Re-check offset before failing, it might have been updated.
                                let offset = *offset_receiver.borrow_and_update();
                                if let Some(offset) = offset {
                                    if offset >= offset_to_wait {
                                        return Ok(());
                                    }
                                }
                                return Err(server_error);
                            }
                        }
                    }
                }
            }

            if let Some(server_error) = error_rx.borrow().clone() {
                if self.is_closed.load(Ordering::Relaxed) {
                    return Err(server_error);
                }
            }

            Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                format!("Stream closed during {}", operation_name.to_lowercase()),
            )))
        };

        match tokio::time::timeout(
            Duration::from_millis(self.options.flush_timeout_ms),
            wait_operation,
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => {
                if let Some(stream_id) = self.stream_id.as_deref() {
                    error!(stream_id = %stream_id, table_name = %self.table_properties.table_name, "{} timed out", operation_name);
                } else {
                    error!(table_name = %self.table_properties.table_name, "{} timed out", operation_name);
                }
                Err(ZerobusError::StreamClosedError(
                    tonic::Status::deadline_exceeded(format!("{} timed out", operation_name)),
                ))
            }
        }
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
    ///     let _offset = stream.ingest_record_offset(vec![i as u8]).await?;
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
        let offset_to_wait = match self.logical_offset_id_generator.last() {
            Some(offset) => offset,
            None => return Ok(()), // Nothing to flush.
        };
        self.wait_for_offset_internal(offset_to_wait, "Flush").await
    }

    /// Waits for server acknowledgment of a specific logical offset.
    ///
    /// This method blocks until the server has acknowledged the record or batch at the
    /// specified offset. Use this with offsets returned from `ingest_record_offset()` or
    /// `ingest_records_offset()` to explicitly control when to wait for acknowledgments.
    ///
    /// # Arguments
    ///
    /// * `offset` - The logical offset ID to wait for (returned from `ingest_record_offset()` or `ingest_records_offset()`)
    ///
    /// # Returns
    ///
    /// `Ok(())` when the record/batch at the specified offset has been acknowledged.
    ///
    /// # Errors
    ///
    /// * `StreamClosedError` - If the stream is closed or times out while waiting
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # async fn example(stream: ZerobusStream) -> Result<(), ZerobusError> {
    /// # let my_record = vec![1, 2, 3];
    /// // Ingest multiple records and collect their offsets
    /// let mut offsets = Vec::new();
    /// for i in 0..100 {
    ///     let offset = stream.ingest_record_offset(vec![i as u8]).await?;
    ///     offsets.push(offset);
    /// }
    ///
    /// // Wait for specific offsets
    /// for offset in offsets {
    ///     stream.wait_for_offset(offset).await?;
    /// }
    /// println!("All records acknowledged");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait_for_offset(&self, offset: OffsetId) -> ZerobusResult<()> {
        self.wait_for_offset_internal(offset, "Waiting for acknowledgement")
            .await
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
        let flush_result = self.flush().await;
        self.is_closed.store(true, Ordering::Relaxed);
        self.shutdown_all_tasks_gracefully().await;
        flush_result
    }

    /// Gracefully shuts down the supervisor task.
    ///
    /// Signals cancellation and waits for the task to exit. If the timeout
    /// is provided and expires, forcefully aborts the task.
    async fn shutdown_all_tasks_gracefully(&mut self) {
        self.cancellation_token.cancel();

        // Shutdown supervisor task.
        match tokio::time::timeout(
            Duration::from_secs(SHUTDOWN_TIMEOUT_SECS),
            &mut self.supervisor_task,
        )
        .await
        {
            Ok(_) => {
                debug!("Supervisor task exited gracefully");
            }
            Err(_) => {
                warn!("Supervisor task did not exit within timeout, aborting");
                self.supervisor_task.abort();
            }
        }
        // Shutdown callback handler task, if there are any callbacks.
        if let Some(mut task) = self.callback_handler_task.take() {
            if let Some(callback_max_wait_time_ms) = self.options.callback_max_wait_time_ms {
                match tokio::time::timeout(
                    Duration::from_millis(callback_max_wait_time_ms),
                    &mut task,
                )
                .await
                {
                    Ok(_) => {
                        debug!("Callback handler task exited gracefully");
                    }
                    Err(_) => {
                        debug!("Callback handler task did not exit within timeout, aborting");
                        task.abort();
                    }
                }
            } else {
                debug!("Callback max wait time is not set, waiting indefinitely");
                let _ = (&mut task).await;
            }
        }
    }

    /// Returns all records that were ingested but not acknowledged by the server.
    ///
    /// This method should only be called after a stream has failed or been closed.
    /// It's useful for implementing custom retry logic or persisting failed records.
    ///
    /// **Note:** This method flattens all unacknowledged records into a single iterator,
    /// losing the original batch grouping.
    /// If you want to preserve the batch grouping, use `ZerobusStream::get_unacked_batches()` instead.
    /// If you want to re-ingest unacknowledged records while preserving their batch
    /// structure, use `ZerobusSdk::recreate_stream()` instead.
    ///
    ///
    /// # Returns
    ///
    /// An iterator over individual `EncodedRecord` items. All unacknowledged records are
    /// flattened into a single sequence, regardless of how they were originally ingested
    /// (via `ingest_record()` or `ingest_records()`).
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
    ///         let total_records = unacked.into_iter().count();
    ///         println!("Failed to acknowledge {} records", total_records);
    ///         
    ///         // For re-ingestion with preserved batch structure, use recreate_stream
    ///         let new_stream = sdk.recreate_stream(&stream).await?;
    ///     }
    ///     Ok(_) => println!("All records acknowledged"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_unacked_records(&self) -> ZerobusResult<impl Iterator<Item = EncodedRecord>> {
        Ok(self
            .get_unacked_batches()
            .await?
            .into_iter()
            .flat_map(|batch| batch.into_iter()))
    }

    /// Returns all records that were ingested but not acknowledged by the server, grouped by batch.
    ///
    /// This method should only be called after a stream has failed or been closed.
    /// It's useful for implementing custom retry logic or persisting failed records.
    ///
    /// **Note:** This method returns the unacknowledged records as a vector of `EncodedBatch` items,
    /// where each batch corresponds to how records were ingested:
    /// - Each `ingest_record()` call creates a single batch containing one record
    /// - Each `ingest_records()` call creates a single batch containing multiple records
    ///
    /// For alternatives, see `ZerobusStream::get_unacked_records()` and `ZerobusSdk::recreate_stream()`.
    ///
    /// # Returns
    ///
    /// A vector of `EncodedBatch` items. Records are grouped by their original ingestion call.
    pub async fn get_unacked_batches(&self) -> ZerobusResult<Vec<EncodedBatch>> {
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
