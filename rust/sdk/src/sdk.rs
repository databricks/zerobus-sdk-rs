//! The `ZerobusSdk` client — the entry point for creating ingestion streams.
//!
//! This module owns the shared gRPC channel, applies TLS and proxy
//! configuration, and exposes `stream_builder` / `recreate_stream` for
//! consumers. Stream-specific logic lives in `crate::stream`.

use std::sync::Arc;
use std::time::Duration;

use tonic::transport::{Channel, Endpoint};
use tracing::{error, info, instrument};

use crate::databricks::zerobus::zerobus_client::ZerobusClient;
use crate::proxy::{self, ConnectorFactory};
use crate::stream::ZerobusStream;
use crate::{StreamBuilder, TlsConfig, ZerobusError, ZerobusResult, ZerobusSdkBuilder};

#[cfg(feature = "arrow-flight")]
use crate::stream::ZerobusArrowStream;

/// Default identifier the SDK sends as the HTTP `user-agent` header on every
/// request. Use [`ZerobusSdkBuilder::application_name`] to append an
/// application suffix.
pub const DEFAULT_SDK_IDENTIFIER: &str = concat!("zerobus-sdk-rs/", env!("CARGO_PKG_VERSION"));

/// The main interface for interacting with the Zerobus API.
/// # Examples
/// ```rust,ignore
/// // Create SDK using the builder
/// let sdk = ZerobusSdk::builder()
///     .endpoint("https://your-workspace.zerobus.region.cloud.databricks.com")
///     .unity_catalog_url("https://your-workspace.cloud.databricks.com")
///     .build()?;
///
/// // Create a stream via the stream builder
/// let stream = sdk
///     .stream_builder()
///     .table("catalog.schema.table")
///     .oauth("client-id", "client-secret")
///     .compiled_proto(descriptor_proto)
///     .max_inflight_requests(100)
///     .build()
///     .await?;
///
/// // Queue records, then confirm durability once
/// let offset_id = stream.ingest_record_offset(ProtoMessage(row)).await?;
/// stream.flush().await?;
/// ```
#[non_exhaustive]
pub struct ZerobusSdk {
    pub zerobus_endpoint: String,
    pub unity_catalog_url: String,
    shared_channel: tokio::sync::Mutex<Option<ZerobusClient<Channel>>>,
    pub(crate) workspace_id: String,
    pub(crate) tls_config: Arc<dyn TlsConfig>,
    pub(crate) connector_factory: Option<ConnectorFactory>,
    /// Final value sent as the HTTP `user-agent` header on every request.
    /// Either `"zerobus-sdk-rs/<version>"` or `"zerobus-sdk-rs/<version> <application_name>"`.
    pub(crate) sdk_identifier: Arc<str>,
    /// Shared cache of OAuth tokens, keyed per table, reused across all streams
    /// created from this SDK instance via the default OAuth path.
    pub(crate) token_cache: Arc<crate::token_cache::TokenCache>,
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

    /// Creates a new stream builder for configuring an ingestion stream.
    ///
    /// All setters can be called in any order. The builder validates at
    /// `build()` time that table name, authentication, and format have
    /// been configured. Use [`StreamBuilder::validate()`] to check the
    /// configuration without opening a stream.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // JSON stream with OAuth
    /// let stream = sdk
    ///     .stream_builder()
    ///     .table("catalog.schema.table")
    ///     .oauth("client-id", "client-secret")
    ///     .json()
    ///     .build()
    ///     .await?;
    ///
    /// // Proto stream with custom headers
    /// let stream = sdk
    ///     .stream_builder()
    ///     .table("catalog.schema.table")
    ///     .headers_provider(my_provider)
    ///     .compiled_proto(descriptor)
    ///     .max_inflight_requests(500_000)
    ///     .build()
    ///     .await?;
    /// ```
    pub fn stream_builder(&self) -> StreamBuilder<'_> {
        StreamBuilder::new(self)
    }

    /// Creates a new SDK instance with explicit configuration.
    ///
    /// This is used internally by the builder pattern. `sdk_identifier` is the
    /// fully-resolved value sent as the HTTP `user-agent` header; the builder
    /// is responsible for composing it from the default prefix and any
    /// caller-supplied `application_name` or override.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_config(
        zerobus_endpoint: String,
        unity_catalog_url: String,
        workspace_id: String,
        tls_config: Arc<dyn TlsConfig>,
        connector_factory: Option<ConnectorFactory>,
        sdk_identifier: Arc<str>,
        token_cache_enabled: bool,
        token_refresh_buffer: Duration,
    ) -> Self {
        ZerobusSdk {
            zerobus_endpoint,
            unity_catalog_url,
            workspace_id,
            shared_channel: tokio::sync::Mutex::new(None),
            tls_config,
            connector_factory,
            sdk_identifier,
            token_cache: Arc::new(crate::token_cache::TokenCache::new(
                token_cache_enabled,
                token_refresh_buffer,
            )),
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
        let channel = self.get_or_create_channel_zerobus_client().await?;
        let new_stream = ZerobusStream::new_stream(
            channel,
            stream.table_properties.clone(),
            Arc::clone(&stream.headers_provider),
            stream.options.clone(),
        )
        .await;

        match new_stream {
            Ok(new_stream) => {
                if let Some(stream_id) = new_stream.stream_id.as_ref() {
                    info!(stream_id = %stream_id, "Successfully recreated ephemeral stream");
                } else {
                    error!("Successfully recreated a stream but stream_id is None");
                }

                for batch in batches {
                    let ack = new_stream.ingest_internal(batch).await?;
                    tokio::spawn(ack);
                }

                Ok(new_stream)
            }
            Err(e) => {
                error!("Stream recreation failed with error: {}", e);
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

        let new_stream = ZerobusArrowStream::new(
            &self.zerobus_endpoint,
            Arc::clone(&self.tls_config),
            self.connector_factory.clone(),
            stream.table_properties.clone(),
            stream.headers_provider(),
            stream.options().clone(),
            Arc::clone(&self.sdk_identifier),
        )
        .await;

        match new_stream {
            Ok(new_stream) => {
                info!(
                    table_name = %new_stream.table_name(),
                    "Successfully recreated Arrow Flight stream"
                );

                for batch in batches {
                    let _offset = new_stream.ingest_batch(batch).await?;
                }

                Ok(new_stream)
            }
            Err(e) => {
                error!("Arrow Flight stream recreation failed: {}", e);
                Err(e)
            }
        }
    }

    /// Gets or creates the shared Channel for all streams.
    /// The first call creates the Channel, subsequent calls clone it.
    /// All clones share the same underlying TCP connection via HTTP/2 multiplexing.
    pub(crate) async fn get_or_create_channel_zerobus_client(
        &self,
    ) -> ZerobusResult<ZerobusClient<Channel>> {
        let mut guard = self.shared_channel.lock().await;

        if guard.is_none() {
            // Create the channel for the first time.
            let endpoint = Endpoint::from_shared(self.zerobus_endpoint.clone())
                .map_err(|err| ZerobusError::ChannelCreationError(err.to_string()))?
                .user_agent(self.sdk_identifier.as_ref())
                .map_err(|err| ZerobusError::ChannelCreationError(err.to_string()))?;

            let endpoint = self.tls_config.configure_endpoint(endpoint)?;

            let host = endpoint.uri().host().unwrap_or_default().to_string();
            let proxy_connector = proxy::resolve_connector(&host, self.connector_factory.as_ref())?;

            let channel = match proxy_connector {
                Some(pc) => endpoint.connect_with_connector_lazy(pc),
                None => endpoint.connect_lazy(),
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
