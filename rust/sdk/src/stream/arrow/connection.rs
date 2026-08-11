//! Arrow Flight transport construction and request encoding.
//!
//! A `FlightConnection` owns both halves of one DoPut exchange. Request shutdown
//! is observable so rotation can half-close without dropping the HTTP/2 stream.

use std::future::{pending, Future};
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::task::Poll;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::{FlightClient, FlightData, PutResult};
use futures::{stream::poll_fn, Stream, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio::time::{timeout, timeout_at, Duration, Instant};
use tokio_util::sync::CancellationToken;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tracing::{error, info, warn};

use super::batch::make_ipc_write_options;
use super::metadata::{FlightAckMetadata, FlightBatchMetadata};
use super::{
    configured_deadline, ArrowStreamConfigurationOptions, ArrowTableProperties, RecordBatch,
    ZerobusArrowStream,
};
use crate::errors::ZerobusError;
use crate::headers_provider::HeadersProvider;
use crate::proxy::{self, ConnectorFactory};
use crate::tls_config::TlsConfig;
use crate::ZerobusResult;

pub(super) type FlightResponseStream =
    Pin<Box<dyn Stream<Item = Result<PutResult, FlightError>> + Send>>;
type FlightRequestStream = Pin<Box<dyn Stream<Item = Result<FlightData, FlightError>> + Send>>;

/// Stops one Flight request body without dropping the HTTP/2 stream and reports when
/// tonic has polled that body to EOF.
pub(super) struct RequestBodyControl {
    shutdown: CancellationToken,
    eof_rx: watch::Receiver<bool>,
}

impl RequestBodyControl {
    #[cfg(test)]
    pub(super) fn completed_for_test() -> Self {
        let (_eof_tx, eof_rx) = watch::channel(true);
        Self {
            shutdown: CancellationToken::new(),
            eof_rx,
        }
    }

    pub(super) fn shutdown(&self) {
        self.shutdown.cancel();
    }

    pub(super) async fn wait_for_eof(&self) {
        let mut eof_rx = self.eof_rx.clone();
        loop {
            if *eof_rx.borrow_and_update() {
                return;
            }
            if eof_rx.changed().await.is_err() {
                pending::<()>().await;
            }
        }
    }
}

/// State owned by a single active DoPut connection.
pub(super) struct FlightConnection {
    response_stream: FlightResponseStream,
    batch_tx: mpsc::Sender<Result<RecordBatch, FlightError>>,
    request_body: RequestBodyControl,
}

impl FlightConnection {
    pub(super) fn sender(&self) -> mpsc::Sender<Result<RecordBatch, FlightError>> {
        self.batch_tx.clone()
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        FlightResponseStream,
        mpsc::Sender<Result<RecordBatch, FlightError>>,
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
        endpoint: &str,
        tls_config: &Arc<dyn TlsConfig>,
        connector_factory: Option<&ConnectorFactory>,
        table_properties: &ArrowTableProperties,
        options: &ArrowStreamConfigurationOptions,
        headers_provider: &Arc<dyn HeadersProvider>,
        sdk_identifier: &str,
    ) -> ZerobusResult<FlightConnection> {
        // Share one deadline across connection setup and auth-rejection invalidation.
        // This preserves the original auth error if a custom provider stalls instead of
        // reclassifying the attempt as a retryable setup timeout.
        let attempt_timeout = Duration::from_millis(options.recovery_timeout_ms);
        let attempt_started = Instant::now();
        let attempt_deadline =
            configured_deadline(attempt_started, attempt_timeout, "recovery_timeout_ms")?;
        let result = timeout_at(attempt_deadline, async {
            let client = Self::create_flight_client(
                endpoint,
                tls_config,
                connector_factory,
                table_properties,
                options,
                headers_provider,
                sdk_identifier,
            )
            .await?;

            Self::start_stream_connection(client, table_properties, options).await
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
                    && timeout_at(attempt_deadline, headers_provider.invalidate())
                        .await
                        .is_err()
                {
                    warn!(target: super::LOG_TARGET,
                        timeout_ms = options.recovery_timeout_ms,
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
        batch_rx: mpsc::Receiver<Result<RecordBatch, FlightError>>,
        table_properties: &ArrowTableProperties,
        options: &ArrowStreamConfigurationOptions,
    ) -> ZerobusResult<(FlightRequestStream, RequestBodyControl)> {
        let ipc_write_options = make_ipc_write_options(options.ipc_compression)?;
        let schema = Arc::clone(&table_properties.schema);
        let batch_stream = tokio_stream::wrappers::ReceiverStream::new(batch_rx);
        let offset_counter = Arc::new(AtomicI64::new(0));
        let offset_counter_clone = Arc::clone(&offset_counter);
        let mut encoded: FlightRequestStream = Box::pin(
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
                        }
                        flight_data
                    })
                }),
        );

        let shutdown = CancellationToken::new();
        let mut cancelled = Box::pin(shutdown.clone().cancelled_owned());
        let (eof_tx, eof_rx) = watch::channel(false);
        let controlled = poll_fn(move |cx| {
            if cancelled.as_mut().poll(cx).is_ready() {
                eof_tx.send_replace(true);
                return Poll::Ready(None);
            }
            match encoded.as_mut().poll_next(cx) {
                Poll::Ready(None) => {
                    eof_tx.send_replace(true);
                    Poll::Ready(None)
                }
                result => result,
            }
        });

        Ok((
            Box::pin(controlled),
            RequestBodyControl { shutdown, eof_rx },
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
    ) -> ZerobusResult<FlightConnection> {
        // Create channel for sending RecordBatches.
        let (batch_tx, batch_rx) =
            mpsc::channel::<Result<RecordBatch, FlightError>>(options.max_inflight_batches);

        let (flight_data_stream, request_body) =
            Self::make_request_stream(batch_rx, table_properties, options)?;

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
        endpoint: &str,
        tls_config: &Arc<dyn TlsConfig>,
        connector_factory: Option<&ConnectorFactory>,
        table_properties: &ArrowTableProperties,
        options: &ArrowStreamConfigurationOptions,
        headers_provider: &Arc<dyn HeadersProvider>,
        sdk_identifier: &str,
    ) -> ZerobusResult<FlightConnection> {
        let client = Self::create_flight_client(
            endpoint,
            tls_config,
            connector_factory,
            table_properties,
            options,
            headers_provider,
            sdk_identifier,
        )
        .await?;

        let (batch_tx, batch_rx) =
            mpsc::channel::<Result<RecordBatch, FlightError>>(options.max_inflight_batches);
        let (flight_data_stream, request_body) =
            Self::make_request_stream(batch_rx, table_properties, options)?;

        let mut flight_client = client;
        let mut response_stream = flight_client
            .do_put(flight_data_stream)
            .await
            // `.into()` preserves the inner gRPC code; `Status::from_error` would
            // flatten it to `Unknown` and break auth/retry classification.
            .map_err(|e| ZerobusError::CreateStreamError(e.into()))?;

        let setup_timeout = Duration::from_millis(options.connection_timeout_ms);
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
                    options.connection_timeout_ms
                );
                return Err(ZerobusError::ConnectionTimeout(format!(
                    "Timed out waiting for server reconnect confirmation ({}ms)",
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
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_flight::error::FlightError;
    use async_trait::async_trait;
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};

    use super::super::{ArrowSchema, RecordBatch};
    use super::{
        ArrowStreamConfigurationOptions, ArrowTableProperties, FlightConnection,
        FlightResponseStream, HeadersProvider, RequestBodyControl, TlsConfig, ZerobusArrowStream,
        ZerobusResult,
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
        let (batch_tx, mut batch_rx) = mpsc::channel::<Result<RecordBatch, FlightError>>(1);
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
