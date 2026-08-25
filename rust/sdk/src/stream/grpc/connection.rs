//! gRPC stream connection setup.
//!
//! This module is transport-specific: it opens the bidirectional gRPC stream
//! used by `ZerobusStream`. It handles both stream kinds through the transport
//! seam (`super::transport`): ephemeral streams over the `EphemeralStream` RPC
//! and, behind the `eos` feature, persistent streams over `PersistentStream`.
//! The Arrow Flight transport has its own equivalent under `stream/arrow/`.

use std::sync::Arc;

use prost::Message;
use tokio::time::Duration;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tracing::{debug, error, info, instrument, warn};

use super::supervisor::StreamInitInfo;
use super::transport::{self, InboundStream, Opened, OutboundSink, TransportKind};
use crate::databricks::zerobus::zerobus_client::ZerobusClient;
use crate::databricks::zerobus::{CreateIngestStreamRequest, RecordType};
use crate::{HeadersProvider, TableProperties, ZerobusError, ZerobusResult};

/// A freshly opened stream connection: the outbound sink, the inbound response
/// stream, the server-assigned `stream_id`, and (persistent resume only) the
/// committed-offset watermark to resume from.
pub(super) struct StreamConnection {
    pub(super) sink: OutboundSink,
    pub(super) inbound: InboundStream,
    pub(super) init_info: StreamInitInfo,
}

impl super::ZerobusStream {
    /// Opens a stream connection to the Zerobus API for the given transport
    /// kind. Returns the sink, inbound stream, stream id, and resume watermark.
    ///
    /// On a server-side authentication rejection it asks the headers provider
    /// to invalidate cached credentials so the next attempt re-derives them.
    /// This covers IdP-revoked tokens, not a same-named table recreated within
    /// the token's lifetime, which the server accepts.
    pub(super) async fn create_stream_connection(
        channel: ZerobusClient<Channel>,
        table_properties: &TableProperties,
        headers_provider: &Arc<dyn HeadersProvider>,
        record_type: RecordType,
        kind: &TransportKind,
    ) -> ZerobusResult<StreamConnection> {
        let result = Self::create_stream_connection_inner(
            channel,
            table_properties,
            headers_provider,
            record_type,
            kind,
        )
        .await;
        if let Err(err) = &result {
            if err.is_auth_rejection() {
                headers_provider.invalidate().await;
            }
        }
        result
    }

    /// Initial-setup variant of [`create_stream_connection`] that bounds both the
    /// connection and the post-rejection credential invalidation under a single
    /// `recovery_timeout_ms` deadline.
    ///
    /// This preserves the original auth rejection if a custom provider stalls in
    /// `invalidate()`, so a stalled provider cannot turn a known auth rejection into a
    /// retryable `DeadlineExceeded` and thereby bypass the one-shot auth-retry limit.
    /// Reconnect keeps using [`create_stream_connection`] so its behavior is unchanged.
    pub(super) async fn create_initial_stream_connection(
        channel: ZerobusClient<Channel>,
        table_properties: &TableProperties,
        headers_provider: &Arc<dyn HeadersProvider>,
        record_type: RecordType,
        kind: &TransportKind,
        recovery_timeout_ms: u64,
    ) -> ZerobusResult<StreamConnection> {
        let attempt_timeout = Duration::from_millis(recovery_timeout_ms);
        let attempt_started = tokio::time::Instant::now();
        let result = tokio::time::timeout(
            attempt_timeout,
            Self::create_stream_connection_inner(
                channel,
                table_properties,
                headers_provider,
                record_type,
                kind,
            ),
        )
        .await
        .map_err(|_| {
            ZerobusError::CreateStreamError(tonic::Status::deadline_exceeded(
                "Stream creation timed out",
            ))
        })?;

        if let Err(err) = &result {
            let invalidate_timeout = attempt_timeout.saturating_sub(attempt_started.elapsed());
            if err.is_auth_rejection()
                && tokio::time::timeout(invalidate_timeout, headers_provider.invalidate())
                    .await
                    .is_err()
            {
                warn!(
                    timeout_ms = recovery_timeout_ms,
                    "Initial headers provider invalidation timed out; preserving auth rejection"
                );
            }
        }
        result
    }

    #[instrument(level = "debug", skip_all, fields(table_name = %table_properties.table_name))]
    async fn create_stream_connection_inner(
        mut channel: ZerobusClient<Channel>,
        table_properties: &TableProperties,
        headers_provider: &Arc<dyn HeadersProvider>,
        record_type: RecordType,
        kind: &TransportKind,
    ) -> ZerobusResult<StreamConnection> {
        let (sink, request_body) = transport::make_outbound(kind);
        let mut request = tonic::Request::new(request_body);

        let stream_metadata = request.metadata_mut();
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
                        error!(table_name = %table_properties.table_name, "authorization token is not a valid HTTP header value");
                        ZerobusError::InvalidUCTokenError(
                            "authorization token is not a valid HTTP header value".to_string(),
                        )
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

        let mut inbound = transport::open_rpc(&mut channel, request).await?;

        let create_request = Self::build_create_request(table_properties, record_type)?;

        debug!("Sending stream-open request.");
        sink.send_open(kind, create_request).await.map_err(|_| {
            error!(table_name = %table_properties.table_name, "Failed to send stream-open request");
            ZerobusError::StreamClosedError(tonic::Status::internal(
                "Failed to send stream-open request",
            ))
        })?;

        debug!("Waiting for stream-open response.");
        let opened = inbound.recv_open().await?;

        let init_info = Self::validate_open_response(kind, opened)?;
        info!(stream_id = %init_info.stream_id, last_committed_offset = ?init_info.last_committed_offset, "Successfully opened stream");

        Ok(StreamConnection {
            sink,
            inbound,
            init_info,
        })
    }

    fn validate_open_response(
        kind: &TransportKind,
        opened: Opened,
    ) -> ZerobusResult<StreamInitInfo> {
        let init_info = match (kind, opened) {
            (TransportKind::Ephemeral, Opened::Created { stream_id }) => StreamInitInfo {
                stream_id,
                last_committed_offset: None,
            },
            #[cfg(feature = "eos")]
            (
                TransportKind::Persistent {
                    resume_stream_id: None,
                },
                Opened::Created { stream_id },
            ) => StreamInitInfo {
                stream_id,
                last_committed_offset: None,
            },
            #[cfg(feature = "eos")]
            (
                TransportKind::Persistent {
                    resume_stream_id: Some(stream_id),
                },
                Opened::Resumed {
                    last_committed_offset,
                },
            ) => StreamInitInfo {
                stream_id: stream_id.clone(),
                last_committed_offset,
            },
            #[cfg(feature = "eos")]
            _ => {
                return Err(ZerobusError::UnexpectedStreamResponseError(
                    "Persistent stream setup response did not match the requested operation"
                        .to_string(),
                ));
            }
        };
        Ok(init_info)
    }

    /// Builds the `CreateIngestStreamRequest` sent when opening (or, on the
    /// persistent path, creating) a stream. Encodes the descriptor for proto
    /// streams and validates its presence.
    fn build_create_request(
        table_properties: &TableProperties,
        record_type: RecordType,
    ) -> ZerobusResult<CreateIngestStreamRequest> {
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

        Ok(CreateIngestStreamRequest {
            table_name: Some(table_properties.table_name.to_string()),
            descriptor_proto,
            record_type: Some(record_type.into()),
        })
    }
}

#[cfg(all(test, feature = "eos"))]
mod tests {
    use super::*;
    use crate::ZerobusStream;

    #[test]
    fn persistent_create_rejects_resume_response() {
        let kind = TransportKind::Persistent {
            resume_stream_id: None,
        };
        let opened = Opened::Resumed {
            last_committed_offset: Some(3),
        };
        assert!(ZerobusStream::validate_open_response(&kind, opened).is_err());
    }

    #[test]
    fn persistent_resume_rejects_create_response() {
        let kind = TransportKind::Persistent {
            resume_stream_id: Some("stream-id".to_string()),
        };
        let opened = Opened::Created {
            stream_id: "other-id".to_string(),
        };
        assert!(ZerobusStream::validate_open_response(&kind, opened).is_err());
    }
}
