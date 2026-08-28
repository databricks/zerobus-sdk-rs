//! gRPC stream connection setup.
//!
//! This module is transport-specific: it opens the bidirectional gRPC stream
//! used by `ZerobusStream`. It handles both stream kinds through the transport
//! seam (`super::transport`): ephemeral streams over the `EphemeralStream` RPC
//! and persistent streams over `PersistentStream`.
//! The Arrow Flight transport has its own equivalent under `stream/arrow/`.

use std::sync::Arc;

use prost::Message;
use tokio::time::Duration;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tracing::{debug, error, info, instrument, warn};

use super::supervisor::StreamInitInfo;
use super::transport::{
    self, GrpcConnectionMode, InboundStream, Opened, OutboundSink, StreamOpenParams,
};
use crate::databricks::zerobus::zerobus_client::ZerobusClient;
use crate::databricks::zerobus::RecordType;
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
        kind: &GrpcConnectionMode,
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
        kind: &GrpcConnectionMode,
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
        kind: &GrpcConnectionMode,
    ) -> ZerobusResult<StreamConnection> {
        let outbound = transport::make_outbound(kind);
        let mut stream_metadata = tonic::metadata::MetadataMap::new();
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

        let (sink, mut inbound) =
            transport::open_rpc(outbound, &mut channel, stream_metadata).await?;

        let open_params = Self::build_open_params(table_properties, record_type)?;

        debug!("Sending stream-open request.");
        sink.send_open(open_params).await.map_err(|_| {
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
        kind: &GrpcConnectionMode,
        opened: Opened,
    ) -> ZerobusResult<StreamInitInfo> {
        match kind {
            GrpcConnectionMode::Ephemeral => match opened {
                Opened::Created { stream_id } => Ok(StreamInitInfo {
                    stream_id,
                    last_committed_offset: None,
                }),
                _ => Err(Self::mismatched_open_response()),
            },
            GrpcConnectionMode::Persistent { resume_stream_id } => {
                Self::validate_persistent_open_response(resume_stream_id.as_deref(), opened)
            }
        }
    }

    fn validate_persistent_open_response(
        resume_stream_id: Option<&str>,
        opened: Opened,
    ) -> ZerobusResult<StreamInitInfo> {
        match (resume_stream_id, opened) {
            (None, Opened::Created { stream_id }) => Ok(StreamInitInfo {
                stream_id,
                last_committed_offset: None,
            }),
            (
                Some(stream_id),
                Opened::Resumed {
                    last_committed_offset,
                },
            ) => Ok(StreamInitInfo {
                stream_id: stream_id.to_string(),
                last_committed_offset,
            }),
            _ => Err(Self::mismatched_open_response()),
        }
    }

    fn mismatched_open_response() -> ZerobusError {
        ZerobusError::UnexpectedStreamResponseError(
            "Persistent stream setup response did not match the requested operation".to_string(),
        )
    }

    /// Resolves the schema inputs used to construct either a create or resume
    /// opening message. Encodes and validates the descriptor for proto streams.
    fn build_open_params(
        table_properties: &TableProperties,
        record_type: RecordType,
    ) -> ZerobusResult<StreamOpenParams> {
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

        Ok(StreamOpenParams {
            table_name: table_properties.table_name.to_string(),
            descriptor_proto,
            record_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ZerobusStream;

    #[test]
    fn persistent_create_accepts_create_response() {
        let kind = GrpcConnectionMode::Persistent {
            resume_stream_id: None,
        };
        let opened = Opened::Created {
            stream_id: "created-id".to_string(),
        };
        let init = ZerobusStream::validate_open_response(&kind, opened).unwrap();
        assert_eq!(init.stream_id, "created-id");
        assert_eq!(init.last_committed_offset, None);
    }

    #[test]
    fn persistent_resume_accepts_resume_response() {
        let kind = GrpcConnectionMode::Persistent {
            resume_stream_id: Some("stream-id".to_string()),
        };
        let opened = Opened::Resumed {
            last_committed_offset: Some(3),
        };
        let init = ZerobusStream::validate_open_response(&kind, opened).unwrap();
        assert_eq!(init.stream_id, "stream-id");
        assert_eq!(init.last_committed_offset, Some(3));
    }

    #[test]
    fn persistent_create_rejects_resume_response() {
        let kind = GrpcConnectionMode::Persistent {
            resume_stream_id: None,
        };
        let opened = Opened::Resumed {
            last_committed_offset: Some(3),
        };
        assert!(ZerobusStream::validate_open_response(&kind, opened).is_err());
    }

    #[test]
    fn persistent_resume_rejects_create_response() {
        let kind = GrpcConnectionMode::Persistent {
            resume_stream_id: Some("stream-id".to_string()),
        };
        let opened = Opened::Created {
            stream_id: "other-id".to_string(),
        };
        assert!(ZerobusStream::validate_open_response(&kind, opened).is_err());
    }
}
