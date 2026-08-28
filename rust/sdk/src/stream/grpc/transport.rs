//! Transport seam between the ephemeral and persistent gRPC stream kinds.
//!
//! The two stream kinds ride different RPCs (`EphemeralStream` vs
//! `PersistentStream`) with different request/response envelopes, and differ
//! in exactly three ways:
//!
//! 1. **Opening** — ephemeral always sends `create_stream`; persistent sends
//!    `create_stream` (new) or `resume_stream` (reconnect), and a resume comes
//!    back with a committed-offset watermark.
//! 2. **Offset on the wire** — persistent SDK and wire offsets match exactly.
//!    Ephemeral uses a fresh 0-based wire offset for each server stream while
//!    its SDK offset preserves continuity across recovery.
//! 3. **Response parsing** — the oneof variants live in different generated
//!    enums.
//!
//! Everything else — the landing zone, backpressure, ack tracking, flush,
//! close, callbacks, and the create → spawn → recover supervisor loop — is
//! identical and stays transport-agnostic. This module normalizes the three
//! differences so the sender/receiver tasks never mention a concrete RPC.

use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use crate::databricks::zerobus::ephemeral_stream_request::Payload as EphemeralRequestPayload;
use crate::databricks::zerobus::ephemeral_stream_response::Payload as EphemeralResponsePayload;
use crate::databricks::zerobus::zerobus_client::ZerobusClient;
use crate::databricks::zerobus::{
    CloseStreamSignal, CreateIngestStreamRequest, EphemeralStreamRequest, EphemeralStreamResponse,
    IngestRecordResponse, RecordType,
};
use crate::{EncodedBatch, OffsetId, OffsetIdGenerator, ZerobusError, ZerobusResult};

use crate::databricks::zerobus::persistent_stream_request::Payload as PersistentRequestPayload;
use crate::databricks::zerobus::persistent_stream_response::Payload as PersistentResponsePayload;
use crate::databricks::zerobus::resume_ingest_stream_request::Identifier as ResumeIdentifier;
use crate::databricks::zerobus::{
    CreatePersistentStreamRequest, PersistentStreamRequest, PersistentStreamResponse,
    ResumeIngestStreamRequest,
};

/// Buffer size for the outbound request channel handed to tonic.
pub(super) const CHANNEL_BUFFER_SIZE: usize = 2048;

/// Which kind of stream a connection serves. Selects the RPC, the offset
/// policy on the wire, and (for persistent) whether the first message is a
/// create or a resume.
#[derive(Clone)]
pub(super) enum GrpcConnectionMode {
    /// Ephemeral stream: `EphemeralStream` RPC, per-session 0-based wire offsets.
    Ephemeral,
    /// Persistent (Eos) stream: `PersistentStream` RPC, durable wire offsets.
    /// `resume_stream_id` is `Some` when reconnecting to an existing stream and
    /// `None` when creating a new one.
    // Constructed by the feature-gated public API introduced in the downstream PR.
    #[allow(dead_code)]
    Persistent { resume_stream_id: Option<String> },
}

/// Outcome of opening a connection, preserving which operation the server
/// acknowledged so the caller can reject mismatched setup responses.
pub(super) enum Opened {
    Created {
        stream_id: String,
    },
    Resumed {
        last_committed_offset: Option<OffsetId>,
    },
}

/// Schema and destination inputs needed to construct the protocol-specific
/// opening message.
pub(super) struct StreamOpenParams {
    pub(super) table_name: String,
    pub(super) descriptor_proto: Option<Vec<u8>>,
    pub(super) record_type: RecordType,
}

impl StreamOpenParams {
    fn into_create_request(self) -> CreateIngestStreamRequest {
        CreateIngestStreamRequest {
            table_name: Some(self.table_name),
            descriptor_proto: self.descriptor_proto,
            record_type: Some(self.record_type.into()),
        }
    }
}

/// The outbound half of a stream: wraps the concrete tonic request sender and
/// exposes a single neutral `send` that both IO tasks use.
pub(super) enum OutboundSink {
    Ephemeral {
        tx: tokio::sync::mpsc::Sender<EphemeralStreamRequest>,
        wire_offsets: OffsetIdGenerator,
    },
    Persistent {
        tx: tokio::sync::mpsc::Sender<PersistentStreamRequest>,
        resume_stream_id: Option<String>,
    },
}

impl OutboundSink {
    /// Sends the opening message: `create_stream` for a new stream (ephemeral
    /// or persistent), or `resume_stream` when reconnecting to a persistent
    /// one. Create carries the destination table; resume carries only the
    /// descriptor and record type needed to validate the reopened stream.
    pub(super) async fn send_open(&self, params: StreamOpenParams) -> ZerobusResult<()> {
        match self {
            OutboundSink::Ephemeral { tx, .. } => tx
                .send(EphemeralStreamRequest {
                    payload: Some(EphemeralRequestPayload::CreateStream(
                        params.into_create_request(),
                    )),
                })
                .await
                .map_err(|_| Self::open_failed()),
            OutboundSink::Persistent {
                tx,
                resume_stream_id,
            } => {
                let payload = match resume_stream_id {
                    None => PersistentRequestPayload::CreateStream(CreatePersistentStreamRequest {
                        create_stream: Some(params.into_create_request()),
                    }),
                    Some(stream_id) => {
                        PersistentRequestPayload::ResumeStream(ResumeIngestStreamRequest {
                            identifier: Some(ResumeIdentifier::StreamId(stream_id.clone())),
                            descriptor_proto: params.descriptor_proto,
                            record_type: Some(params.record_type.into()),
                        })
                    }
                };
                tx.send(PersistentStreamRequest {
                    payload: Some(payload),
                })
                .await
                .map_err(|_| Self::open_failed())
            }
        }
    }

    fn open_failed() -> ZerobusError {
        ZerobusError::StreamClosedError(tonic::Status::internal(
            "Failed to send stream-open request",
        ))
    }

    /// Sends one ingest batch, mapping the SDK offset to the wire sequence for
    /// this connection. Persistent offsets pass through unchanged; ephemeral
    /// connections use a fresh zero-based sequence.
    pub(super) async fn send_ingest(
        &self,
        batch: EncodedBatch,
        sdk_offset: OffsetId,
    ) -> ZerobusResult<()> {
        let wire_offset = self.next_wire_offset(sdk_offset);
        match self {
            OutboundSink::Ephemeral { tx, .. } => {
                let payload = batch.into_request_payload(wire_offset);
                tx.send(EphemeralStreamRequest {
                    payload: Some(payload),
                })
                .await
                .map_err(|_| Self::ingest_failed())
            }
            OutboundSink::Persistent { tx, .. } => {
                let payload = batch.into_persistent_request_payload(wire_offset);
                tx.send(PersistentStreamRequest {
                    payload: Some(payload),
                })
                .await
                .map_err(|_| Self::ingest_failed())
            }
        }
    }

    fn ingest_failed() -> ZerobusError {
        ZerobusError::StreamClosedError(tonic::Status::internal("Failed to send batch"))
    }

    fn next_wire_offset(&self, sdk_offset: OffsetId) -> OffsetId {
        match self {
            OutboundSink::Ephemeral { wire_offsets, .. } => wire_offsets.next(),
            OutboundSink::Persistent { .. } => sdk_offset,
        }
    }
}

/// A server message, normalized across the two response envelopes. Both IO
/// tasks match on this instead of the concrete generated oneof.
pub(super) enum InboundMessage {
    /// A durability acknowledgement carrying the highest acked wire offset.
    Ack(IngestRecordResponse),
    /// The server will close the current session after the given duration.
    Close(CloseStreamSignal),
    /// Any other payload (create/resume responses arrive only while opening and
    /// are handled there; seeing one mid-stream is unexpected).
    Other,
}

/// The inbound half of a stream: wraps the concrete tonic response stream and
/// yields normalized `InboundMessage`s.
pub(super) enum InboundStream {
    Ephemeral(tonic::Streaming<EphemeralStreamResponse>),
    Persistent(tonic::Streaming<PersistentStreamResponse>),
}

impl InboundStream {
    /// Reads the next server message. `Ok(None)` means the server closed the
    /// stream gracefully; `Err` is a transport error.
    pub(super) async fn message(&mut self) -> Result<Option<InboundMessage>, tonic::Status> {
        match self {
            InboundStream::Ephemeral(s) => Ok(s.message().await?.map(|resp| match resp.payload {
                Some(EphemeralResponsePayload::IngestRecordResponse(ack)) => {
                    InboundMessage::Ack(ack)
                }
                Some(EphemeralResponsePayload::CloseStreamSignal(sig)) => {
                    InboundMessage::Close(sig)
                }
                _ => InboundMessage::Other,
            })),
            InboundStream::Persistent(s) => Ok(s.message().await?.map(|resp| match resp.payload {
                Some(PersistentResponsePayload::IngestRecordResponse(ack)) => {
                    InboundMessage::Ack(ack)
                }
                Some(PersistentResponsePayload::CloseStreamSignal(sig)) => {
                    InboundMessage::Close(sig)
                }
                _ => InboundMessage::Other,
            })),
        }
    }

    /// Reads and interprets the first server message after opening.
    ///
    /// For a create (ephemeral or persistent) this is a `CreateStreamResponse`
    /// carrying the minted `stream_id`. For a persistent resume it is a
    /// `ResumeStreamResponse` carrying the committed-offset watermark; the
    /// `stream_id` is already known to the caller, so `Opened.stream_id` is
    /// left empty on that path and filled in by the caller.
    pub(super) async fn recv_open(&mut self) -> ZerobusResult<Opened> {
        match self {
            InboundStream::Ephemeral(s) => {
                let msg = Self::first_message(s.message().await)?;
                match msg.payload {
                    Some(EphemeralResponsePayload::CreateStreamResponse(resp)) => {
                        let stream_id = resp.stream_id.ok_or_else(Self::missing_stream_id)?;
                        Ok(Opened::Created { stream_id })
                    }
                    other => Err(Self::unexpected_open(&other)),
                }
            }
            InboundStream::Persistent(s) => {
                let msg = Self::first_message(s.message().await)?;
                match msg.payload {
                    Some(PersistentResponsePayload::CreateStreamResponse(resp)) => {
                        let stream_id = resp.stream_id.ok_or_else(Self::missing_stream_id)?;
                        Ok(Opened::Created { stream_id })
                    }
                    Some(PersistentResponsePayload::ResumeStreamResponse(resp)) => {
                        Ok(Opened::Resumed {
                            last_committed_offset: resp.last_committed_offset,
                        })
                    }
                    other => Err(Self::unexpected_open(&other)),
                }
            }
        }
    }

    fn first_message<T>(result: Result<Option<T>, tonic::Status>) -> ZerobusResult<T> {
        match result {
            Ok(Some(msg)) => Ok(msg),
            Ok(None) => Err(ZerobusError::CreateStreamError(tonic::Status::ok(
                "Stream closed gracefully by server",
            ))),
            Err(status) => Err(ZerobusError::CreateStreamError(status)),
        }
    }

    fn missing_stream_id() -> ZerobusError {
        ZerobusError::CreateStreamError(tonic::Status::internal(
            "Successfully opened a stream but stream_id is None",
        ))
    }

    fn unexpected_open<T: std::fmt::Debug>(payload: &T) -> ZerobusError {
        ZerobusError::CreateStreamError(tonic::Status::internal(format!(
            "Unexpected response from server while opening stream: {payload:?}"
        )))
    }

    /// Drains and discards remaining server messages during teardown so the
    /// server observes END_STREAM instead of a client RST_STREAM.
    pub(super) async fn drain(&mut self) {
        match self {
            InboundStream::Ephemeral(s) => while matches!(s.message().await, Ok(Some(_))) {},
            InboundStream::Persistent(s) => while matches!(s.message().await, Ok(Some(_))) {},
        }
    }
}

/// Both halves of a newly allocated outbound channel. Keeping them in the same
/// variant guarantees the request stream, retained sender, and opening mode
/// cannot disagree.
pub(super) enum OutboundConnection {
    Ephemeral {
        tx: tokio::sync::mpsc::Sender<EphemeralStreamRequest>,
        requests: ReceiverStream<EphemeralStreamRequest>,
    },
    Persistent {
        tx: tokio::sync::mpsc::Sender<PersistentStreamRequest>,
        requests: ReceiverStream<PersistentStreamRequest>,
        resume_stream_id: Option<String>,
    },
}

pub(super) fn make_outbound(kind: &GrpcConnectionMode) -> OutboundConnection {
    match kind {
        GrpcConnectionMode::Ephemeral => {
            let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_BUFFER_SIZE);
            OutboundConnection::Ephemeral {
                tx,
                requests: ReceiverStream::new(rx),
            }
        }
        GrpcConnectionMode::Persistent { resume_stream_id } => {
            let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_BUFFER_SIZE);
            OutboundConnection::Persistent {
                tx,
                requests: ReceiverStream::new(rx),
                resume_stream_id: resume_stream_id.clone(),
            }
        }
    }
}

/// Dispatches the opening RPC with metadata prepared by the caller and returns
/// both normalized connection halves.
pub(super) async fn open_rpc(
    outbound: OutboundConnection,
    channel: &mut ZerobusClient<Channel>,
    metadata: tonic::metadata::MetadataMap,
) -> ZerobusResult<(OutboundSink, InboundStream)> {
    match outbound {
        OutboundConnection::Ephemeral { tx, requests } => {
            let mut req = tonic::Request::new(requests);
            *req.metadata_mut() = metadata;
            let resp = channel
                .ephemeral_stream(req)
                .await
                .map_err(ZerobusError::CreateStreamError)?;
            Ok((
                OutboundSink::Ephemeral {
                    tx,
                    wire_offsets: OffsetIdGenerator::default(),
                },
                InboundStream::Ephemeral(resp.into_inner()),
            ))
        }
        OutboundConnection::Persistent {
            tx,
            requests,
            resume_stream_id,
        } => {
            let mut req = tonic::Request::new(requests);
            *req.metadata_mut() = metadata;
            let resp = channel
                .persistent_stream(req)
                .await
                .map_err(ZerobusError::CreateStreamError)?;
            Ok((
                OutboundSink::Persistent {
                    tx,
                    resume_stream_id,
                },
                InboundStream::Persistent(resp.into_inner()),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ephemeral_wire_offsets_restart_for_each_connection() {
        let (tx, _rx) = tokio::sync::mpsc::channel(CHANNEL_BUFFER_SIZE);
        let sink = OutboundSink::Ephemeral {
            tx,
            wire_offsets: OffsetIdGenerator::default(),
        };
        assert_eq!(sink.next_wire_offset(40), 0);
        assert_eq!(sink.next_wire_offset(41), 1);

        let (tx, _rx) = tokio::sync::mpsc::channel(CHANNEL_BUFFER_SIZE);
        let recovered = OutboundSink::Ephemeral {
            tx,
            wire_offsets: OffsetIdGenerator::default(),
        };
        assert_eq!(recovered.next_wire_offset(42), 0);
    }

    #[test]
    fn persistent_wire_offsets_match_sdk_offsets() {
        let (tx, _rx) = tokio::sync::mpsc::channel(CHANNEL_BUFFER_SIZE);
        let sink = OutboundSink::Persistent {
            tx,
            resume_stream_id: Some("stream-id".to_string()),
        };
        assert_eq!(sink.next_wire_offset(40), 40);
        assert_eq!(sink.next_wire_offset(41), 41);
    }
}
