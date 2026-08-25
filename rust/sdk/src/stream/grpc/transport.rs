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
//! differences behind two small enums (`OutboundSink`, `InboundStream`) plus a
//! neutral `InboundMessage`, so the sender/receiver tasks never mention a
//! concrete RPC. The enums carry no `dyn` and compile away to the ephemeral
//! arm when the `eos` feature is off.

use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use crate::databricks::zerobus::ephemeral_stream_request::Payload as EphemeralRequestPayload;
use crate::databricks::zerobus::ephemeral_stream_response::Payload as EphemeralResponsePayload;
use crate::databricks::zerobus::zerobus_client::ZerobusClient;
use crate::databricks::zerobus::{
    CloseStreamSignal, CreateIngestStreamRequest, EphemeralStreamRequest, EphemeralStreamResponse,
    IngestRecordResponse,
};
use crate::{EncodedBatch, OffsetId, ZerobusError, ZerobusResult};

#[cfg(feature = "eos")]
use crate::databricks::zerobus::persistent_stream_request::Payload as PersistentRequestPayload;
#[cfg(feature = "eos")]
use crate::databricks::zerobus::persistent_stream_response::Payload as PersistentResponsePayload;
#[cfg(feature = "eos")]
use crate::databricks::zerobus::resume_ingest_stream_request::Identifier as ResumeIdentifier;
#[cfg(feature = "eos")]
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
    #[cfg(feature = "eos")]
    Persistent { resume_stream_id: Option<String> },
}

impl GrpcConnectionMode {
    /// Whether SDK and wire offsets match. They match for persistent streams;
    /// ephemeral streams use a fresh wire sequence for each server stream.
    pub(super) fn wire_offsets_match(&self) -> bool {
        match self {
            GrpcConnectionMode::Ephemeral => false,
            #[cfg(feature = "eos")]
            GrpcConnectionMode::Persistent { .. } => true,
        }
    }
}

/// Outcome of opening a connection, preserving which operation the server
/// acknowledged so the caller can reject mismatched setup responses.
pub(super) enum Opened {
    Created {
        stream_id: String,
    },
    #[cfg(feature = "eos")]
    Resumed {
        last_committed_offset: Option<OffsetId>,
    },
}

/// The outbound half of a stream: wraps the concrete tonic request sender and
/// exposes a single neutral `send` that both IO tasks use.
pub(super) enum OutboundSink {
    Ephemeral(tokio::sync::mpsc::Sender<EphemeralStreamRequest>),
    #[cfg(feature = "eos")]
    Persistent(tokio::sync::mpsc::Sender<PersistentStreamRequest>),
}

impl OutboundSink {
    /// Sends the opening message: `create_stream` for a new stream (ephemeral
    /// or persistent), or `resume_stream` when reconnecting to a persistent
    /// one. `create` carries the table name, descriptor, and record type; it is
    /// ignored on the resume path (the server keeps that state from creation).
    pub(super) async fn send_open(
        &self,
        kind: &GrpcConnectionMode,
        create: CreateIngestStreamRequest,
    ) -> ZerobusResult<()> {
        match (self, kind) {
            (OutboundSink::Ephemeral(tx), GrpcConnectionMode::Ephemeral) => tx
                .send(EphemeralStreamRequest {
                    payload: Some(EphemeralRequestPayload::CreateStream(create)),
                })
                .await
                .map_err(|_| Self::open_failed()),
            #[cfg(feature = "eos")]
            (OutboundSink::Persistent(tx), GrpcConnectionMode::Persistent { resume_stream_id }) => {
                let payload = match resume_stream_id {
                    None => PersistentRequestPayload::CreateStream(CreatePersistentStreamRequest {
                        create_stream: Some(create),
                    }),
                    Some(stream_id) => {
                        // The descriptor is fixed at creation but re-sent on
                        // resume so the server can re-validate it against the
                        // table schema (required for PROTO streams; absent for
                        // JSON / Arrow, matching `create`).
                        PersistentRequestPayload::ResumeStream(ResumeIngestStreamRequest {
                            identifier: Some(ResumeIdentifier::StreamId(stream_id.clone())),
                            descriptor_proto: create.descriptor_proto,
                            record_type: create.record_type,
                        })
                    }
                };
                tx.send(PersistentStreamRequest {
                    payload: Some(payload),
                })
                .await
                .map_err(|_| Self::open_failed())
            }
            // The sink and kind are always constructed together by
            // `make_outbound`, so a mismatch is unreachable. Only present when
            // more than one variant exists (i.e. `eos` is enabled).
            #[cfg(feature = "eos")]
            _ => Err(Self::open_failed()),
        }
    }

    fn open_failed() -> ZerobusError {
        ZerobusError::StreamClosedError(tonic::Status::internal(
            "Failed to send stream-open request",
        ))
    }

    /// Sends one ingest batch on the wire, numbered with `offset_id`.
    pub(super) async fn send_ingest(
        &self,
        batch: EncodedBatch,
        offset_id: OffsetId,
    ) -> ZerobusResult<()> {
        match self {
            OutboundSink::Ephemeral(tx) => {
                let payload = batch.into_request_payload(offset_id);
                tx.send(EphemeralStreamRequest {
                    payload: Some(payload),
                })
                .await
                .map_err(|_| Self::ingest_failed())
            }
            #[cfg(feature = "eos")]
            OutboundSink::Persistent(tx) => {
                let payload = batch.into_persistent_request_payload(offset_id);
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
    #[cfg(feature = "eos")]
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
            #[cfg(feature = "eos")]
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
            #[cfg(feature = "eos")]
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
            #[cfg(feature = "eos")]
            InboundStream::Persistent(s) => while matches!(s.message().await, Ok(Some(_))) {},
        }
    }
}

/// Opens the outbound request channel for the given transport kind, returning
/// the neutral sink and the raw tonic request stream to hand to the RPC.
///
/// Split from the RPC call so the connection module can attach metadata to the
/// request before dispatching.
pub(super) fn make_outbound(kind: &GrpcConnectionMode) -> (OutboundSink, OutboundRequestStream) {
    match kind {
        GrpcConnectionMode::Ephemeral => {
            let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_BUFFER_SIZE);
            (
                OutboundSink::Ephemeral(tx),
                OutboundRequestStream::Ephemeral(ReceiverStream::new(rx)),
            )
        }
        #[cfg(feature = "eos")]
        GrpcConnectionMode::Persistent { .. } => {
            let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_BUFFER_SIZE);
            (
                OutboundSink::Persistent(tx),
                OutboundRequestStream::Persistent(ReceiverStream::new(rx)),
            )
        }
    }
}

/// The raw request stream handed to the tonic RPC method. Kept concrete because
/// `ZerobusClient::ephemeral_stream` / `persistent_stream` want the exact type.
pub(super) enum OutboundRequestStream {
    Ephemeral(ReceiverStream<EphemeralStreamRequest>),
    #[cfg(feature = "eos")]
    Persistent(ReceiverStream<PersistentStreamRequest>),
}

/// Dispatches the opening RPC on `channel` with `request` (metadata already
/// attached) and returns the inbound stream. The first server message is read
/// by the caller (`connection.rs`) so it can extract the stream_id / watermark.
pub(super) async fn open_rpc(
    channel: &mut ZerobusClient<Channel>,
    request: tonic::Request<OutboundRequestStream>,
) -> ZerobusResult<InboundStream> {
    let (metadata, extensions, body) = request.into_parts();
    match body {
        OutboundRequestStream::Ephemeral(stream) => {
            let req = tonic::Request::from_parts(metadata, extensions, stream);
            let resp = channel
                .ephemeral_stream(req)
                .await
                .map_err(ZerobusError::CreateStreamError)?;
            Ok(InboundStream::Ephemeral(resp.into_inner()))
        }
        #[cfg(feature = "eos")]
        OutboundRequestStream::Persistent(stream) => {
            let req = tonic::Request::from_parts(metadata, extensions, stream);
            let resp = channel
                .persistent_stream(req)
                .await
                .map_err(ZerobusError::CreateStreamError)?;
            Ok(InboundStream::Persistent(resp.into_inner()))
        }
    }
}
