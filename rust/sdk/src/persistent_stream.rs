//! **Prototype**: a self-contained persistent-stream path.
//!
//! This is intentionally separate from [`crate::ZerobusStream`] and *duplicates* a trimmed version
//! of its task architecture rather than reusing the production code. It keeps the same shape — a
//! **supervisor task** that performs the create/resume handshake and then spawns a **sender task**
//! and a **receiver task**, selecting on them and tearing the other down when one exits — but drops
//! the production landing zone, per-record ack futures, backpressure, pause handling, and recovery
//! loop. It exists to prototype *server-side resumable* streams:
//!
//! - [`PersistentStream::create`] opens a brand-new stream (sends `CreateStream` **without** a
//!   `stream_id`); the server assigns one, fetched via [`PersistentStream::stream_id`].
//! - [`PersistentStream::resume`] reopens an existing stream by id (sends `CreateStream` **with**
//!   that `stream_id`); the server replies with `last_committed_offset` and the client continues
//!   from `+1`.
//!
//! Both go through the single `EphemeralStream` bidi RPC — once without and once with a
//! `stream_id` — exactly as the un-reserved proto fields intend.
//!
//! Not production-grade. Acks are tracked only as a cumulative "last durable offset" for
//! observation, and there is no automatic reconnect — resume is a deliberate, manual operation.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use prost::Message;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinError, JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

use crate::databricks::zerobus::ephemeral_stream_request::Payload as RequestPayload;
use crate::databricks::zerobus::ephemeral_stream_response::Payload as ResponsePayload;
use crate::databricks::zerobus::zerobus_client::ZerobusClient;
use crate::databricks::zerobus::{
    CreateIngestStreamRequest, EphemeralStreamRequest, EphemeralStreamResponse, RecordType,
};
use crate::headers_provider::HeadersProvider;
use crate::record_types::{EncodedBatch, EncodedRecord};
use crate::{OffsetId, TableProperties, ZerobusError, ZerobusResult};

/// Buffer size of the channels feeding the bidi stream.
const REQUEST_CHANNEL_BUFFER_SIZE: usize = 2048;

/// How long [`PersistentStream::close`] waits for the supervisor task to drain.
const CLOSE_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// The pieces produced by the create/resume handshake.
type Handshake = (
    mpsc::Sender<EphemeralStreamRequest>,
    tonic::Streaming<EphemeralStreamResponse>,
    String,
    Option<OffsetId>,
);

/// A prototype resumable ingestion stream. See the module docs.
pub struct PersistentStream {
    /// Records queued by the client; drained by the sender task. Dropping it closes the stream.
    outbound_tx: mpsc::Sender<EphemeralStreamRequest>,
    /// Supervisor task: handshake, then manages the sender + receiver tasks for this connection.
    supervisor_task: JoinHandle<ZerobusResult<()>>,
    /// Cancels the supervisor's sender + receiver tasks.
    cancellation: CancellationToken,
    /// Server-assigned (create) or echoed (resume) stream identifier.
    stream_id: String,
    /// Record encoding this stream was opened with; ingested records must match.
    record_type: RecordType,
    /// Offset reported durable by the server at resume time (`None` for a fresh stream).
    last_committed_offset: Option<OffsetId>,
    /// Next offset to assign; starts at 0 (create) or `last_committed_offset + 1` (resume).
    next_offset: AtomicI64,
    /// Highest offset the server has acknowledged durable so far; `-1` means none yet.
    last_durable_offset: Arc<AtomicI64>,
}

impl PersistentStream {
    /// Opens a brand-new persistent stream (no `stream_id`). Constructed via
    /// [`crate::StreamBuilder::build_persistent`].
    pub(crate) async fn create(
        channel: ZerobusClient<Channel>,
        table_properties: TableProperties,
        headers_provider: Arc<dyn HeadersProvider>,
        record_type: RecordType,
    ) -> ZerobusResult<Self> {
        Self::connect(
            channel,
            table_properties,
            headers_provider,
            record_type,
            None,
        )
        .await
    }

    /// Resumes an existing persistent stream by `stream_id`. Constructed via
    /// [`crate::StreamBuilder::resume_persistent`].
    pub(crate) async fn resume(
        channel: ZerobusClient<Channel>,
        table_properties: TableProperties,
        headers_provider: Arc<dyn HeadersProvider>,
        record_type: RecordType,
        stream_id: String,
    ) -> ZerobusResult<Self> {
        Self::connect(
            channel,
            table_properties,
            headers_provider,
            record_type,
            Some(stream_id),
        )
        .await
    }

    /// Spawns the supervisor and waits for it to report the stream id (and resume offset).
    async fn connect(
        channel: ZerobusClient<Channel>,
        table_properties: TableProperties,
        headers_provider: Arc<dyn HeadersProvider>,
        record_type: RecordType,
        resume_stream_id: Option<String>,
    ) -> ZerobusResult<Self> {
        let (outbound_tx, outbound_rx) =
            mpsc::channel::<EphemeralStreamRequest>(REQUEST_CHANNEL_BUFFER_SIZE);
        let (init_tx, init_rx) = oneshot::channel::<ZerobusResult<(String, Option<OffsetId>)>>();
        let cancellation = CancellationToken::new();
        let last_durable_offset = Arc::new(AtomicI64::new(-1));

        let supervisor_task = tokio::task::spawn(Self::supervisor_task(
            channel,
            table_properties,
            headers_provider,
            record_type,
            resume_stream_id,
            outbound_rx,
            init_tx,
            Arc::clone(&last_durable_offset),
            cancellation.clone(),
        ));

        // Wait for the supervisor to finish the handshake and report the stream id.
        let (stream_id, last_committed_offset) = init_rx.await.map_err(|_| {
            ZerobusError::UnexpectedStreamResponseError(
                "supervisor task died before stream initialization".to_string(),
            )
        })??;

        let next_offset = last_committed_offset.map(|offset| offset + 1).unwrap_or(0);
        Ok(Self {
            outbound_tx,
            supervisor_task,
            cancellation,
            stream_id,
            record_type,
            last_committed_offset,
            next_offset: AtomicI64::new(next_offset),
            last_durable_offset,
        })
    }

    /// Supervisor: perform the handshake, report the result, then run the sender + receiver tasks
    /// for this connection until one of them exits.
    #[allow(clippy::too_many_arguments)]
    async fn supervisor_task(
        channel: ZerobusClient<Channel>,
        table_properties: TableProperties,
        headers_provider: Arc<dyn HeadersProvider>,
        record_type: RecordType,
        resume_stream_id: Option<String>,
        outbound_rx: mpsc::Receiver<EphemeralStreamRequest>,
        init_tx: oneshot::Sender<ZerobusResult<(String, Option<OffsetId>)>>,
        last_durable_offset: Arc<AtomicI64>,
        cancellation: CancellationToken,
    ) -> ZerobusResult<()> {
        // 1. Create / resume handshake.
        let (grpc_tx, response_stream, stream_id, last_committed_offset) = match Self::handshake(
            channel,
            &table_properties,
            &headers_provider,
            record_type,
            resume_stream_id,
        )
        .await
        {
            Ok(handshake) => handshake,
            Err(error) => {
                let _ = init_tx.send(Err(error.clone()));
                return Err(error);
            }
        };
        let _ = init_tx.send(Ok((stream_id.clone(), last_committed_offset)));
        info!(
            stream_id = %stream_id,
            resuming = last_committed_offset.is_some(),
            last_committed_offset = ?last_committed_offset,
            "persistent stream ready",
        );

        // 2. Spawn sender + receiver tasks.
        let mut receiver_task = Self::spawn_receiver_task(
            response_stream,
            last_durable_offset,
            cancellation.clone(),
            stream_id.clone(),
        );
        let mut sender_task = Self::spawn_sender_task(
            outbound_rx,
            grpc_tx,
            cancellation.clone(),
            stream_id.clone(),
        );

        // 3. Wait for either task to finish, then tear the other down.
        let result = tokio::select! {
            receiver_result = &mut receiver_task => {
                debug!(stream_id = %stream_id, "persistent stream: receiver task exited; stopping sender");
                cancellation.cancel();
                let _ = (&mut sender_task).await;
                Self::join(receiver_result)
            }
            sender_result = &mut sender_task => {
                debug!(stream_id = %stream_id, "persistent stream: sender task exited; stopping receiver");
                cancellation.cancel();
                let _ = (&mut receiver_task).await;
                Self::join(sender_result)
            }
        };
        if let Err(error) = &result {
            error!(stream_id = %stream_id, "persistent stream failed: {error}");
        }
        result
    }

    /// Sender task: drain client-queued records onto the gRPC request channel.
    fn spawn_sender_task(
        mut outbound_rx: mpsc::Receiver<EphemeralStreamRequest>,
        grpc_tx: mpsc::Sender<EphemeralStreamRequest>,
        cancellation: CancellationToken,
        stream_id: String,
    ) -> JoinHandle<ZerobusResult<()>> {
        tokio::spawn(async move {
            let mut forwarded: u64 = 0;
            loop {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => {
                        debug!(stream_id = %stream_id, forwarded, "persistent stream: sender cancelled");
                        return Ok(());
                    }
                    queued = outbound_rx.recv() => match queued {
                        Some(request) => {
                            if grpc_tx.send(request).await.is_err() {
                                return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                                    "gRPC request channel closed",
                                )));
                            }
                            forwarded += 1;
                        }
                        // Client dropped the stream (or called close); end gracefully.
                        None => {
                            debug!(stream_id = %stream_id, forwarded, "persistent stream: outbound queue closed; sender exiting");
                            return Ok(());
                        }
                    }
                }
            }
        })
    }

    /// Receiver task: drain server responses, recording the cumulative durability ack.
    fn spawn_receiver_task(
        mut response_stream: tonic::Streaming<EphemeralStreamResponse>,
        last_durable_offset: Arc<AtomicI64>,
        cancellation: CancellationToken,
        stream_id: String,
    ) -> JoinHandle<ZerobusResult<()>> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => {
                        debug!(stream_id = %stream_id, "persistent stream: receiver cancelled");
                        return Ok(());
                    }
                    message = response_stream.message() => match message {
                        Ok(Some(response)) => match response.payload {
                            Some(ResponsePayload::IngestRecordResponse(ack)) => {
                                if let Some(offset) = ack.durability_ack_up_to_offset {
                                    last_durable_offset.fetch_max(offset, Ordering::SeqCst);
                                    debug!(
                                        stream_id = %stream_id,
                                        durable_offset = offset,
                                        "persistent stream: durability ack",
                                    );
                                }
                            }
                            Some(ResponsePayload::CloseStreamSignal(_)) => {
                                info!(stream_id = %stream_id, "persistent stream received a close signal from the server");
                            }
                            _ => {}
                        },
                        Ok(None) => {
                            debug!(stream_id = %stream_id, "persistent stream response stream ended");
                            return Ok(());
                        }
                        Err(status) => return Err(ZerobusError::StreamClosedError(status)),
                    }
                }
            }
        })
    }

    /// Opens the bidi RPC and performs the `CreateStream` handshake (with or without `stream_id`).
    async fn handshake(
        mut channel: ZerobusClient<Channel>,
        table_properties: &TableProperties,
        headers_provider: &Arc<dyn HeadersProvider>,
        record_type: RecordType,
        resume_stream_id: Option<String>,
    ) -> ZerobusResult<Handshake> {
        let (grpc_tx, grpc_rx) =
            mpsc::channel::<EphemeralStreamRequest>(REQUEST_CHANNEL_BUFFER_SIZE);
        let mut request_stream = tonic::Request::new(ReceiverStream::new(grpc_rx));

        // Attach auth + table metadata (mirrors ZerobusStream::create_stream_connection).
        let headers = headers_provider.get_headers().await?;
        let metadata = request_stream.metadata_mut();
        for (key, value) in headers {
            if key == "authorization" {
                let mut auth_value = MetadataValue::try_from(value.as_str())
                    .map_err(|_| ZerobusError::InvalidUCTokenError(value.clone()))?;
                auth_value.set_sensitive(true);
                metadata.insert("authorization", auth_value);
            } else {
                let header_value = MetadataValue::try_from(value.as_str())
                    .map_err(|_| ZerobusError::InvalidArgument(key.to_string()))?;
                metadata.insert(key, header_value);
            }
        }

        let mut response_stream = channel
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

        let resuming = resume_stream_id.is_some();
        debug!(
            table = %table_properties.table_name,
            resuming,
            ?record_type,
            "persistent stream: sending CreateStream handshake",
        );
        let create_request = RequestPayload::CreateStream(CreateIngestStreamRequest {
            table_name: Some(table_properties.table_name.clone()),
            stream_id: resume_stream_id,
            descriptor_proto,
            record_type: Some(record_type.into()),
        });
        grpc_tx
            .send(EphemeralStreamRequest {
                payload: Some(create_request),
            })
            .await
            .map_err(|_| {
                ZerobusError::StreamClosedError(tonic::Status::internal(
                    "failed to send CreateStream request",
                ))
            })?;

        match response_stream.message().await {
            Ok(Some(response)) => match response.payload {
                Some(ResponsePayload::CreateStreamResponse(resp)) => {
                    let stream_id = resp.stream_id.ok_or_else(|| {
                        ZerobusError::CreateStreamError(tonic::Status::internal(
                            "CreateStreamResponse missing stream_id",
                        ))
                    })?;
                    Ok((
                        grpc_tx,
                        response_stream,
                        stream_id,
                        resp.last_committed_offset,
                    ))
                }
                unexpected => Err(ZerobusError::CreateStreamError(tonic::Status::internal(
                    format!("unexpected first response from server: {unexpected:?}"),
                ))),
            },
            Ok(None) => Err(ZerobusError::CreateStreamError(tonic::Status::internal(
                "server closed the stream before the CreateStream response",
            ))),
            Err(status) => Err(ZerobusError::CreateStreamError(status)),
        }
    }

    /// Flattens a task join result into a [`ZerobusResult`].
    fn join(result: Result<ZerobusResult<()>, JoinError>) -> ZerobusResult<()> {
        match result {
            Ok(inner) => inner,
            Err(join_error) => Err(ZerobusError::UnexpectedStreamResponseError(format!(
                "persistent stream task panicked: {join_error}"
            ))),
        }
    }

    /// The server-assigned (create) or echoed (resume) stream id.
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    /// The offset the server reported as already durable at resume time (`None` for a fresh
    /// stream).
    pub fn last_committed_offset(&self) -> Option<OffsetId> {
        self.last_committed_offset
    }

    /// The highest offset the server has acknowledged durable since this connection opened.
    pub fn last_durable_offset(&self) -> Option<OffsetId> {
        let value = self.last_durable_offset.load(Ordering::SeqCst);
        if value < 0 {
            None
        } else {
            Some(value)
        }
    }

    /// Queues one record for the sender task and returns the offset assigned to it. Fire-and-forget:
    /// use [`Self::last_durable_offset`] to observe server acknowledgement.
    pub async fn ingest(&self, record: impl Into<EncodedRecord>) -> ZerobusResult<OffsetId> {
        let batch = EncodedBatch::try_from_record(record, self.record_type).ok_or_else(|| {
            ZerobusError::InvalidArgument(
                "record type does not match stream configuration".to_string(),
            )
        })?;
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
        let payload = batch.into_request_payload(offset);
        self.outbound_tx
            .send(EphemeralStreamRequest {
                payload: Some(payload),
            })
            .await
            .map_err(|_| {
                ZerobusError::StreamClosedError(tonic::Status::internal(
                    "persistent stream is closed",
                ))
            })?;
        debug!(stream_id = %self.stream_id, offset, "persistent stream: queued record");
        Ok(offset)
    }

    /// Closes the stream from the client side and waits briefly for the supervisor to wind down.
    ///
    /// Dropping the outbound channel ends the sender task, which lets the supervisor cancel the
    /// receiver task and return. (Dropping a `PersistentStream` without calling `close` detaches
    /// the tasks; they end when the connection drops.)
    pub async fn close(self) -> ZerobusResult<()> {
        info!(
            stream_id = %self.stream_id,
            last_durable_offset = ?self.last_durable_offset(),
            "persistent stream: closing",
        );
        drop(self.outbound_tx);
        match tokio::time::timeout(CLOSE_DRAIN_TIMEOUT, self.supervisor_task).await {
            Ok(join_result) => Self::join(join_result),
            Err(_timeout) => {
                warn!(stream_id = %self.stream_id, "persistent stream close timed out; cancelling tasks");
                self.cancellation.cancel();
                Ok(())
            }
        }
    }
}
