//! **Prototype**: a *stateful* mock Zerobus server that models server-side persistent (resumable)
//! streams — the behavior shinkansen-dp doesn't implement yet.
//!
//! Unlike `mock_grpc.rs` (which replays scripted responses), this mock keeps an in-memory
//! `stream_id -> last_committed_offset` map that survives reconnects:
//!
//! - `CreateStream` **without** a `stream_id` → mint a new id, committed = none → return the id.
//! - `CreateStream` **with** a known `stream_id` → return that id + its stored committed offset
//!   (resume).
//! - `IngestRecord{,Batch}(offset)` → ack `durability_ack_up_to_offset = offset` and persist it as
//!   the stream's committed offset.
//!
//! That is enough to exercise the SDK's `PersistentStream` create → ingest → close → resume flow
//! end-to-end against a server that actually resumes.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

pub mod databricks {
    pub mod zerobus {
        tonic::include_proto!("databricks.zerobus");
    }
}
use databricks::zerobus::{
    ephemeral_stream_request::Payload as RequestPayload,
    ephemeral_stream_response::Payload as ResponsePayload,
    zerobus_server::{Zerobus, ZerobusServer},
    CreateIngestStreamResponse, EphemeralStreamRequest, EphemeralStreamResponse,
    IngestRecordResponse,
};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info};

/// Persistent per-stream state shared across connections (the thing a real server would durably
/// store). Maps `stream_id -> last committed offset` (`-1` = nothing committed yet).
#[derive(Clone, Default)]
pub struct PersistentState {
    streams: Arc<Mutex<HashMap<String, i64>>>,
    counter: Arc<Mutex<u64>>,
}

impl PersistentState {
    /// The committed offset stored for `stream_id`, if any (`-1`/absent → `None`).
    pub async fn committed_offset(&self, stream_id: &str) -> Option<i64> {
        match self.streams.lock().await.get(stream_id).copied() {
            Some(offset) if offset >= 0 => Some(offset),
            _ => None,
        }
    }
}

struct PersistentMockServer {
    state: PersistentState,
}

#[tonic::async_trait]
impl Zerobus for PersistentMockServer {
    type EphemeralStreamStream =
        Pin<Box<dyn Stream<Item = Result<EphemeralStreamResponse, Status>> + Send>>;

    async fn ephemeral_stream(
        &self,
        request: Request<Streaming<EphemeralStreamRequest>>,
    ) -> Result<Response<Self::EphemeralStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(128);
        let state = self.state.clone();

        tokio::spawn(async move {
            // First message must be CreateStream (create or resume).
            let create = match inbound.message().await {
                Ok(Some(message)) => match message.payload {
                    Some(RequestPayload::CreateStream(create)) => create,
                    _ => {
                        let _ = tx
                            .send(Err(Status::invalid_argument(
                                "first message must be CreateStream",
                            )))
                            .await;
                        return;
                    }
                },
                _ => return,
            };

            // Resolve create-vs-resume and the offset to report back.
            let (stream_id, last_committed_offset) = {
                let mut streams = state.streams.lock().await;
                match create.stream_id {
                    // Resume a stream we know about.
                    Some(id) if streams.contains_key(&id) => {
                        let committed = streams[&id];
                        let reported = if committed >= 0 {
                            Some(committed)
                        } else {
                            None
                        };
                        (id, reported)
                    }
                    // Resume requested for an unknown id: treat as a fresh stream under that id.
                    Some(id) => {
                        streams.insert(id.clone(), -1);
                        (id, None)
                    }
                    // Brand-new stream: mint an id.
                    None => {
                        let mut counter = state.counter.lock().await;
                        *counter += 1;
                        let id = format!("persistent-stream-{}", *counter);
                        streams.insert(id.clone(), -1);
                        (id, None)
                    }
                }
            };

            match last_committed_offset {
                Some(offset) => info!(
                    stream_id = %stream_id,
                    resumed_from = offset,
                    "mock: resuming persistent stream",
                ),
                None => info!(stream_id = %stream_id, "mock: opened new persistent stream"),
            }

            let create_response = EphemeralStreamResponse {
                payload: Some(ResponsePayload::CreateStreamResponse(
                    CreateIngestStreamResponse {
                        stream_id: Some(stream_id.clone()),
                        last_committed_offset,
                    },
                )),
            };
            if tx.send(Ok(create_response)).await.is_err() {
                return;
            }

            // Ingest loop: ack each offset and persist it as the committed offset.
            while let Ok(Some(message)) = inbound.message().await {
                let offset = match message.payload {
                    Some(RequestPayload::IngestRecord(record)) => record.offset_id,
                    Some(RequestPayload::IngestRecordBatch(batch)) => batch.offset_id,
                    _ => None,
                };
                if let Some(offset) = offset {
                    state.streams.lock().await.insert(stream_id.clone(), offset);
                    debug!(stream_id = %stream_id, offset, "mock: persisted + acked offset");
                    let ack = EphemeralStreamResponse {
                        payload: Some(ResponsePayload::IngestRecordResponse(
                            IngestRecordResponse {
                                durability_ack_up_to_offset: Some(offset),
                            },
                        )),
                    };
                    if tx.send(Ok(ack)).await.is_err() {
                        return;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// Starts the stateful persistent mock on a random local port. Returns the shared state (for
/// assertions) and the `http://` URL to point the SDK at.
pub async fn start_persistent_mock() -> Result<(PersistentState, String), Box<dyn std::error::Error>>
{
    let state = PersistentState::default();
    let server = PersistentMockServer {
        state: state.clone(),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);

    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ZerobusServer::new(server))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });

    // Give the server a moment to start accepting connections.
    tokio::time::sleep(Duration::from_millis(100)).await;
    Ok((state, url))
}
