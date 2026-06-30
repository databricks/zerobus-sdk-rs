//! Outbound gRPC sender task.
//!
//! Transport-specific: reads from the landing zone (transport-agnostic) and
//! writes `EphemeralStreamRequest` messages over the gRPC outbound channel.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::error;

use super::types::RecordLandingZone;
use super::ZerobusStream;
use crate::databricks::zerobus::EphemeralStreamRequest;
use crate::offset_generator::OffsetIdGenerator;
use crate::{ZerobusError, ZerobusResult};

impl ZerobusStream {
    /// Spawns a task that continuously sends records to the Zerobus API by observing the landing zone
    /// to get records and sending them through the outbound stream to the gRPC stream.
    pub(super) fn spawn_sender_task(
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
}
