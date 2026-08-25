//! Outbound gRPC sender task.
//!
//! Transport-specific: reads from the landing zone (transport-agnostic) and
//! writes ingest messages through the `OutboundSink`, which hides whether the
//! underlying RPC is `EphemeralStream` or `PersistentStream`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::error;

use super::transport::OutboundSink;
use super::types::RecordLandingZone;
use super::ZerobusStream;
use crate::offset_generator::OffsetIdGenerator;
use crate::{ZerobusError, ZerobusResult};

impl ZerobusStream {
    /// Spawns a task that continuously sends records to the Zerobus API by observing the landing zone
    /// to get records and sending them through the outbound stream to the gRPC stream.
    ///
    /// Persistent SDK and wire offsets match exactly. Ephemeral streams use a
    /// fresh 0-based wire sequence for each server stream so their SDK offsets
    /// can preserve continuity across recovery.
    pub(super) fn spawn_sender_task(
        sink: OutboundSink,
        landing_zone: RecordLandingZone,
        is_paused: Arc<AtomicBool>,
        server_error_tx: tokio::sync::watch::Sender<Option<ZerobusError>>,
        cancellation_token: CancellationToken,
        wire_offsets_match: bool,
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
                let wire_offset = if wire_offsets_match {
                    item.offset_id
                } else {
                    physical_offset_id_generator.next()
                };

                let send_result = sink.send_ingest(item.payload, wire_offset).await;

                if let Err(err) = send_result {
                    error!("Failed to send record: {}", err);
                    let _ = server_error_tx.send(Some(err.clone()));
                    return Err(err);
                }
            }
        })
    }
}
