//! Outbound gRPC sender task.
//!
//! Transport-specific: reads from the landing zone (transport-agnostic) and
//! writes `EphemeralStreamRequest` messages over the gRPC outbound channel.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::error;

use super::types::{RecordLandingZone, SentOffsetWatermark};
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
        highest_sent_offset: SentOffsetWatermark,
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

                let permit = tokio::select! {
                    biased;
                    _ = cancellation_token.cancelled() => return Ok(()),
                    permit = outbound_stream.reserve() => permit,
                };
                let permit = match permit {
                    Ok(permit) => permit,
                    Err(err) => {
                        error!("Failed to reserve outbound stream capacity: {}", err);
                        let error = ZerobusError::StreamClosedError(tonic::Status::internal(
                            "Failed to send record",
                        ));
                        let _ = server_error_tx.send(Some(error.clone()));
                        return Err(error);
                    }
                };

                let offset_id = physical_offset_id_generator.next();
                let request_payload = item.payload.into_request_payload(offset_id);
                let request = EphemeralStreamRequest {
                    payload: Some(request_payload),
                };

                {
                    let mut watermark = highest_sent_offset
                        .lock()
                        .expect("Sent offset watermark lock poisoned");
                    permit.send(request);
                    *watermark = offset_id;
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};

    use tokio::sync::{mpsc, watch};
    use tokio::time::{timeout, Duration};
    use tokio_util::sync::CancellationToken;

    use super::ZerobusStream;
    use crate::databricks::zerobus::RecordType;
    use crate::landing_zone::LandingZone;
    use crate::stream::grpc::types::IngestRequest;
    use crate::EncodedBatch;

    async fn wait_for_watermark(watermark: &Arc<Mutex<i64>>, expected: i64) {
        timeout(Duration::from_secs(1), async {
            loop {
                if *watermark.lock().expect("watermark lock poisoned") == expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("sender did not publish the expected watermark");
    }

    #[tokio::test]
    async fn blocked_channel_does_not_publish_unsent_offset() {
        let landing_zone = Arc::new(LandingZone::new(2));
        for logical_offset in 0..2 {
            let payload =
                EncodedBatch::try_from_record(vec![logical_offset as u8], RecordType::Proto)
                    .expect("record type must match");
            landing_zone
                .add(Box::new(IngestRequest {
                    payload,
                    offset_id: logical_offset,
                }))
                .await;
        }

        let (outbound_tx, mut outbound_rx) = mpsc::channel(1);
        let (server_error_tx, _server_error_rx) = watch::channel(None);
        let cancellation_token = CancellationToken::new();
        let highest_sent_offset = Arc::new(Mutex::new(-1));
        let task = ZerobusStream::spawn_sender_task(
            outbound_tx,
            Arc::clone(&landing_zone),
            Arc::new(AtomicBool::new(false)),
            server_error_tx,
            cancellation_token.clone(),
            Arc::clone(&highest_sent_offset),
        );

        wait_for_watermark(&highest_sent_offset, 0).await;
        timeout(Duration::from_secs(1), async {
            while landing_zone.observed_count() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the second item was not observed");
        assert_eq!(
            *highest_sent_offset.lock().expect("watermark lock poisoned"),
            0,
            "the second item is observed but blocked on channel capacity"
        );

        outbound_rx
            .recv()
            .await
            .expect("first request must be sent");
        wait_for_watermark(&highest_sent_offset, 1).await;
        outbound_rx
            .recv()
            .await
            .expect("second request must be sent");

        cancellation_token.cancel();
        task.await
            .expect("sender task must not panic")
            .expect("sender task must stop cleanly");
    }
}
