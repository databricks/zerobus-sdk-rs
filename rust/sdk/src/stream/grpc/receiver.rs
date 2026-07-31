//! Inbound gRPC receiver task.
//!
//! Transport-specific: reads `EphemeralStreamResponse` messages from the
//! gRPC inbound stream, dispatches durability acks to oneshot senders /
//! callbacks, and signals the supervisor via `server_error_tx` / pause
//! deadlines.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument, span, Level};

use super::types::{CallbackMessage, OneshotMap, RecordLandingZone, SentOffsetWatermark};
use super::{ZerobusStream, STREAM_TEARDOWN_DRAIN_TIMEOUT_MS};
use crate::databricks::zerobus::ephemeral_stream_response::Payload as ResponsePayload;
use crate::databricks::zerobus::{
    CloseStreamSignal, EphemeralStreamResponse, IngestRecordResponse,
};
use crate::{OffsetId, StreamConfigurationOptions, ZerobusError, ZerobusResult};

fn validate_ack_offset(
    ack_offset: OffsetId,
    last_acked_offset: OffsetId,
    highest_sent_offset: OffsetId,
) -> ZerobusResult<bool> {
    if ack_offset < 0 {
        return Err(ZerobusError::InvalidStateError(format!(
            "Server ack offset {ack_offset} is negative"
        )));
    }
    if ack_offset <= last_acked_offset {
        return Ok(false);
    }
    if ack_offset > highest_sent_offset {
        return Err(ZerobusError::InvalidStateError(format!(
            "Server ack offset {ack_offset} exceeds highest sent offset {highest_sent_offset}"
        )));
    }
    Ok(true)
}

impl ZerobusStream {
    /// Spawns a task that continuously reads from `response_grpc_stream`
    /// and propagates the received durability acknowledgements to the
    /// corresponding pending acks promises.
    #[instrument(level = "debug", skip_all)]
    #[allow(clippy::too_many_arguments)]
    pub(super) fn spawn_receiver_task(
        mut response_grpc_stream: tonic::Streaming<EphemeralStreamResponse>,
        last_received_offset_id_tx: tokio::sync::watch::Sender<Option<OffsetId>>,
        landing_zone: RecordLandingZone,
        oneshot_map: Arc<tokio::sync::Mutex<OneshotMap>>,
        is_paused: Arc<AtomicBool>,
        options: StreamConfigurationOptions,
        server_error_tx: tokio::sync::watch::Sender<Option<ZerobusError>>,
        recv_drain_token: CancellationToken,
        callback_tx: Option<tokio::sync::mpsc::UnboundedSender<CallbackMessage>>,
        highest_sent_offset: SentOffsetWatermark,
    ) -> tokio::task::JoinHandle<ZerobusResult<()>> {
        tokio::spawn(async move {
            let span = span!(Level::DEBUG, "inbound_stream_processor");
            let _guard = span.enter();
            let mut last_acked_offset: OffsetId = -1;
            let mut pause_deadline: Option<tokio::time::Instant> = None;
            // Set when we exit because the supervisor signalled close (`recv_drain_token`).
            // On that path we drain the response stream inline so the server sees END_STREAM
            // instead of RST_STREAM. On all other exits (recovery / errors) the runtime is
            // still up, so a detached drain is used to avoid blocking recovery.
            let mut close_initiated = false;

            'recv_loop: loop {
                if let Some(deadline) = pause_deadline {
                    let now = tokio::time::Instant::now();
                    let all_acked = landing_zone.is_observed_empty();

                    if now >= deadline {
                        info!("Graceful close timeout reached. Triggering recovery.");
                        break 'recv_loop;
                    } else if all_acked {
                        info!("All in-flight records acknowledged during graceful close. Triggering recovery.");
                        break 'recv_loop;
                    }
                }

                let message_result = if let Some(deadline) = pause_deadline {
                    tokio::select! {
                        biased;
                        _ = recv_drain_token.cancelled() => {
                            close_initiated = true;
                            break 'recv_loop;
                        }
                        _ = tokio::time::sleep_until(deadline) => {
                            continue;
                        }
                        res = tokio::time::timeout(
                            Duration::from_millis(options.server_lack_of_ack_timeout_ms),
                            response_grpc_stream.message(),
                        ) => res,
                    }
                } else {
                    tokio::select! {
                        biased;
                        _ = recv_drain_token.cancelled() => {
                            close_initiated = true;
                            break 'recv_loop;
                        }
                        res = tokio::time::timeout(
                            Duration::from_millis(options.server_lack_of_ack_timeout_ms),
                            response_grpc_stream.message(),
                        ) => res,
                    }
                };

                match message_result {
                    Ok(Ok(Some(ingest_record_response))) => match ingest_record_response.payload {
                        Some(ResponsePayload::IngestRecordResponse(IngestRecordResponse {
                            durability_ack_up_to_offset,
                        })) => {
                            let durability_ack_up_to_offset = match durability_ack_up_to_offset {
                                Some(offset) => offset,
                                None => {
                                    error!("Missing ack offset in server response");
                                    let error =
                                        ZerobusError::StreamClosedError(tonic::Status::internal(
                                            "Missing ack offset in server response",
                                        ));
                                    let _ = server_error_tx.send(Some(error.clone()));
                                    return Err(error);
                                }
                            };
                            let sent_offset = *highest_sent_offset
                                .lock()
                                .expect("Sent offset watermark lock poisoned");
                            match validate_ack_offset(
                                durability_ack_up_to_offset,
                                last_acked_offset,
                                sent_offset,
                            ) {
                                Ok(true) => {}
                                Ok(false) => continue,
                                Err(error) => {
                                    error!("{error}");
                                    let _ = server_error_tx.send(Some(error.clone()));
                                    return Err(error);
                                }
                            }
                            let mut last_logical_acked_offset = -2;
                            let mut map = oneshot_map.lock().await;
                            for offset_to_ack in
                                (last_acked_offset + 1)..=durability_ack_up_to_offset
                            {
                                let record = match landing_zone.remove_observed() {
                                    Ok(record) => record,
                                    Err(_) => {
                                        let message = format!(
                                            "Server ack offset {durability_ack_up_to_offset} could not be applied at physical offset {offset_to_ack}"
                                        );
                                        error!("{message}");
                                        let error = ZerobusError::InvalidStateError(message);
                                        let _ = server_error_tx.send(Some(error.clone()));
                                        return Err(error);
                                    }
                                };
                                let logical_offset = record.offset_id;
                                last_logical_acked_offset = logical_offset;

                                if let Some(sender) = map.remove(&logical_offset) {
                                    let _ = sender.send(Ok(logical_offset));
                                }

                                if let Some(ref tx) = callback_tx {
                                    let _ = tx.send(CallbackMessage::Ack(logical_offset));
                                }
                            }
                            drop(map);
                            last_acked_offset = durability_ack_up_to_offset;
                            if last_logical_acked_offset != -2 {
                                let _ignore_on_channel_break = last_received_offset_id_tx
                                    .send(Some(last_logical_acked_offset));
                            }
                        }
                        Some(ResponsePayload::CloseStreamSignal(CloseStreamSignal {
                            duration,
                        })) => {
                            if options.recovery {
                                let server_duration_ms = duration
                                    .as_ref()
                                    .map(|d| d.seconds as u64 * 1000 + d.nanos as u64 / 1_000_000)
                                    .unwrap_or(0);

                                let wait_duration_ms = match options.stream_paused_max_wait_time_ms
                                {
                                    None => server_duration_ms,
                                    Some(0) => {
                                        // Immediate recovery
                                        info!("Server will close the stream in {}ms. Triggering stream recovery.", server_duration_ms);
                                        break 'recv_loop;
                                    }
                                    Some(max_wait) => std::cmp::min(max_wait, server_duration_ms),
                                };

                                if wait_duration_ms == 0 {
                                    info!("Server will close the stream. Triggering immediate recovery.");
                                    break 'recv_loop;
                                }

                                is_paused.store(true, Ordering::Relaxed);
                                pause_deadline = Some(
                                    tokio::time::Instant::now()
                                        + Duration::from_millis(wait_duration_ms),
                                );
                                info!(
                                    "Server will close the stream in {}ms. Entering graceful close period (waiting up to {}ms for in-flight acks).",
                                    server_duration_ms, wait_duration_ms
                                );
                            }
                        }
                        unexpected_message => {
                            error!("Unexpected response from server {unexpected_message:?}");
                            let error = ZerobusError::StreamClosedError(tonic::Status::internal(
                                "Unexpected response from server",
                            ));
                            let _ = server_error_tx.send(Some(error.clone()));
                            return Err(error);
                        }
                    },
                    Ok(Ok(None)) => {
                        info!("Server closed the stream without errors.");
                        let error = ZerobusError::StreamClosedError(tonic::Status::ok(
                            "Stream closed by server without errors.",
                        ));
                        let _ = server_error_tx.send(Some(error.clone()));
                        return Err(error);
                    }
                    Ok(Err(status)) => {
                        error!("Unexpected response from server {status:?}");
                        let error = ZerobusError::StreamClosedError(status);
                        let _ = server_error_tx.send(Some(error.clone()));
                        return Err(error);
                    }
                    Err(_timeout) => {
                        // No message received for server_lack_of_ack_timeout_ms.
                        if pause_deadline.is_none() && !landing_zone.is_observed_empty() {
                            error!(
                                "Server ack timeout: no response for {}ms",
                                options.server_lack_of_ack_timeout_ms
                            );
                            let error = ZerobusError::StreamClosedError(
                                tonic::Status::deadline_exceeded("Server ack timeout"),
                            );
                            let _ = server_error_tx.send(Some(error.clone()));
                            return Err(error);
                        }
                    }
                }
            }

            // Drain remaining server messages so the server sees END_STREAM instead of
            // the client RST_STREAM-ing the response. Inline on close (runtime may exit
            // right after); detached on recovery / errors so recovery isn't delayed.
            if close_initiated {
                let _ = tokio::time::timeout(
                    Duration::from_millis(STREAM_TEARDOWN_DRAIN_TIMEOUT_MS),
                    async {
                        while response_grpc_stream
                            .message()
                            .await
                            .ok()
                            .flatten()
                            .is_some()
                        {}
                    },
                )
                .await;
            } else {
                tokio::spawn(async move {
                    let _ = tokio::time::timeout(
                        Duration::from_millis(STREAM_TEARDOWN_DRAIN_TIMEOUT_MS),
                        async move {
                            while response_grpc_stream
                                .message()
                                .await
                                .ok()
                                .flatten()
                                .is_some()
                            {}
                        },
                    )
                    .await;
                });
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::validate_ack_offset;
    use crate::ZerobusError;

    #[test]
    fn negative_ack_is_rejected() {
        let error = validate_ack_offset(-1, -1, 0).expect_err("negative ack must fail");
        assert!(matches!(
            error,
            ZerobusError::InvalidStateError(message)
                if message == "Server ack offset -1 is negative"
        ));
    }

    #[test]
    fn duplicate_or_regressive_ack_is_ignored() {
        assert!(!validate_ack_offset(3, 3, 5).expect("duplicate ack is valid"));
        assert!(!validate_ack_offset(2, 3, 5).expect("regressive ack is valid"));
    }

    #[test]
    fn regressive_ack_does_not_lower_watermark() {
        let mut last_acked_offset = -1;
        for ack_offset in [1, 0, 2] {
            if validate_ack_offset(ack_offset, last_acked_offset, 2)
                .expect("ack sequence must be valid")
            {
                last_acked_offset = ack_offset;
            }
        }
        assert_eq!(last_acked_offset, 2);
    }

    #[test]
    fn ack_beyond_highest_sent_offset_is_rejected() {
        let error = validate_ack_offset(4, 2, 3).expect_err("over-ack must fail");
        assert!(matches!(
            error,
            ZerobusError::InvalidStateError(message)
                if message
                    == "Server ack offset 4 exceeds highest sent offset 3"
        ));
    }

    #[test]
    fn advancing_ack_within_sent_range_is_applied() {
        assert!(validate_ack_offset(4, 2, 4).expect("valid ack must advance"));
    }
}
