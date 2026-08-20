//! Supervisor task: stream lifecycle and recovery loop.
//!
//! The supervisor orchestrates a create-stream → spawn-sender/receiver →
//! watch-for-failure → recover loop. The high-level pattern is reusable
//! across transports, but the current implementation is wired to the gRPC
//! transport (it calls into `create_stream_connection`, spawns the gRPC
//! sender/receiver tasks, and uses `ZerobusClient<Channel>`). Future work
//! to share this with Arrow Flight should abstract the transport seams.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::time::Duration;
use tokio_retry::strategy::FixedInterval;
use tokio_retry::RetryIf;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::{debug, error, info, instrument, warn};

use super::transport::TransportKind;
use super::types::{CallbackMessage, OneshotMap, RecordLandingZone};
use super::{ZerobusStream, STREAM_TEARDOWN_DRAIN_TIMEOUT_MS};
use crate::databricks::zerobus::zerobus_client::ZerobusClient;
use crate::errors::should_retry_initial_connection;
use crate::{
    EncodedBatch, HeadersProvider, OffsetId, StreamConfigurationOptions, TableProperties,
    ZerobusError, ZerobusResult,
};

/// What the supervisor reports back to the constructor once the stream is
/// first opened: the server-assigned identity and, for a persistent resume, the
/// committed-offset watermark the caller uses to reseed its offset generator.
pub(super) struct StreamInitInfo {
    pub(super) stream_id: String,
    pub(super) last_committed_offset: Option<OffsetId>,
}

impl ZerobusStream {
    /// Supervisor task is responsible for managing the stream lifecycle.
    /// It handles stream creation, recovery, and error handling.
    ///
    /// `kind` selects the transport (ephemeral vs persistent). For a persistent
    /// stream the supervisor opens with a create (or a resume, if the caller
    /// supplied a stream id) on the first iteration, then resumes by the minted
    /// id on every subsequent recovery — an ephemeral stream re-creates each
    /// time, as before.
    #[allow(clippy::too_many_arguments)]
    #[instrument(level = "debug", skip_all, fields(table_name = %table_properties.table_name))]
    pub(super) async fn supervisor_task(
        channel: ZerobusClient<Channel>,
        table_properties: TableProperties,
        headers_provider: Arc<dyn HeadersProvider>,
        options: StreamConfigurationOptions,
        // Mutated only on the persistent recovery path (flip create → resume);
        // that mutation is `eos`-gated, so `mut` is otherwise unused.
        #[cfg_attr(not(feature = "eos"), allow(unused_mut))] mut kind: TransportKind,
        landing_zone: RecordLandingZone,
        oneshot_map: Arc<tokio::sync::Mutex<OneshotMap>>,
        logical_last_received_offset_id_tx: tokio::sync::watch::Sender<Option<OffsetId>>,
        is_closed: Arc<AtomicBool>,
        failed_records: Arc<RwLock<Vec<EncodedBatch>>>,
        stream_init_result_tx: tokio::sync::oneshot::Sender<ZerobusResult<StreamInitInfo>>,
        server_error_tx: tokio::sync::watch::Sender<Option<ZerobusError>>,
        cancellation_token: CancellationToken,
        callback_tx: Option<tokio::sync::mpsc::UnboundedSender<CallbackMessage>>,
    ) -> ZerobusResult<()> {
        let durable_wire_offset = kind.uses_durable_wire_offset();
        let mut initial_stream_creation = true;
        let mut stream_init_result_tx = Some(stream_init_result_tx);
        // One-shot budget: initial setup may spend a single recovery retry to refresh a
        // stale credential after an auth rejection. Reconnect iterations never consume it,
        // so mid-stream auth behavior is unchanged. Auth errors stay globally non-retryable.
        let mut initial_auth_retry_available = options.recovery_retries > 0;

        loop {
            debug!("Supervisor task loop");

            if cancellation_token.is_cancelled() {
                debug!("Supervisor task cancelled, exiting");
                return Ok(());
            }

            if !initial_stream_creation {
                info!(
                    pending_batches = landing_zone.len(),
                    pending_records = landing_zone.total_count(),
                    "Stream lost; starting recovery"
                );
            }

            let landing_zone_sender = Arc::clone(&landing_zone);
            let landing_zone_receiver = Arc::clone(&landing_zone);
            let landing_zone_recovery = Arc::clone(&landing_zone);

            // 1. Create a stream.
            let strategy = FixedInterval::from_millis(options.recovery_backoff_ms)
                .take(options.recovery_retries as usize);

            // Initial setup may refresh one stale credential (auth rejection consumes the
            // one-shot budget); reconnect keeps the plain `recovery && is_retryable()` rule
            // so mid-stream behavior is unchanged. Both connection paths invalidate on auth
            // rejection, so the retry re-mints via the headers provider. Initial setup bounds
            // that invalidation under the setup deadline so a stalled provider cannot bypass
            // the one-shot limit; reconnect keeps its existing timeout-wrapped path unchanged.
            let is_initial = initial_stream_creation;
            let attempt = AtomicUsize::new(0);
            let create_attempt = || {
                let channel = channel.clone();
                let table_properties = table_properties.clone();
                let headers_provider = Arc::clone(&headers_provider);
                let record_type = options.record_type;
                let attempt = &attempt;
                let kind = &kind;

                async move {
                    let attempt_no = attempt.fetch_add(1, Ordering::Relaxed) + 1;
                    let result = if is_initial {
                        Self::create_initial_stream_connection(
                            channel,
                            &table_properties,
                            &headers_provider,
                            record_type,
                            kind,
                            options.recovery_timeout_ms,
                        )
                        .await
                    } else {
                        tokio::time::timeout(
                            Duration::from_millis(options.recovery_timeout_ms),
                            Self::create_stream_connection(
                                channel,
                                &table_properties,
                                &headers_provider,
                                record_type,
                                kind,
                            ),
                        )
                        .await
                        .map_err(|_| {
                            ZerobusError::CreateStreamError(tonic::Status::deadline_exceeded(
                                "Stream creation timed out",
                            ))
                        })
                        .and_then(|res| res)
                    };
                    if let Err(ref e) = result {
                        warn!(
                            attempt = attempt_no,
                            max_attempts = options.recovery_retries + 1,
                            retryable = e.is_retryable(),
                            backoff_ms = options.recovery_backoff_ms,
                            "Stream creation attempt failed: {}",
                            e
                        );
                    }
                    result
                }
            };
            let should_retry = |e: &ZerobusError| {
                if is_initial {
                    should_retry_initial_connection(
                        e,
                        options.recovery,
                        &mut initial_auth_retry_available,
                    )
                } else {
                    options.recovery && e.is_retryable()
                }
            };
            let creation = RetryIf::spawn(strategy, create_attempt, should_retry).await;

            let connection = match creation {
                Ok(connection) => connection,
                Err(e) => {
                    if initial_stream_creation {
                        if let Some(tx) = stream_init_result_tx.take() {
                            let _ = tx.send(Err(e.clone()));
                        }
                    } else {
                        is_closed.store(true, Ordering::Relaxed);
                        error!(
                            attempts = attempt.load(Ordering::Relaxed),
                            max_attempts = options.recovery_retries + 1,
                            table_name = %table_properties.table_name,
                            "Recovery failed; giving up stream re-creation: {}",
                            e
                        );
                        Self::fail_all_pending_records(
                            landing_zone.clone(),
                            oneshot_map.clone(),
                            failed_records.clone(),
                            &e,
                            &callback_tx,
                        )
                        .await;
                    }
                    return Err(e);
                }
            };
            let stream_id = connection.stream_id;
            let last_committed_offset = connection.last_committed_offset;
            if initial_stream_creation {
                if let Some(stream_init_result_tx_inner) = stream_init_result_tx.take() {
                    let _ = stream_init_result_tx_inner.send(Ok(StreamInitInfo {
                        stream_id: stream_id.clone(),
                        last_committed_offset,
                    }));
                }
                initial_stream_creation = false;
                // A persistent stream recovers by resuming its now-known id, not
                // by creating a fresh stream on every reconnect.
                #[cfg(feature = "eos")]
                if let TransportKind::Persistent { resume_stream_id } = &mut kind {
                    *resume_stream_id = Some(stream_id.clone());
                }
                info!(stream_id = %stream_id, "Successfully created stream");
            } else {
                info!(stream_id = %stream_id, "Successfully recovered stream");
                let _ = server_error_tx.send(None);
            }

            // 2. Reset landing zone.
            let resent_records = landing_zone_recovery.observed_count();
            let resent_batches = landing_zone_recovery.reset_observe();

            // Resume alignment (persistent streams). The server reports how far
            // it has durably committed. A dropped connection can lose the ack
            // for records the server did commit, so the retained tail can start
            // at or below that watermark. Re-sending an already-committed offset
            // would be rejected by the server and kill the stream (Decision 6:
            // server rejects), so reconcile first: resolve those records'
            // pending acks locally and re-send only the offsets above the
            // watermark. This is not user-duplicate dedup — it realigns the
            // SDK's retained tail with the server's durable position on resume.
            let mut initial_last_acked_offset: OffsetId = -1;
            if durable_wire_offset {
                if let Some(watermark) = last_committed_offset {
                    initial_last_acked_offset = watermark;
                    Self::reconcile_committed_on_resume(
                        &landing_zone_recovery,
                        &oneshot_map,
                        &logical_last_received_offset_id_tx,
                        &callback_tx,
                        watermark,
                    )
                    .await;
                }
            }

            if resent_batches > 0 {
                info!(
                    stream_id = %stream_id,
                    resent_batches,
                    resent_records,
                    pending_batches = landing_zone_recovery.len(),
                    pending_records = landing_zone_recovery.total_count(),
                    "Recovered stream; re-sending unacknowledged records"
                );
            }

            // 3. Spawn receiver and sender task.
            let is_paused = Arc::new(AtomicBool::new(false));

            // Per-stream child token
            let per_stream_token = cancellation_token.child_token();
            // Separate token for recv_task's close path
            let recv_drain_token = CancellationToken::new();

            let mut recv_task = Self::spawn_receiver_task(
                connection.inbound,
                logical_last_received_offset_id_tx.clone(),
                landing_zone_receiver,
                oneshot_map.clone(),
                Arc::clone(&is_paused),
                options.clone(),
                server_error_tx.clone(),
                recv_drain_token.clone(),
                callback_tx.clone(),
                initial_last_acked_offset,
            );
            let mut send_task = Self::spawn_sender_task(
                connection.sink,
                landing_zone_sender,
                Arc::clone(&is_paused),
                server_error_tx.clone(),
                per_stream_token.clone(),
                durable_wire_offset,
            );

            // 4. Wait for any of the two tasks to end.
            let result = tokio::select! {
                recv_result = &mut recv_task => {
                    per_stream_token.cancel();
                    let _ = tokio::time::timeout(
                        Duration::from_millis(STREAM_TEARDOWN_DRAIN_TIMEOUT_MS),
                        &mut send_task,
                    )
                    .await;
                    if !send_task.is_finished() {
                        send_task.abort();
                    }
                    match recv_result {
                        Ok(Err(e)) => Err(e),
                        Err(e) => Err(ZerobusError::UnexpectedStreamResponseError(
                            format!("Receiver task panicked: {}", e)
                        )),
                        Ok(Ok(())) => {
                            info!("Receiver task completed successfully");
                            Ok(())
                        }
                    }
                }
                send_result = &mut send_task => {
                    // Draining the recv_task prevents RST_STREAM(CANCEL) from being sent alongside END_STREAM.
                    if matches!(send_result, Ok(Ok(()))) && cancellation_token.is_cancelled() {
                        recv_drain_token.cancel();
                        let _ = tokio::time::timeout(
                            Duration::from_millis(STREAM_TEARDOWN_DRAIN_TIMEOUT_MS),
                            &mut recv_task,
                        )
                        .await;
                    }
                    recv_task.abort();
                    match send_result {
                        Ok(Err(e)) => Err(e),
                        Err(e) => Err(ZerobusError::UnexpectedStreamResponseError(
                            format!("Sender task panicked: {}", e)
                        )),
                        Ok(Ok(())) => Ok(()) // This only happens when the sender task receives a cancellation signal.
                    }
                }
            };

            // 5. Handle errors.
            if let Err(error) = result {
                error!(stream_id = %stream_id, "Stream failure detected: {}", error);
                let error = match &error {
                    // Mapping this to pass certain e2e tests.
                    // TODO: Remove this once we fix tests.
                    ZerobusError::StreamClosedError(status)
                        if status.code() == tonic::Code::InvalidArgument =>
                    {
                        ZerobusError::InvalidArgument(status.message().to_string())
                    }
                    _ => error,
                };
                let _ = server_error_tx.send(Some(error.clone()));
                if !error.is_retryable() || !options.recovery {
                    is_closed.store(true, Ordering::Relaxed);
                    // A mid-stream auth rejection means the cached token is no
                    // longer accepted; drop it so the next stream re-mints.
                    if error.is_auth_rejection() {
                        headers_provider.invalidate().await;
                    }
                    Self::fail_all_pending_records(
                        landing_zone.clone(),
                        oneshot_map.clone(),
                        failed_records.clone(),
                        &error,
                        &callback_tx,
                    )
                    .await;
                    return Err(error);
                }
            }
        }
    }

    /// Reconciles the retained landing-zone tail with the server's durable
    /// position when resuming a persistent stream: removes the prefix of records
    /// the server has already committed (logical offset ≤ `watermark`) and
    /// resolves their pending acks locally, so they are never re-sent.
    ///
    /// Re-sending an already-committed offset would be rejected by the server
    /// and kill the stream (Decision 6: server rejects), so this realignment is
    /// what makes resume survive a lost-ack race. It is not user-duplicate
    /// dedup — the records removed here are ones the SDK itself queued and the
    /// server durably stored; only their acknowledgement was lost with the
    /// dropped connection.
    ///
    /// The caller must have reset observation first, so every retained record
    /// sits in the landing-zone queue in ascending offset order — the committed
    /// ones therefore form a contiguous front prefix. Each removed offset is
    /// acked to its waiter, reported to the ack callback, and advances the
    /// last-received-offset watermark, matching what a real server ack would do.
    async fn reconcile_committed_on_resume(
        landing_zone: &RecordLandingZone,
        oneshot_map: &Arc<tokio::sync::Mutex<OneshotMap>>,
        logical_last_received_offset_id_tx: &tokio::sync::watch::Sender<Option<OffsetId>>,
        callback_tx: &Option<tokio::sync::mpsc::UnboundedSender<CallbackMessage>>,
        watermark: OffsetId,
    ) {
        let committed = landing_zone.remove_front_while(|item| item.offset_id <= watermark);
        if committed.is_empty() {
            return;
        }
        let reconciled_records: usize =
            committed.iter().map(|r| r.payload.get_record_count()).sum();
        let mut map = oneshot_map.lock().await;
        let mut highest = None;
        for item in &committed {
            if let Some(sender) = map.remove(&item.offset_id) {
                let _ = sender.send(Ok(item.offset_id));
            }
            if let Some(tx) = callback_tx {
                let _ = tx.send(CallbackMessage::Ack(item.offset_id));
            }
            highest = Some(item.offset_id);
        }
        drop(map);
        if let Some(highest) = highest {
            let _ = logical_last_received_offset_id_tx.send(Some(highest));
        }
        info!(
            watermark,
            reconciled_batches = committed.len(),
            reconciled_records,
            "Persistent resume: reconciled already-committed records at/below the watermark"
        );
    }

    /// Fails all pending records by removing them from the landing zone and sending error to all pending acks promises.
    pub(super) async fn fail_all_pending_records(
        landing_zone: RecordLandingZone,
        oneshot_map: Arc<tokio::sync::Mutex<OneshotMap>>,
        failed_records: Arc<RwLock<Vec<EncodedBatch>>>,
        error: &ZerobusError,
        callback_tx: &Option<tokio::sync::mpsc::UnboundedSender<CallbackMessage>>,
    ) {
        // Hold the `failed_records` lock across the landing-zone drain so this
        // serializes with `get_unacked_batches` (which drains the zone under
        // the same lock): whichever runs first gets the records, and the other
        // sees them in `failed_records`. Extend rather than overwrite for the
        // same reason.
        let mut failed = failed_records.write().await;
        failed.reserve(landing_zone.len());
        let records = landing_zone.remove_all();
        if !records.is_empty() {
            let unacked_records: usize = records.iter().map(|r| r.payload.get_record_count()).sum();
            error!(
                unacked_batches = records.len(),
                unacked_records,
                "Stream failed; {} batches ({} records) left unacknowledged and retained for retrieval via get_unacked_records/get_unacked_batches: {}",
                records.len(),
                unacked_records,
                error
            );
        }
        let mut map = oneshot_map.lock().await;
        let error_message = error.to_string();
        for record in records {
            failed.push(record.payload);
            if let Some(sender) = map.remove(&record.offset_id) {
                let _ = sender.send(Err(error.clone()));
            }
            if let Some(tx) = callback_tx {
                let _ = tx.send(CallbackMessage::Error(
                    record.offset_id,
                    error_message.clone(),
                ));
            }
        }
    }
}
