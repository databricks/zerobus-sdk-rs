//! Fan-out wrapper that distributes ingestion across multiple [`ZerobusStream`]s.
//!
//! A single `ZerobusStream` is throughput-limited by its in-flight window;
//! `MultiplexedStream` routes records round-robin across a fixed set of
//! sub-streams to raise aggregate throughput. When the chosen sub-stream is at
//! capacity the call awaits drain rather than rerouting, so per-sub-stream
//! ordering is preserved.
//!
//! Each ingest returns an opaque [`MessageId`] that packs the sub-stream index
//! and its offset into a single `i64` (6 bits of stream index → up to 64
//! sub-streams). Callers later pass it to
//! [`wait_for_message_id`](MultiplexedStream::wait_for_message_id) without
//! needing to know which sub-stream handled the record.
//!
//! The mux is poisoned on the first unrecoverable sub-stream error: remaining
//! sub-streams are flushed and further ingest calls fail. Any records still
//! buffered can be recovered via
//! [`get_unacked_records`](MultiplexedStream::get_unacked_records) or
//! [`get_unacked_batches`](MultiplexedStream::get_unacked_batches).

use futures::future::join_all;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::stream::StreamShutdownHandle;
use crate::{
    AckCallback, EncodedBatch, EncodedRecord, OffsetId, ZerobusError, ZerobusResult, ZerobusStream,
};

const CAPACITY_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// Ensures a cancelled poison future still shuts sub-stream tasks down, but
/// only after all ingests admitted before poison have left the admission
/// section. Normal completion disarms the fallback after signaling directly.
struct PoisonCleanupGuard {
    admission: Arc<tokio::sync::RwLock<()>>,
    shutdown_handles: Option<Vec<StreamShutdownHandle>>,
    runtime: tokio::runtime::Handle,
}

impl PoisonCleanupGuard {
    fn new(
        admission: Arc<tokio::sync::RwLock<()>>,
        shutdown_handles: Vec<StreamShutdownHandle>,
    ) -> Self {
        Self {
            admission,
            shutdown_handles: Some(shutdown_handles),
            runtime: tokio::runtime::Handle::current(),
        }
    }

    fn finish(mut self) {
        for handle in self.shutdown_handles.take().unwrap_or_default() {
            handle.signal();
        }
    }
}

impl Drop for PoisonCleanupGuard {
    fn drop(&mut self) {
        let Some(shutdown_handles) = self.shutdown_handles.take() else {
            return;
        };

        warn!("Mux poison cleanup was cancelled; finishing sub-stream shutdown in background");
        let admission = Arc::clone(&self.admission);
        drop(self.runtime.spawn(async move {
            // This is a barrier only. Readers that were already admitted have
            // no further await between their final checks and enqueue.
            {
                let _admission = admission.write().await;
            }
            for handle in shutdown_handles {
                handle.signal();
            }
        }));
    }
}

/// Number of bits reserved for the stream index.
/// 6 bits supports up to 64 sub-streams.
const STREAM_BITS: u32 = 6;
const OFFSET_MASK: i64 = (1i64 << (64 - STREAM_BITS)) - 1;

/// Opaque identifier returned by ingest methods on MultiplexedStream.
/// Encodes the sub-stream index and sub-stream offset in a single i64.
///
/// Unlike a `ZerobusStream` offset, `MessageId` values are not ordered — pass
/// them to [`MultiplexedStream::wait_for_message_id`] to await acknowledgment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MessageId(i64);

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MessageId(stream={}, offset={})",
            self.stream_index(),
            self.sub_offset()
        )
    }
}

impl MessageId {
    pub(crate) fn new(stream_index: usize, sub_offset: OffsetId) -> Self {
        debug_assert!(stream_index < (1 << STREAM_BITS));
        debug_assert!((0..=OFFSET_MASK).contains(&sub_offset));
        Self(((stream_index as i64) << (64 - STREAM_BITS)) | (sub_offset & OFFSET_MASK))
    }

    /// Returns the sub-stream index this message was sent to.
    pub fn stream_index(&self) -> usize {
        ((self.0 as u64) >> (64 - STREAM_BITS)) as usize
    }

    /// Returns the offset within the sub-stream.
    pub fn sub_offset(&self) -> OffsetId {
        self.0 & OFFSET_MASK
    }

    /// Returns the raw i64 value, e.g. for transport across an FFI boundary.
    pub fn raw(&self) -> i64 {
        self.0
    }

    /// Construct from a raw i64 value previously obtained from [`MessageId::raw`].
    ///
    /// Only round-trip values from `raw()`: a fabricated id pointing at an
    /// offset that was never ingested makes `wait_for_message_id` wait until
    /// the flush timeout (indefinitely if none is configured).
    pub fn from_raw(raw: i64) -> Self {
        Self(raw)
    }
}

struct MultiplexedAckCallbackAdapter {
    stream_index: usize,
    callback: Arc<dyn AckCallback<MessageId>>,
}

impl AckCallback for MultiplexedAckCallbackAdapter {
    fn on_ack(&self, offset_id: OffsetId) {
        self.callback
            .on_ack(MessageId::new(self.stream_index, offset_id));
    }

    fn on_error(&self, offset_id: OffsetId, error_message: &str) {
        self.callback
            .on_error(MessageId::new(self.stream_index, offset_id), error_message);
    }
}

/// Creates the callback installed on one multiplexed sub-stream.
///
/// The adapter captures the sub-stream index and converts each stream-local
/// [`OffsetId`] into the [`MessageId`] exposed by [`MultiplexedStream`].
#[allow(dead_code)]
pub(crate) fn multiplexed_ack_callback(
    stream_index: usize,
    callback: Arc<dyn AckCallback<MessageId>>,
) -> Arc<dyn AckCallback> {
    assert!(
        stream_index < (1 << STREAM_BITS),
        "MultiplexedStream supports at most {} sub-streams",
        1 << STREAM_BITS
    );
    Arc::new(MultiplexedAckCallbackAdapter {
        stream_index,
        callback,
    })
}

/// Distributes ingestion round-robin across a fixed set of [`ZerobusStream`]s.
///
/// See the [module-level documentation](self) for routing, `MessageId`, and
/// poisoning semantics.
pub struct MultiplexedStream {
    streams: Vec<ZerobusStream>,
    round_robin_counter: AtomicUsize,
    is_closed: AtomicBool,
    closed_token: CancellationToken,
    failure_context: OnceLock<String>,
    admission: Arc<tokio::sync::RwLock<()>>,
}

impl MultiplexedStream {
    /// Creates a multiplexed stream over the given sub-streams.
    ///
    /// Ingest waits up to 30 seconds for capacity on its selected sub-stream.
    ///
    /// # Panics
    ///
    /// Panics if `streams` is empty or holds more than 64 sub-streams.
    pub fn new(streams: Vec<ZerobusStream>) -> Self {
        assert!(
            !streams.is_empty(),
            "MultiplexedStream requires at least one sub-stream"
        );
        assert!(
            streams.len() <= (1 << STREAM_BITS),
            "MultiplexedStream supports at most {} sub-streams",
            1 << STREAM_BITS
        );
        Self {
            streams,
            round_robin_counter: AtomicUsize::new(0),
            is_closed: AtomicBool::new(false),
            closed_token: CancellationToken::new(),
            failure_context: OnceLock::new(),
            admission: Arc::new(tokio::sync::RwLock::new(())),
        }
    }

    #[allow(clippy::result_large_err)]
    fn check_closed(&self) -> ZerobusResult<()> {
        if self.is_closed_fast() {
            return Err(ZerobusError::InvalidStateError(
                "MultiplexedStream is closed".to_string(),
            ));
        }
        Ok(())
    }

    fn is_closed_fast(&self) -> bool {
        self.is_closed.load(Ordering::Relaxed)
    }

    async fn shutdown_on_failure(&self, trigger_index: usize, cause: &ZerobusError) {
        if self.is_closed.swap(true, Ordering::Relaxed) {
            return;
        }
        let _ = self
            .failure_context
            .set(format!("sub-stream {trigger_index} failed: {cause}"));
        self.closed_token.cancel();

        error!(
            trigger_stream_index = trigger_index,
            cause = %cause,
            num_streams = self.streams.len(),
            "MultiplexedStream poisoned due to sub-stream failure"
        );

        let cleanup = PoisonCleanupGuard::new(
            Arc::clone(&self.admission),
            self.streams
                .iter()
                .map(ZerobusStream::shutdown_handle)
                .collect(),
        );

        // Drain any readers already admitted before `is_closed` was set. The
        // write lock is only a barrier: readers arriving after it is released
        // will observe the closed state and reject the ingest.
        {
            let _admission = self.admission.write().await;
        }

        let flush_results = join_all(self.streams.iter().map(|s| s.flush())).await;
        for (i, result) in flush_results.into_iter().enumerate() {
            if let Err(e) = result {
                warn!(stream_index = i, error = %e, "Failed to flush sub-stream during shutdown");
            }
        }

        // Signal each sub-stream to tear down its background tasks (gRPC
        // connection, supervisor, callback handler). Full join/abort of the
        // task handles happens later in `close` or `Drop`.
        cleanup.finish();
    }

    // TODO: if the picked sub-stream is at capacity, try the next one before
    // falling back to waiting.
    fn pick_substream(&self) -> usize {
        self.round_robin_counter.fetch_add(1, Ordering::Relaxed) % self.streams.len()
    }

    async fn reserve_capacity(
        &self,
        stream: &ZerobusStream,
        idx: usize,
    ) -> ZerobusResult<crate::landing_zone::CapacityReservation> {
        let started_at = tokio::time::Instant::now();
        let timeout_ms = CAPACITY_WAIT_TIMEOUT.as_millis();
        let table_name = stream.table_properties.table_name.as_str();
        let max_inflight_requests = stream.options.max_inflight_requests;

        self.check_closed()?;

        let wait_for_reservation = async {
            let reservation = stream.reserve_capacity();
            tokio::pin!(reservation);

            match tokio::time::timeout(Duration::from_secs(1), &mut reservation).await {
                Ok(result) => return result,
                Err(_) => {
                    let waited_ms = started_at.elapsed().as_millis();
                    warn!(
                        stream_index = idx,
                        table_name,
                        waited_ms,
                        timeout_ms,
                        max_inflight_requests,
                        "Backpressure: sub-stream at capacity, waiting for drain"
                    );
                }
            }

            reservation.await
        };

        let result = tokio::select! {
            result = tokio::time::timeout(CAPACITY_WAIT_TIMEOUT, wait_for_reservation) => result,
            _ = self.closed_token.cancelled() => {
                let waited_ms = started_at.elapsed().as_millis();
                let cause = self.failure_context.get().map_or(
                    "mux closed without a recorded sub-stream failure",
                    String::as_str,
                );
                warn!(
                    stream_index = idx,
                    table_name,
                    waited_ms,
                    max_inflight_requests,
                    cause,
                    "Multiplexed capacity wait cancelled by shutdown"
                );
                return Err(ZerobusError::InvalidStateError(
                    format!(
                        "MultiplexedStream closed after {waited_ms} ms while waiting for capacity on sub-stream {idx} for table {table_name} (max_inflight_requests: {max_inflight_requests}; cause: {cause})"
                    ),
                ));
            }
        };

        match result {
            Ok(Ok(reservation)) => Ok(reservation),
            Ok(Err(e)) => Err(self.handle_ingest_error(e, stream, idx).await),
            Err(_) => {
                let waited_ms = started_at.elapsed().as_millis();
                warn!(
                    stream_index = idx,
                    table_name,
                    waited_ms,
                    timeout_ms,
                    max_inflight_requests,
                    "Timed out waiting for multiplexed sub-stream capacity"
                );
                Err(ZerobusError::ConnectionTimeout(format!(
                    "Timed out after {waited_ms} ms waiting for capacity on multiplexed sub-stream {idx} for table {table_name} (configured timeout: {timeout_ms} ms, max_inflight_requests: {max_inflight_requests})"
                )))
            }
        }
    }

    async fn enqueue_reserved(
        &self,
        stream: &ZerobusStream,
        idx: usize,
        encoded_batch: EncodedBatch,
    ) -> ZerobusResult<MessageId> {
        let reservation = self.reserve_capacity(stream, idx).await?;
        let enqueue_result = stream
            .enqueue_reserved_admitted(encoded_batch, reservation, || async {
                let admission = self.admission.read().await;
                self.check_closed()?;
                Ok(admission)
            })
            .await;

        match enqueue_result {
            Ok(off) => Ok(MessageId::new(idx, off)),
            Err(e) => Err(self.handle_ingest_error(e, stream, idx).await),
        }
    }

    // Only poison the mux when the sub-stream itself has reached a terminal
    // state (`is_closed`): recovery is exhausted or a non-retryable server
    // error fired, so its offsets/pending records are unrecoverable. Other
    // ingest errors (e.g. `InvalidArgument` on a record-type mismatch) leave
    // the sub-stream healthy and would be wrong to escalate — one bad payload
    // shouldn't kill the other sub-streams.
    async fn handle_ingest_error(
        &self,
        e: ZerobusError,
        stream: &ZerobusStream,
        idx: usize,
    ) -> ZerobusError {
        if stream.is_closed() {
            self.shutdown_on_failure(idx, &e).await;
        } else {
            warn!(stream_index = idx, error = %e, "Ingest errored but sub-stream still alive");
        }
        e
    }

    /// Ingests a single record into the next sub-stream (round-robin).
    ///
    /// Returns once the record is queued; use
    /// [`wait_for_message_id`](Self::wait_for_message_id) with the returned id
    /// to await server acknowledgment. If the chosen sub-stream is at capacity,
    /// this waits for it to drain rather than rerouting.
    pub async fn ingest_record(
        &self,
        payload: impl Into<EncodedRecord>,
    ) -> ZerobusResult<MessageId> {
        self.check_closed()?;
        let record = payload.into();
        let idx = self.pick_substream();
        let stream = &self.streams[idx];
        let encoded_batch = stream.prepare_record(record)?;
        self.enqueue_reserved(stream, idx, encoded_batch).await
    }

    /// Ingests a batch of records into a single sub-stream (round-robin).
    ///
    /// The whole batch lands on one sub-stream so a single returned id covers
    /// it. Returns `None` for an empty batch.
    // TODO: Check if there is a performance advantage in splitting this payload in multiple streams
    pub async fn ingest_records<I, T>(&self, payload: I) -> ZerobusResult<Option<MessageId>>
    where
        I: IntoIterator<Item = T>,
        T: Into<EncodedRecord>,
    {
        self.check_closed()?;
        let records: Vec<EncodedRecord> = payload.into_iter().map(Into::into).collect();
        if records.is_empty() {
            return Ok(None);
        }
        let idx = self.pick_substream();
        let stream = &self.streams[idx];
        let encoded_batch = stream.prepare_records(records)?;
        self.enqueue_reserved(stream, idx, encoded_batch)
            .await
            .map(Some)
    }

    /// Waits until every record already queued on every sub-stream is
    /// acknowledged by the server.
    ///
    /// If a sub-stream flush fails because that sub-stream reached a terminal
    /// state, the mux is poisoned. The first flush error is returned;
    /// additional ones are logged.
    pub async fn flush(&self) -> ZerobusResult<()> {
        self.check_closed()?;
        let results = join_all(self.streams.iter().map(|s| s.flush())).await;
        let mut first_error: Option<ZerobusError> = None;
        let mut first_closed: Option<(usize, ZerobusError)> = None;
        for (i, result) in results.into_iter().enumerate() {
            if let Err(e) = result {
                if self.streams[i].is_closed() && first_closed.is_none() {
                    first_closed = Some((i, e.clone()));
                }
                if first_error.is_none() {
                    first_error = Some(e);
                } else {
                    warn!(
                        stream_index = i,
                        error = %e,
                        "Additional sub-stream flush error (first error will be returned)"
                    );
                }
            }
        }
        match first_error {
            Some(e) => {
                if let Some((closed_idx, closed_err)) = first_closed {
                    self.shutdown_on_failure(closed_idx, &closed_err).await;
                } else {
                    warn!(error = %e, "flush errored but sub-streams still alive");
                }
                Err(e)
            }
            None => Ok(()),
        }
    }

    /// Waits for server acknowledgment of the record or batch behind a
    /// [`MessageId`] returned from [`ingest_record`](Self::ingest_record) or
    /// [`ingest_records`](Self::ingest_records).
    pub async fn wait_for_message_id(&self, message_id: MessageId) -> ZerobusResult<()> {
        let idx = message_id.stream_index();
        if idx >= self.streams.len() {
            return Err(ZerobusError::InvalidArgument(format!(
                "Invalid stream index {} in message id",
                idx
            )));
        }
        match self.streams[idx]
            .wait_for_offset(message_id.sub_offset())
            .await
        {
            Ok(()) => Ok(()),
            Err(e) => {
                if self.streams[idx].is_closed() {
                    self.shutdown_on_failure(idx, &e).await;
                } else {
                    warn!(
                        stream_index = idx,
                        error = %e,
                        "wait_for_offset errored but sub-stream still alive"
                    );
                }
                Err(e)
            }
        }
    }

    /// Flushes and closes all sub-streams, releasing their resources.
    ///
    /// The first flush/close error is returned (additional ones are logged);
    /// on error, use [`get_unacked_records`](Self::get_unacked_records) to
    /// recover records that were never acknowledged.
    pub async fn close(&mut self) -> ZerobusResult<()> {
        info!("Closing MultiplexedStream");
        self.is_closed.store(true, Ordering::Relaxed);
        self.closed_token.cancel();

        let mut first_error: Option<ZerobusError> = None;

        // Flush all sub-streams in parallel first; the per-stream `close`
        // below flushes again, but by then each stream is already drained so
        // the sequential pass is cheap.
        let flush_results = join_all(self.streams.iter().map(|s| s.flush())).await;
        for (i, result) in flush_results.into_iter().enumerate() {
            if let Err(e) = result {
                if first_error.is_none() {
                    first_error = Some(e);
                } else {
                    warn!(
                        stream_index = i,
                        error = %e,
                        "Additional sub-stream flush error during close"
                    );
                }
            }
        }

        for (i, stream) in self.streams.iter_mut().enumerate() {
            if let Err(e) = stream.close().await {
                if first_error.is_none() {
                    first_error = Some(e);
                } else {
                    warn!(
                        stream_index = i,
                        error = %e,
                        "Additional sub-stream close error"
                    );
                }
            }
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Returns whether the mux is closed — either via [`close`](Self::close)
    /// or because a sub-stream failure poisoned it.
    pub fn is_closed(&self) -> bool {
        self.is_closed_fast() || self.streams.iter().any(ZerobusStream::is_closed)
    }

    /// Returns records that were ingested but not acknowledged.
    ///
    /// Closes the mux first to ensure all sub-streams have reached their terminal state,
    /// so results are always complete. Any error from close is swallowed — if records can
    /// still be recovered, they will be returned.
    pub async fn get_unacked_records(
        &mut self,
    ) -> ZerobusResult<impl Iterator<Item = EncodedRecord>> {
        let _ = self.close().await;
        let mut all_records = Vec::new();
        for stream in &self.streams {
            all_records.extend(stream.get_unacked_records().await?);
        }
        Ok(all_records.into_iter())
    }

    /// Returns batches that were ingested but not acknowledged.
    ///
    /// Closes the mux first to ensure all sub-streams have reached their terminal state,
    /// so results are always complete. Any error from close is swallowed — if records can
    /// still be recovered, they will be returned.
    pub async fn get_unacked_batches(&mut self) -> ZerobusResult<Vec<EncodedBatch>> {
        let _ = self.close().await;
        let mut all_batches = Vec::new();
        for stream in &self.streams {
            all_batches.extend(stream.get_unacked_batches().await?);
        }
        Ok(all_batches)
    }
}

impl Drop for MultiplexedStream {
    fn drop(&mut self) {
        self.is_closed.store(true, Ordering::Relaxed);
        self.closed_token.cancel();
        // Fire cancellation on every sub-stream in parallel so their
        // background tasks can start unwinding concurrently. The Vec drop
        // below then runs each `ZerobusStream::Drop`, which aborts any
        // JoinHandles that haven't already exited.
        for stream in &self.streams {
            stream.signal_shutdown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingMultiplexedCallback {
        acks: Mutex<Vec<MessageId>>,
        errors: Mutex<Vec<(MessageId, String)>>,
    }

    impl AckCallback<MessageId> for RecordingMultiplexedCallback {
        fn on_ack(&self, message_id: MessageId) {
            self.acks.lock().unwrap().push(message_id);
        }

        fn on_error(&self, message_id: MessageId, error_message: &str) {
            self.errors
                .lock()
                .unwrap()
                .push((message_id, error_message.to_string()));
        }
    }

    #[test]
    #[should_panic(expected = "MultiplexedStream requires at least one sub-stream")]
    fn test_constructor_panics_on_empty_streams() {
        MultiplexedStream::new(vec![]);
    }

    #[test]
    fn test_message_id_roundtrip() {
        for stream_idx in 0..64 {
            for sub_offset in [0i64, 1, 100, 1_000_000, i64::MAX >> STREAM_BITS] {
                let id = MessageId::new(stream_idx, sub_offset);
                assert_eq!(id.stream_index(), stream_idx);
                assert_eq!(id.sub_offset(), sub_offset);
            }
        }
    }

    #[test]
    fn test_message_id_zero() {
        let id = MessageId::new(0, 0);
        assert_eq!(id.raw(), 0);
        assert_eq!(id.stream_index(), 0);
        assert_eq!(id.sub_offset(), 0);
    }

    #[test]
    fn test_message_id_different_streams_same_offset() {
        let a = MessageId::new(0, 42);
        let b = MessageId::new(1, 42);
        assert_ne!(a, b);
        assert_eq!(a.sub_offset(), b.sub_offset());
        assert_ne!(a.stream_index(), b.stream_index());
    }

    #[test]
    fn test_multiplexed_ack_callback_routes_substream_ids() {
        let callback = Arc::new(RecordingMultiplexedCallback::default());
        let stream_0 = multiplexed_ack_callback(0, callback.clone());
        let stream_1 = multiplexed_ack_callback(1, callback.clone());

        stream_0.on_ack(42);
        stream_1.on_ack(42);
        stream_1.on_error(43, "test error");

        assert_eq!(
            callback.acks.lock().unwrap().as_slice(),
            &[MessageId::new(0, 42), MessageId::new(1, 42)]
        );
        assert_eq!(
            callback.errors.lock().unwrap().as_slice(),
            &[(MessageId::new(1, 43), "test error".to_string())]
        );
    }

    #[test]
    #[should_panic(expected = "MultiplexedStream supports at most 64 sub-streams")]
    fn test_multiplexed_ack_callback_rejects_invalid_stream_index() {
        let callback = Arc::new(RecordingMultiplexedCallback::default());
        multiplexed_ack_callback(64, callback);
    }

    #[tokio::test]
    async fn poison_cleanup_survives_cancellation_and_waits_for_admission() {
        let admission = Arc::new(tokio::sync::RwLock::new(()));
        let admitted_ingest = admission.read().await;
        let is_closed = Arc::new(AtomicBool::new(false));
        let terminal_token = CancellationToken::new();
        let cancellation_token = CancellationToken::new();
        let shutdown_handle = StreamShutdownHandle::new(
            Arc::clone(&is_closed),
            terminal_token.clone(),
            cancellation_token.clone(),
        );

        let cleanup = PoisonCleanupGuard::new(Arc::clone(&admission), vec![shutdown_handle]);
        drop(cleanup);
        tokio::task::yield_now().await;

        assert!(!is_closed.load(Ordering::Relaxed));
        assert!(!terminal_token.is_cancelled());
        assert!(!cancellation_token.is_cancelled());

        drop(admitted_ingest);
        terminal_token.cancelled().await;

        assert!(is_closed.load(Ordering::Relaxed));
        assert!(cancellation_token.is_cancelled());
    }
}
