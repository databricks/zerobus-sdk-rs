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

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use futures::future::join_all;
use tracing::{error, info, warn};

use crate::{EncodedBatch, EncodedRecord, OffsetId, ZerobusError, ZerobusResult, ZerobusStream};

const CAPACITY_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

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

/// Distributes ingestion round-robin across a fixed set of [`ZerobusStream`]s.
///
/// See the [module-level documentation](self) for routing, `MessageId`, and
/// poisoning semantics.
pub struct MultiplexedStream {
    streams: Vec<ZerobusStream>,
    round_robin_counter: AtomicUsize,
    is_closed: AtomicBool,
    admission: tokio::sync::RwLock<()>,
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
            admission: tokio::sync::RwLock::new(()),
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

        error!(
            trigger_stream_index = trigger_index,
            cause = %cause,
            num_streams = self.streams.len(),
            "MultiplexedStream poisoned due to sub-stream failure"
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
        for s in &self.streams {
            s.signal_shutdown();
        }
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
        let mut backoff_ms = 1u64;
        let mut logged_backpressure = false;
        let started_at = tokio::time::Instant::now();
        let deadline = started_at + CAPACITY_WAIT_TIMEOUT;

        loop {
            self.check_closed()?;

            if stream.is_closed() {
                let err = ZerobusError::InvalidStateError(format!(
                    "Sub-stream {} closed unexpectedly",
                    idx
                ));
                self.shutdown_on_failure(idx, &err).await;
                return Err(err);
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(ZerobusError::ConnectionTimeout(format!(
                    "Timed out waiting for capacity on multiplexed sub-stream {}",
                    idx
                )));
            }

            let wait_duration = std::time::Duration::from_millis(backoff_ms)
                .min(deadline.saturating_duration_since(now));

            match tokio::time::timeout(wait_duration, stream.reserve_capacity()).await {
                Ok(Ok(reservation)) => return Ok(reservation),
                Ok(Err(e)) => return Err(self.handle_ingest_error(e, stream, idx).await),
                // Timed out waiting for a permit: the sub-stream is still at
                // capacity. Loop to re-check liveness and keep waiting.
                Err(_elapsed) => {}
            }

            backoff_ms = (backoff_ms * 2).min(50);

            let total_wait_ms = started_at.elapsed().as_millis();
            if !logged_backpressure && total_wait_ms >= 1000 {
                warn!(
                    stream_index = idx,
                    total_wait_ms, "Backpressure: sub-stream at capacity, waiting for drain"
                );
                logged_backpressure = true;
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
        let encoded_batch = stream.prepare_record_batch(record)?;
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
        let encoded_batch = stream.prepare_records_batch(records)?;
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
}
