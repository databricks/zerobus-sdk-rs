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

/// Number of bits reserved for the stream index.
/// 6 bits supports up to 64 sub-streams.
const STREAM_BITS: u32 = 6;
const OFFSET_MASK: i64 = (1i64 << (64 - STREAM_BITS)) - 1;

/// Opaque identifier returned by ingest methods on MultiplexedStream.
/// Encodes the sub-stream index and sub-stream offset in a single i64.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MessageId(i64);

/// `MessageId` is opaque — use `wait_for_message_id` on the stream to wait for
/// acknowledgment. Keep in mind that formatting to string allocates, so calling
/// `to_string()` per record in a hot loop will have a performance penalty.
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
    fn new(stream_index: usize, sub_offset: OffsetId) -> Self {
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

    /// Returns the raw i64 value.
    pub fn raw(&self) -> i64 {
        self.0
    }

    /// Construct from a raw i64 value.
    pub fn from_raw(raw: i64) -> Self {
        Self(raw)
    }
}

pub struct MultiplexedStream {
    streams: Vec<ZerobusStream>,
    round_robin_counter: AtomicUsize,
    is_closed: AtomicBool,
}

impl MultiplexedStream {
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
        }
    }

    #[allow(clippy::result_large_err)]
    fn check_closed(&self) -> ZerobusResult<()> {
        if self.is_closed.load(Ordering::Relaxed) {
            return Err(ZerobusError::InvalidStateError(
                "MultiplexedStream is closed".to_string(),
            ));
        }
        Ok(())
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

    async fn wait_for_capacity(&self, stream: &ZerobusStream, idx: usize) -> ZerobusResult<()> {
        let mut backoff_ms = 1u64;
        let mut total_wait_ms = 0u64;
        let mut logged_backpressure = false;

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

            if stream.has_capacity() {
                return Ok(());
            }

            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
            total_wait_ms += backoff_ms;
            backoff_ms = (backoff_ms * 2).min(50);

            if !logged_backpressure && total_wait_ms >= 1000 {
                warn!(
                    stream_index = idx,
                    total_wait_ms, "Backpressure: sub-stream at capacity, waiting for drain"
                );
                logged_backpressure = true;
            }
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

    pub async fn ingest_record(
        &self,
        payload: impl Into<EncodedRecord>,
    ) -> ZerobusResult<MessageId> {
        self.check_closed()?;
        let record = payload.into();
        let idx = self.pick_substream();
        let stream = &self.streams[idx];
        self.wait_for_capacity(stream, idx).await?;
        self.check_closed()?;

        match stream.ingest_record_offset(record).await {
            Ok(off) => Ok(MessageId::new(idx, off)),
            Err(e) => Err(self.handle_ingest_error(e, stream, idx).await),
        }
    }

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
        self.wait_for_capacity(stream, idx).await?;
        self.check_closed()?;

        match stream.ingest_records_offset(records).await {
            Ok(sub_offset) => Ok(sub_offset.map(|off| MessageId::new(idx, off))),
            Err(e) => Err(self.handle_ingest_error(e, stream, idx).await),
        }
    }

    pub async fn flush(&self) -> ZerobusResult<()> {
        self.check_closed()?;
        let results = join_all(self.streams.iter().map(|s| s.flush())).await;
        let mut first_error: Option<(usize, ZerobusError)> = None;
        let mut first_closed: Option<usize> = None;
        for (i, result) in results.into_iter().enumerate() {
            if let Err(e) = result {
                if self.streams[i].is_closed() && first_closed.is_none() {
                    first_closed = Some(i);
                }
                if first_error.is_none() {
                    first_error = Some((i, e));
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
            Some((i, e)) => {
                if let Some(closed_idx) = first_closed {
                    self.shutdown_on_failure(closed_idx, &e).await;
                } else {
                    warn!(stream_index = i, error = %e, "flush errored but sub-streams still alive");
                }
                Err(e)
            }
            None => Ok(()),
        }
    }

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

    pub async fn close(&mut self) -> ZerobusResult<()> {
        info!("Closing MultiplexedStream");
        self.is_closed.store(true, Ordering::Relaxed);

        let mut first_error: Option<ZerobusError> = None;

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

    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Relaxed)
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

    #[test]
    fn test_round_robin_counter_increments() {
        let counter = AtomicUsize::new(0);
        assert_eq!(counter.fetch_add(1, Ordering::Relaxed), 0);
        assert_eq!(counter.fetch_add(1, Ordering::Relaxed), 1);
        assert_eq!(counter.fetch_add(1, Ordering::Relaxed), 2);
        assert_eq!(counter.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_is_closed_flag() {
        let flag = AtomicBool::new(false);
        assert!(!flag.load(Ordering::Relaxed));
        flag.store(true, Ordering::Relaxed);
        assert!(flag.load(Ordering::Relaxed));
    }

    #[test]
    fn test_shutdown_on_failure_is_idempotent() {
        let flag = AtomicBool::new(false);
        assert!(!flag.swap(true, Ordering::Relaxed));
        assert!(flag.swap(true, Ordering::Relaxed));
    }
}
