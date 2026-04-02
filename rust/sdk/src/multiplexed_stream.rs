use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

use futures::future::join_all;

use crate::{
    EncodedBatch, EncodedRecord, OffsetId, ZerobusError, ZerobusResult, ZerobusStream,
};

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
        write!(f, "MessageId(stream={}, offset={})", self.stream_index(), self.sub_offset())
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
    failed_records: Mutex<Vec<EncodedRecord>>,
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
            failed_records: Mutex::new(Vec::new()),
        }
    }

    fn check_closed(&self) -> ZerobusResult<()> {
        if self.is_closed.load(Ordering::Relaxed) {
            return Err(ZerobusError::InvalidStateError(
                "MultiplexedStream is closed".to_string(),
            ));
        }
        Ok(())
    }

    async fn shutdown_on_failure(&self) {
        if self.is_closed.swap(true, Ordering::Relaxed) {
            return;
        }

        let _ = join_all(self.streams.iter().map(|s| s.flush())).await;

        let mut all_unacked = Vec::new();
        for stream in &self.streams {
            if let Ok(records) = stream.get_unacked_records().await {
                all_unacked.extend(records);
            }
        }

        self.failed_records
            .lock()
            .expect("failed_records lock poisoned")
            .extend(all_unacked);
    }

    async fn ingest_to_substream<F>(
        &self,
        ingest_fn: F,
    ) -> ZerobusResult<Option<MessageId>>
    where
        F: for<'a> Fn(
            &'a ZerobusStream,
        )
            -> Pin<Box<dyn Future<Output = ZerobusResult<Option<OffsetId>>> + 'a>>,
    {
        self.check_closed()?;

        let idx = self.round_robin_counter.fetch_add(1, Ordering::Relaxed) % self.streams.len();
        let stream = &self.streams[idx];

        loop {
            if stream.is_closed() {
                self.shutdown_on_failure().await;
                return Err(ZerobusError::InvalidStateError(
                    format!("Sub-stream {} closed unexpectedly", idx),
                ));
            }

            if stream.has_capacity() {
                match ingest_fn(stream).await {
                    Ok(sub_offset) => {
                        return Ok(sub_offset.map(|off| MessageId::new(idx, off)));
                    }
                    Err(e) => {
                        self.shutdown_on_failure().await;
                        return Err(e);
                    }
                }
            }

            // Stream is full — back off to let sender tasks drain the landing zone
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    pub async fn ingest_record(
        &self,
        payload: impl Into<EncodedRecord>,
    ) -> ZerobusResult<MessageId> {
        let record = payload.into();
        self.ingest_to_substream(|stream| {
            let record = record.clone();
            Box::pin(async move { stream.ingest_record_offset(record).await.map(Some) })
        })
        .await
        .map(|opt| opt.unwrap())
    }

    pub async fn ingest_records<I, T>(
        &self,
        payload: I,
    ) -> ZerobusResult<Option<MessageId>>
    where
        I: IntoIterator<Item = T>,
        T: Into<EncodedRecord>,
    {
        let records: Vec<EncodedRecord> = payload.into_iter().map(Into::into).collect();
        if records.is_empty() {
            return Ok(None);
        }
        self.ingest_to_substream(|stream| {
            let records = records.clone();
            Box::pin(async move { stream.ingest_records_offset(records).await })
        })
        .await
    }

    pub async fn flush(&self) -> ZerobusResult<()> {
        self.check_closed()?;
        let results = join_all(self.streams.iter().map(|s| s.flush())).await;
        let mut first_error = None;
        for result in results {
            if let Err(e) = result {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
        match first_error {
            Some(e) => Err(e),
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
        self.streams[idx]
            .wait_for_offset(message_id.sub_offset())
            .await
    }

    pub async fn close(&mut self) -> ZerobusResult<()> {
        self.is_closed.store(true, Ordering::Relaxed);

        let mut first_error: Option<ZerobusError> = None;

        let flush_results = join_all(self.streams.iter().map(|s| s.flush())).await;
        for result in flush_results {
            if let Err(e) = result {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }

        for stream in &mut self.streams {
            if let Err(e) = stream.close().await {
                if first_error.is_none() {
                    first_error = Some(e);
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

    pub async fn get_unacked_records(
        &self,
    ) -> ZerobusResult<impl Iterator<Item = EncodedRecord>> {
        let mut all_records = self
            .failed_records
            .lock()
            .expect("failed_records lock poisoned")
            .clone();
        for stream in &self.streams {
            if let Ok(records) = stream.get_unacked_records().await {
                all_records.extend(records);
            }
        }
        Ok(all_records.into_iter())
    }

    pub async fn get_unacked_batches(&self) -> ZerobusResult<Vec<EncodedBatch>> {
        let mut all_batches = Vec::new();
        for stream in &self.streams {
            if let Ok(batches) = stream.get_unacked_batches().await {
                all_batches.extend(batches);
            }
        }
        Ok(all_batches)
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
