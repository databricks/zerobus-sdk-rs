//! Persistent (Eos) ingestion stream — the public, durable-stream type.
//!
//! `PersistentStream` is a thin wrapper over the shared gRPC stream engine
//! (`ZerobusStream`). It exists so the persistent contract is visible in the
//! type: the durable [`stream_id`](PersistentStream::stream_id) resume handle
//! and the [`last_committed_offset`](PersistentStream::last_committed_offset)
//! resume watermark are surfaced here rather than on the ephemeral stream. All
//! ingestion, flushing, and teardown delegate to the underlying engine, so
//! there is no duplicated stream machinery.
//!
//! Create or resume one via
//! [`ZerobusSdk::persistent_stream_builder`](crate::ZerobusSdk::persistent_stream_builder).

use crate::{EncodedBatch, EncodedRecord, OffsetId, ZerobusResult, ZerobusStream};

/// A durable, recoverable ingestion stream (Eos).
///
/// Unlike an ephemeral [`ZerobusStream`], a persistent stream's identity and
/// committed offset are recorded server-side, so it can be resumed after a
/// restart with exactly-once delivery into Delta. Persist
/// [`stream_id`](Self::stream_id) to reconnect later via
/// [`resume`](crate::PersistentStreamBuilder::resume).
///
/// # Examples
///
/// ```no_run
/// # use databricks_zerobus_ingest_sdk::*;
/// # async fn example(stream: PersistentStream, records: Vec<Vec<u8>>) -> Result<(), ZerobusError> {
/// // Idiomatic flow: ingest in a loop, then flush() once to confirm the batch.
/// for record in records {
///     stream.ingest_record_offset(record).await?;
/// }
/// stream.flush().await?;
/// # Ok(())
/// # }
/// ```
#[non_exhaustive]
pub struct PersistentStream {
    inner: ZerobusStream,
}

impl PersistentStream {
    /// Wraps a persistent-mode engine stream. Constructed by the persistent
    /// stream builder.
    pub(crate) fn new(inner: ZerobusStream) -> Self {
        Self { inner }
    }

    /// The durable stream identity — the resume handle.
    ///
    /// Persist this to reconnect to the same stream after a restart via
    /// [`resume`](crate::PersistentStreamBuilder::resume). Present once the
    /// stream has been opened.
    pub fn stream_id(&self) -> Option<&str> {
        self.inner.stream_id.as_deref()
    }

    /// The offset the server had durably committed at resume time.
    ///
    /// Records ingested after a resume are assigned offsets starting at
    /// `last_committed_offset + 1`. Returns `None` for a freshly created stream
    /// (nothing committed yet).
    pub fn last_committed_offset(&self) -> Option<OffsetId> {
        self.inner.last_committed_offset
    }

    /// Ingests a single record and returns its logical offset once queued.
    ///
    /// See [`ZerobusStream::ingest_record_offset`].
    pub async fn ingest_record_offset(
        &self,
        payload: impl Into<EncodedRecord>,
    ) -> ZerobusResult<OffsetId> {
        self.inner.ingest_record_offset(payload).await
    }

    /// Ingests a batch of records and returns the logical offset once queued.
    ///
    /// See [`ZerobusStream::ingest_records_offset`].
    pub async fn ingest_records_offset<I, T>(&self, payload: I) -> ZerobusResult<Option<OffsetId>>
    where
        I: IntoIterator<Item = T>,
        T: Into<EncodedRecord>,
    {
        self.inner.ingest_records_offset(payload).await
    }

    /// Flushes all currently pending records and waits for their acknowledgments.
    ///
    /// See [`ZerobusStream::flush`].
    pub async fn flush(&self) -> ZerobusResult<()> {
        self.inner.flush().await
    }

    /// Waits for server acknowledgment of a specific logical offset.
    ///
    /// See [`ZerobusStream::wait_for_offset`].
    pub async fn wait_for_offset(&self, offset: OffsetId) -> ZerobusResult<()> {
        self.inner.wait_for_offset(offset).await
    }

    /// Returns whether the stream has been closed.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Closes the stream gracefully after flushing all pending records.
    ///
    /// See [`ZerobusStream::close`].
    pub async fn close(&mut self) -> ZerobusResult<()> {
        self.inner.close().await
    }

    /// Returns all records that were ingested but not acknowledged by the server.
    ///
    /// See [`ZerobusStream::get_unacked_records`].
    pub async fn get_unacked_records(&self) -> ZerobusResult<impl Iterator<Item = EncodedRecord>> {
        self.inner.get_unacked_records().await
    }

    /// Returns unacknowledged records grouped by batch.
    ///
    /// See [`ZerobusStream::get_unacked_batches`].
    pub async fn get_unacked_batches(&self) -> ZerobusResult<Vec<EncodedBatch>> {
        self.inner.get_unacked_batches().await
    }
}
