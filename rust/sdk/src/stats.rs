//! Pluggable stream telemetry.
//!
//! A [`StatsExporter`] receives typed [`StreamStat`] events from a stream (batch
//! sends with byte sizes, acknowledgements, reconnects) and routes them into
//! whatever metrics system the caller uses. Register one on the stream builder
//! with `.stats_exporter(...)`.
//!
//! **Beta**: currently emitted only by Arrow Flight streams (behind the
//! `arrow-flight` feature). The event set is `#[non_exhaustive]` and may grow.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::offset_generator::OffsetId;

/// Byte-size and count stats for an encoded batch.
///
/// Payload only — the offset that identifies the batch is carried separately on
/// [`StreamStat::BatchSent`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchStats {
    /// Number of rows in the batch.
    pub records: u64,
    /// Bytes actually placed on the wire (Arrow IPC frame, after IPC compression
    /// when enabled).
    pub wire_bytes: u64,
    /// Uncompressed Arrow payload size, independent of the wire codec.
    /// Best-effort: Arrow buffer bytes, excluding IPC framing.
    pub uncompressed_bytes: u64,
}

/// A telemetry event emitted by a stream. All variants are owned and `Copy`, so an
/// event can be forwarded across a channel directly (see [`ChannelExporter`]).
///
/// An *offset* identifies one ingest call (one batch), not one row — a batch of
/// 1000 rows has a single offset. Byte sizes are therefore per batch.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamStat {
    /// A batch was encoded and sent on the wire. Emitted at send time, so it counts
    /// every transmission: a batch replayed during recovery is re-sent and emits again.
    BatchSent {
        /// Offset assigned to the batch by `ingest_batch`.
        offset: OffsetId,
        /// Byte-size and row-count stats for the batch.
        stats: BatchStats,
    },
    /// A batch was durably acknowledged by the server. Pure durability signal —
    /// byte sizes are reported by [`BatchSent`](Self::BatchSent).
    BatchAcked {
        /// Offset assigned to the batch by `ingest_batch`.
        offset: OffsetId,
    },
    /// The stream reconnected after a transient failure.
    Reconnected {
        /// 1-based recovery attempt that succeeded.
        attempt: u32,
    },
}

/// Sink for stream telemetry. Register with `.stats_exporter(...)` on the builder.
///
/// `record` runs inline on the stream's hot / IO tasks, so it must be cheap and
/// non-blocking — do lightweight work (increment a counter, forward to a channel).
/// Use [`ChannelExporter`] to hand stats off to another task for heavier handling.
pub trait StatsExporter: Send + Sync {
    /// Handle one telemetry event. Must not block.
    fn record(&self, stat: StreamStat);
}

/// A [`StatsExporter`] that forwards events to a bounded channel, dropping (and
/// counting) events when the channel is full so a slow consumer never stalls
/// ingestion. Drain the paired receiver from [`channel_exporter`].
pub struct ChannelExporter {
    tx: mpsc::Sender<StreamStat>,
    dropped: AtomicU64,
}

impl ChannelExporter {
    /// Number of events dropped because the channel was full.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl StatsExporter for ChannelExporter {
    fn record(&self, stat: StreamStat) {
        if self.tx.try_send(stat).is_err() {
            // Full or closed: drop. Stats are best-effort; never block ingest.
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Creates a [`ChannelExporter`] with a bounded buffer of `capacity` events and
/// returns it with the receiver to drain elsewhere.
pub fn channel_exporter(capacity: usize) -> (Arc<ChannelExporter>, mpsc::Receiver<StreamStat>) {
    let (tx, rx) = mpsc::channel(capacity.max(1));
    (
        Arc::new(ChannelExporter {
            tx,
            dropped: AtomicU64::new(0),
        }),
        rx,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn channel_exporter_delivers_and_drops_when_full() {
        let (exporter, mut rx) = channel_exporter(1);
        exporter.record(StreamStat::BatchSent {
            offset: 7,
            stats: BatchStats {
                records: 3,
                wire_bytes: 100,
                uncompressed_bytes: 120,
            },
        });
        // Buffer (cap 1) is now full; this one is dropped and counted.
        exporter.record(StreamStat::Reconnected { attempt: 1 });

        assert_eq!(
            rx.recv().await.unwrap(),
            StreamStat::BatchSent {
                offset: 7,
                stats: BatchStats {
                    records: 3,
                    wire_bytes: 100,
                    uncompressed_bytes: 120,
                },
            }
        );
        assert_eq!(exporter.dropped(), 1);
    }
}
