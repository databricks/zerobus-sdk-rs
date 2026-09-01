//! Bounded per-offset encoded byte-size accounting for the Arrow stream.
//!
//! The Flight encoder captures each batch's sizes as it runs. [`BatchStatsTracker`]
//! records them, bounded to a fixed number of recent offsets, so
//! [`ZerobusArrowStream::take_offset_details`] can report them without
//! re-serialising the `RecordBatch`.
//!
//! [`ZerobusArrowStream::take_offset_details`]: super::ZerobusArrowStream::take_offset_details

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::offset_generator::OffsetId;

/// Encoded byte sizes reported by
/// [`ZerobusArrowStream::take_offset_details`](super::ZerobusArrowStream::take_offset_details).
///
/// **Beta**: part of the Beta Arrow API and may change before GA. Sizes are
/// best-effort instrumentation captured as the SDK encodes each batch, and
/// accumulate across every transmission (a batch re-sent during recovery counts
/// each send).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OffsetDetails {
    /// Full FlightData frame placed on the wire for this offset's batch (Arrow IPC
    /// header + body + app metadata, **after** IPC compression when enabled), including
    /// IPC framing overhead. The actual bytes sent over the network.
    pub wire_byte_size: u64,
    /// Uncompressed Arrow data payload size of this offset's batch, measured from the
    /// batch buffers and **independent of the wire compression codec** — the same batch
    /// reports the same value whether or not compression is on. Best-effort: it is the
    /// Arrow buffer bytes and excludes IPC framing (header + inter-buffer padding), so
    /// with compression off it is somewhat below `wire_byte_size`. Dictionary-encoded
    /// columns over-count: this counts the full dictionary each batch, while IPC sends
    /// the dictionary once and only the keys per batch.
    pub uncompressed_byte_size: u64,
    /// Running total of [`wire_byte_size`](Self::wire_byte_size) across the stream
    /// at the moment this offset's batch was last encoded. Counts retransmits, so a
    /// resend refreshes it to the later, larger total — values are not ordered by
    /// offset. The stream's monotonic running total, not a per-offset prefix sum.
    pub cumulative_wire_byte_size: u64,
    /// Running total of [`uncompressed_byte_size`](Self::uncompressed_byte_size)
    /// across the stream at the moment this offset's batch was last encoded. Counts
    /// retransmits, so a resend refreshes it to the later, larger total — values are
    /// not ordered by offset. The stream's monotonic running total, not a per-offset
    /// prefix sum.
    pub cumulative_uncompressed_byte_size: u64,
}

/// Bounded, self-contained byte-size accounting shared with the Flight encoder.
///
/// The stream holds one `Arc<BatchStatsTracker>`; the encoder closure gets a
/// clone and calls [`record`](Self::record), while `take_offset_details` calls
/// [`take`](Self::take).
pub(super) struct BatchStatsTracker {
    /// Per-offset details, bounded to `cap` entries; the smallest offset is
    /// evicted first.
    details: Mutex<BTreeMap<OffsetId, OffsetDetails>>,
    /// Monotonic total wire bytes sent on this stream.
    cumulative_wire: AtomicU64,
    /// Monotonic total uncompressed (pre-compression) bytes sent on this stream.
    cumulative_uncompressed: AtomicU64,
    cap: usize,
}

impl BatchStatsTracker {
    pub(super) fn new(cap: usize) -> Self {
        Self {
            details: Mutex::new(BTreeMap::new()),
            cumulative_wire: AtomicU64::new(0),
            cumulative_uncompressed: AtomicU64::new(0),
            cap: cap.max(1),
        }
    }

    /// Adds `wire` (post-compression on-the-wire) and `uncompressed`
    /// (pre-compression encoded) bytes against `offset`, bumping both monotonic
    /// running totals. Either may be `0` when only one is known at the call site.
    /// Accumulates across retransmits. A single call keeps this off the hot path
    /// cheap — one lock, one map entry.
    pub(super) fn record(&self, offset: OffsetId, wire: u64, uncompressed: u64) {
        let cumulative_wire = self.cumulative_wire.fetch_add(wire, Ordering::Relaxed) + wire;
        let cumulative_uncompressed = self
            .cumulative_uncompressed
            .fetch_add(uncompressed, Ordering::Relaxed)
            + uncompressed;
        let mut details = self.details.lock().unwrap();
        let entry = details.entry(offset).or_default();
        entry.wire_byte_size += wire;
        entry.uncompressed_byte_size += uncompressed;
        entry.cumulative_wire_byte_size = cumulative_wire;
        entry.cumulative_uncompressed_byte_size = cumulative_uncompressed;
        // Bound the map by evicting the smallest offset once over the cap. Consumers
        // drain entries they wait on, so steady state stays near the in-flight count.
        while details.len() > self.cap {
            details.pop_first();
        }
    }

    /// Removes and returns the details for `offset`, or `None` on miss.
    pub(super) fn take(&self, offset: OffsetId) -> Option<OffsetDetails> {
        self.details.lock().unwrap().remove(&offset)
    }
}

#[cfg(test)]
mod tests {
    use super::{BatchStatsTracker, OffsetDetails};

    #[test]
    fn accumulates_both_totals_and_take_consumes() {
        let tracker = BatchStatsTracker::new(16);
        tracker.record(0, 100, 120);
        tracker.record(1, 40, 50);

        assert_eq!(
            tracker.take(0),
            Some(OffsetDetails {
                wire_byte_size: 100,
                uncompressed_byte_size: 120,
                cumulative_wire_byte_size: 100,
                cumulative_uncompressed_byte_size: 120,
            })
        );
        assert_eq!(
            tracker.take(1),
            Some(OffsetDetails {
                wire_byte_size: 40,
                uncompressed_byte_size: 50,
                cumulative_wire_byte_size: 140,
                cumulative_uncompressed_byte_size: 170,
            })
        );
        // Consumed on read: a second take misses and returns None.
        assert_eq!(tracker.take(0), None);
    }

    #[test]
    fn split_calls_accumulate_into_one_entry() {
        // Wire and uncompressed can be recorded in separate calls (as the compressed
        // path does: uncompressed on batch pull, wire on frame emit); both land on
        // the same offset entry with independent running totals.
        let tracker = BatchStatsTracker::new(16);
        tracker.record(3, 0, 200); // uncompressed only (input side)
        tracker.record(3, 90, 0); // wire only (output side)

        assert_eq!(
            tracker.take(3),
            Some(OffsetDetails {
                wire_byte_size: 90,
                uncompressed_byte_size: 200,
                cumulative_wire_byte_size: 90,
                cumulative_uncompressed_byte_size: 200,
            })
        );
    }

    #[test]
    fn resend_accumulates_per_offset_and_cumulative() {
        // A batch re-sent during recovery is recorded again at the same offset;
        // both the per-offset sizes and the running totals count every transmission.
        let tracker = BatchStatsTracker::new(16);
        tracker.record(6, 100, 100);
        tracker.record(6, 60, 60); // recovery replay (e.g. a partial suffix)

        assert_eq!(
            tracker.take(6),
            Some(OffsetDetails {
                wire_byte_size: 160,
                uncompressed_byte_size: 160,
                cumulative_wire_byte_size: 160,
                cumulative_uncompressed_byte_size: 160,
            })
        );
    }

    #[test]
    fn cap_evicts_smallest_offset() {
        let tracker = BatchStatsTracker::new(2);
        tracker.record(0, 10, 10);
        tracker.record(1, 10, 10);
        tracker.record(2, 10, 10); // overflows: offset 0 evicted

        assert_eq!(tracker.take(0), None);
        assert_eq!(tracker.take(1).unwrap().wire_byte_size, 10);
        assert_eq!(tracker.take(2).unwrap().wire_byte_size, 10);
    }
}
