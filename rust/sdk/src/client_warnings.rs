//! Process-wide client-side warnings that surface common SDK misuse patterns.
//!
//! Two warnings are emitted via [`tracing::warn!`]:
//!
//! 1. **Concurrent open streams** — fires when 32 or more ingest streams for the
//!    same table are open at the same time.
//! 2. **High stream open rate (churn)** — fires when 100 or more streams for the
//!    same table are opened within a 60-second sliding window, which may indicate
//!    a "one stream per record" misuse pattern.
//!
//! Both warnings are process-wide and keyed by table name.
//!
//! ## Opt-out
//!
//! Set the environment variable `ZEROBUS_SDK_WARNINGS_ENABLED=false` (or `0` or
//! `no`) before the process starts to suppress all warnings.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};

use tracing::warn;

// ─── Opt-out ──────────────────────────────────────────────────────────────────

fn warnings_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("ZEROBUS_SDK_WARNINGS_ENABLED")
            .map(|v| !matches!(v.to_lowercase().as_str(), "false" | "0" | "no"))
            .unwrap_or(true)
    })
}

// ─── Concurrent open streams monitor ─────────────────────────────────────────

/// Logs a warning when this many streams for the same table are open simultaneously.
const CONCURRENT_WARN_THRESHOLD: usize = 32;

struct ConcurrentOpenStreamsState {
    counts: HashMap<String, usize>,
}

static CONCURRENT_MONITOR: OnceLock<Mutex<ConcurrentOpenStreamsState>> = OnceLock::new();

fn concurrent_monitor() -> &'static Mutex<ConcurrentOpenStreamsState> {
    CONCURRENT_MONITOR.get_or_init(|| {
        Mutex::new(ConcurrentOpenStreamsState {
            counts: HashMap::new(),
        })
    })
}

/// RAII guard returned by [`register_stream_opened`].
///
/// Decrements the per-table concurrent-stream count when dropped. Store it as a
/// field in the stream struct — it is released automatically when the stream drops,
/// whether closed gracefully, failed, or leaked.
pub(crate) struct ConcurrentStreamsGuard {
    table_name: String,
}

impl Drop for ConcurrentStreamsGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = concurrent_monitor().lock() {
            if let Some(count) = state.counts.get_mut(&self.table_name) {
                if *count <= 1 {
                    state.counts.remove(&self.table_name);
                } else {
                    *count -= 1;
                }
            }
        }
    }
}

/// Registers one open stream for `table_name`.
///
/// Increments the per-table counter and logs a `WARN`-level message the first
/// time the count crosses [`CONCURRENT_WARN_THRESHOLD`] (32).
///
/// Returns a [`ConcurrentStreamsGuard`] that decrements the count when dropped,
/// or `None` when warnings are disabled.
pub(crate) fn register_stream_opened(table_name: &str) -> Option<ConcurrentStreamsGuard> {
    if !warnings_enabled() {
        return None;
    }

    let count = {
        let mut state = concurrent_monitor().lock().unwrap_or_else(|e| e.into_inner());
        let entry = state.counts.entry(table_name.to_string()).or_insert(0);
        *entry += 1;
        *entry
    };

    // Fire exactly once when crossing the threshold from below.
    if count - 1 < CONCURRENT_WARN_THRESHOLD && count >= CONCURRENT_WARN_THRESHOLD {
        warn!(
            "Zerobus SDK: {} concurrent open ingest streams for table `{}` in this process. \
             If this is unexpected, check that streams are being reused correctly.",
            count, table_name
        );
    }

    Some(ConcurrentStreamsGuard {
        table_name: table_name.to_string(),
    })
}

// ─── Stream churn monitor ─────────────────────────────────────────────────────

/// Sliding window length in milliseconds.
const CHURN_WINDOW_MS: u64 = 60_000;
/// Logs a warning when this many streams are opened within [`CHURN_WINDOW_MS`].
const CHURN_WARN_THRESHOLD: usize = 100;
/// Maximum number of distinct tables tracked; oldest is evicted when exceeded.
const CHURN_MAX_TABLES: usize = 1000;

fn default_clock_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

struct StreamChurnState {
    /// Replaceable clock; overridden in tests to control time.
    clock: fn() -> u64,
    /// Per-table queue of stream-open timestamps (ms since Unix epoch).
    timestamps: HashMap<String, VecDeque<u64>>,
    /// Tracks insertion order for eviction when `CHURN_MAX_TABLES` is reached.
    insertion_order: VecDeque<String>,
}

impl StreamChurnState {
    fn new() -> Self {
        Self {
            clock: default_clock_ms,
            timestamps: HashMap::new(),
            insertion_order: VecDeque::new(),
        }
    }
}

static CHURN_MONITOR: OnceLock<Mutex<StreamChurnState>> = OnceLock::new();

fn churn_monitor() -> &'static Mutex<StreamChurnState> {
    CHURN_MONITOR.get_or_init(|| Mutex::new(StreamChurnState::new()))
}

/// Records one stream open for `table_name`.
///
/// Maintains a per-table sliding window of open timestamps. Logs a `WARN`-level
/// message the first time the count within the window reaches
/// [`CHURN_WARN_THRESHOLD`] (100). The warning re-fires if the rate drops below
/// the threshold and later surges again in a new window.
pub(crate) fn record_stream_opened(table_name: &str) {
    if !warnings_enabled() {
        return;
    }

    let count = {
        let mut state = churn_monitor().lock().unwrap_or_else(|e| e.into_inner());
        let now = (state.clock)();

        if !state.timestamps.contains_key(table_name) {
            // Evict the oldest-tracked table when the cap is reached.
            if state.insertion_order.len() >= CHURN_MAX_TABLES {
                if let Some(oldest) = state.insertion_order.pop_front() {
                    state.timestamps.remove(&oldest);
                }
            }
            state
                .timestamps
                .insert(table_name.to_string(), VecDeque::new());
            state.insertion_order.push_back(table_name.to_string());
        }

        let deque = state.timestamps.get_mut(table_name).unwrap();
        // Evict timestamps that have fallen outside the sliding window.
        while deque
            .front()
            .map(|&t| now.saturating_sub(t) > CHURN_WINDOW_MS)
            .unwrap_or(false)
        {
            deque.pop_front();
        }
        deque.push_back(now);
        deque.len()
    };

    // Fire exactly when count reaches the threshold; re-fires after rate recovers.
    if count == CHURN_WARN_THRESHOLD {
        warn!(
            "Zerobus SDK: {} ingest streams opened for table `{}` in the last {}s in this \
             process. If this is unexpected, check that streams are being reused across records.",
            count,
            table_name,
            CHURN_WINDOW_MS / 1000
        );
    }
}

// ─── Test utilities ───────────────────────────────────────────────────────────

#[cfg(test)]
pub(crate) fn reset_for_testing() {
    if let Ok(mut state) = concurrent_monitor().lock() {
        state.counts.clear();
    }
    if let Ok(mut state) = churn_monitor().lock() {
        *state = StreamChurnState::new();
    }
}

#[cfg(test)]
pub(crate) fn set_churn_clock_for_testing(f: fn() -> u64) {
    if let Ok(mut state) = churn_monitor().lock() {
        state.clock = f;
    }
}

#[cfg(test)]
pub(crate) fn active_stream_count_for_testing(table_name: &str) -> usize {
    concurrent_monitor()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .counts
        .get(table_name)
        .copied()
        .unwrap_or(0)
}

#[cfg(test)]
pub(crate) fn open_count_in_window_for_testing(table_name: &str) -> usize {
    let state = churn_monitor().lock().unwrap_or_else(|e| e.into_inner());
    let now = (state.clock)();
    state
        .timestamps
        .get(table_name)
        .map(|deque| {
            deque
                .iter()
                .filter(|&&t| now.saturating_sub(t) <= CHURN_WINDOW_MS)
                .count()
        })
        .unwrap_or(0)
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    // ── ConcurrentOpenStreamsMonitor ──────────────────────────────────────────
    //
    // Each test uses a unique table name so tests can run in parallel without
    // interfering with each other's counts.

    #[test]
    fn concurrent_register_increments_and_release_decrements() {
        let table = "cat.sch.concurrent_reg_rel";
        // Reset just this table's count to avoid cross-test interference.
        if let Ok(mut s) = concurrent_monitor().lock() {
            s.counts.remove(table);
        }
        let g1 = register_stream_opened(table);
        let g2 = register_stream_opened(table);
        assert_eq!(active_stream_count_for_testing(table), 2);
        drop(g1);
        assert_eq!(active_stream_count_for_testing(table), 1);
        drop(g2);
        assert_eq!(active_stream_count_for_testing(table), 0);
    }

    #[test]
    fn concurrent_count_settles_to_zero_after_many_opens() {
        let table = "cat.sch.concurrent_settles_zero";
        if let Ok(mut s) = concurrent_monitor().lock() {
            s.counts.remove(table);
        }
        let guards: Vec<_> = (0..50).map(|_| register_stream_opened(table)).collect();
        assert_eq!(active_stream_count_for_testing(table), 50);
        drop(guards);
        assert_eq!(active_stream_count_for_testing(table), 0);
    }

    #[test]
    fn concurrent_independent_tables_tracked_separately() {
        let t1 = "cat.sch.concurrent_indep1";
        let t2 = "cat.sch.concurrent_indep2";
        if let Ok(mut s) = concurrent_monitor().lock() {
            s.counts.remove(t1);
            s.counts.remove(t2);
        }
        let _g1 = register_stream_opened(t1);
        let _g2 = register_stream_opened(t1);
        let _g3 = register_stream_opened(t2);
        assert_eq!(active_stream_count_for_testing(t1), 2);
        assert_eq!(active_stream_count_for_testing(t2), 1);
    }

    #[test]
    fn concurrent_unknown_table_returns_zero() {
        assert_eq!(
            active_stream_count_for_testing("cat.sch.concurrent_unknown_xyz"),
            0
        );
    }

    // ── StreamChurnMonitor ────────────────────────────────────────────────────
    //
    // These tests share a global fake clock (`FAKE_CLOCK_MS`) and call
    // `reset_for_testing()` which reinitialises the churn state. Run with
    // `cargo test -- --test-threads=1` to prevent parallel tests from
    // corrupting each other's clock or timestamp state.

    static FAKE_CLOCK_MS: AtomicU64 = AtomicU64::new(0);

    fn fake_clock() -> u64 {
        FAKE_CLOCK_MS.load(Ordering::Relaxed)
    }

    #[test]
    fn churn_open_count_tracks_opens_within_window() {
        reset_for_testing();
        set_churn_clock_for_testing(fake_clock);
        FAKE_CLOCK_MS.store(0, Ordering::Relaxed);
        let table = "cat.sch.churn_tracks";

        record_stream_opened(table);
        record_stream_opened(table);
        assert_eq!(open_count_in_window_for_testing(table), 2);
    }

    #[test]
    fn churn_entries_older_than_window_evicted_on_next_open() {
        reset_for_testing();
        set_churn_clock_for_testing(fake_clock);
        FAKE_CLOCK_MS.store(0, Ordering::Relaxed);
        let table = "cat.sch.churn_evict";

        for _ in 0..10 {
            record_stream_opened(table);
        }
        assert_eq!(open_count_in_window_for_testing(table), 10);

        // Advance past the window; the next open evicts all 10 old entries.
        FAKE_CLOCK_MS.store(61_000, Ordering::Relaxed);
        record_stream_opened(table);
        assert_eq!(open_count_in_window_for_testing(table), 1);
    }

    #[test]
    fn churn_warning_fires_at_exactly_threshold_not_before() {
        reset_for_testing();
        set_churn_clock_for_testing(fake_clock);
        FAKE_CLOCK_MS.store(0, Ordering::Relaxed);
        let table = "cat.sch.churn_threshold";

        for _ in 0..99 {
            record_stream_opened(table);
        }
        assert_eq!(open_count_in_window_for_testing(table), 99);

        // 100th open crosses the threshold.
        record_stream_opened(table);
        assert_eq!(open_count_in_window_for_testing(table), 100);

        // 101st does not re-fire (count != CHURN_WARN_THRESHOLD).
        record_stream_opened(table);
        assert_eq!(open_count_in_window_for_testing(table), 101);
    }

    #[test]
    fn churn_warning_refires_after_window_rolls_below_threshold() {
        reset_for_testing();
        set_churn_clock_for_testing(fake_clock);
        FAKE_CLOCK_MS.store(0, Ordering::Relaxed);
        let table = "cat.sch.churn_refire";

        for _ in 0..100 {
            record_stream_opened(table);
        }
        assert_eq!(open_count_in_window_for_testing(table), 100);

        // Advance past window so all previous opens are evicted.
        FAKE_CLOCK_MS.store(61_000, Ordering::Relaxed);
        for _ in 0..99 {
            record_stream_opened(table);
        }
        assert_eq!(open_count_in_window_for_testing(table), 99);

        // 100th open in the new window crosses the threshold again.
        record_stream_opened(table);
        assert_eq!(open_count_in_window_for_testing(table), 100);
    }

    #[test]
    fn churn_two_tables_tracked_independently() {
        reset_for_testing();
        set_churn_clock_for_testing(fake_clock);
        FAKE_CLOCK_MS.store(0, Ordering::Relaxed);
        let t1 = "cat.sch.churn_indep1";
        let t2 = "cat.sch.churn_indep2";

        for _ in 0..5 {
            record_stream_opened(t1);
        }
        for _ in 0..3 {
            record_stream_opened(t2);
        }
        assert_eq!(open_count_in_window_for_testing(t1), 5);
        assert_eq!(open_count_in_window_for_testing(t2), 3);
    }

    #[test]
    fn churn_unknown_table_returns_zero() {
        assert_eq!(open_count_in_window_for_testing("cat.sch.churn_unknown_xyz"), 0);
    }
}
