#ifndef ZEROBUS_EXAMPLES_BENCH_CONFIG_HPP
#define ZEROBUS_EXAMPLES_BENCH_CONFIG_HPP

// Tuning knobs for the throughput benchmark (benchmark.cpp). Kept separate from
// demo_config.hpp (which holds the shared, non-secret connection info the
// benchmark reuses: table_name(), kZerobusEndpoint, kWorkspaceUrl). Every knob
// here has an env-var override read in benchmark.cpp, so you can sweep without
// recompiling.

#include <cstddef>
#include <cstdint>

namespace zerobus_bench {

// Core per-request payload ceiling: a single batch's total encoded bytes must
// stay under this or the ingest call throws. Mirrors the Rust core's
// max_ingest_payload_bytes (10 MiB - 64 KiB).
inline constexpr std::size_t kMaxIngestPayloadBytes =
    10 * 1024 * 1024 - 64 * 1024;

// Keep each batch comfortably under the cap: the core adds a little per-record
// framing on top of the raw encoded bytes we count.
inline constexpr double kBatchSafetyMargin = 0.90;

// Record sizes (approx encoded bytes) to sweep. Throughput is expected to climb
// with size — small records are dominated by per-batch/FFI framing overhead.
inline constexpr std::size_t kRecordSizesBytes[] = {256, 1024, 4096, 16384};

// Total data volume pushed per run. Sized so the timed window is several
// seconds at ~100 MB/s (512 MiB ~= 5s floor). Raise (e.g. 2 GiB) for a headline
// number where ramp-up is negligible. Override with ZEROBUS_BENCH_TOTAL_BYTES.
inline constexpr std::uint64_t kTargetTotalBytes = 512ull * 1024 * 1024;

// A fixed pool of distinct pre-encoded records that the timed loop cycles
// through, so memory stays bounded (record_size * kRecordPoolCount) regardless
// of total volume. Distinct records avoid trivial wire-side dedup/compression.
inline constexpr std::size_t kRecordPoolCount = 4096;

// Flush every N batches during the run (0 = single final flush only). Periodic
// flushing bounds outstanding data, keeps each flush well under
// flush_timeout_ms (5 min), and keeps the sender from tripping
// server_lack_of_ack_timeout_ms (60s). All flush time is inside the timed
// window either way.
inline constexpr int kFlushEveryNBatches = 32;

// Upper bound on configured records per batch (the payload-cap formula lowers
// this further for large records). Override with
// ZEROBUS_BENCH_RECORDS_PER_BATCH.
inline constexpr std::size_t kRecordsPerBatch = 2048;

// In-flight (unacked) batch window. The core default; batches, not records, so
// it is not the throughput bottleneck here. Override with
// ZEROBUS_BENCH_MAX_INFLIGHT.
inline constexpr std::size_t kMaxInflightRequests = 1'000'000;

// Batches sent + flushed before the clock starts, to pay the one-time TLS
// handshake / gRPC establishment / OAuth token exchange outside the
// measurement.
inline constexpr int kWarmupBatches = 1;

}  // namespace zerobus_bench

#endif  // ZEROBUS_EXAMPLES_BENCH_CONFIG_HPP
