// Zerobus C++ SDK — ingestion throughput benchmark.
//
// Measures how close the SDK gets to a target throughput (e.g. ~100 MB/s) of
// durably-acknowledged ingestion against the real endpoint, comparing the JSON
// and protobuf wire formats across a sweep of record sizes.
//
// Method (see bench_config.hpp for the knobs):
//   * Pre-encode a fixed pool of distinct records BEFORE timing, so the
//     measurement covers transport (SDK + FFI + gRPC + TLS + network + ack),
//     not serialization.
//   * Send a warmup batch and flush it (pays the one-time TLS/OAuth cost)
//     outside the clock.
//   * Time a tight loop of batch ingests (queue only — never wait per record),
//     flushing periodically, then a final flush so every byte is durably acked
//     when the clock stops.
//   * Report queued throughput (ingest loop only, an upper bound) and — the
//     honest figure — acked throughput (including the flush).
//
// Only the OAuth client credentials come from the environment; connection info
// is in demo_config.hpp. Optional env overrides:
//   ZEROBUS_BENCH_TOTAL_BYTES, ZEROBUS_BENCH_RECORDS_PER_BATCH,
//   ZEROBUS_BENCH_MAX_INFLIGHT, ZEROBUS_BENCH_FLUSH_EVERY_N.
//
//   export ZEROBUS_CLIENT_ID=... ZEROBUS_CLIENT_SECRET=...
//   ./build/examples/zerobus_benchmark
//
// NOTE: this writes many real rows to the target table, and the measured
// ceiling reflects this host's network path to the endpoint — it measures the
// SDK *and* the link, not the SDK in isolation.

#include <google/protobuf/descriptor.pb.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "air_quality.pb.h"
#include "bench_config.hpp"
#include "demo_config.hpp"
#include "zerobus/zerobus.hpp"

namespace {

using clock_type = std::chrono::steady_clock;

std::string require_env(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') {
    std::cerr << "error: environment variable " << name << " is not set.\n";
    std::exit(2);
  }
  return value;
}

// Optional numeric env override; returns `fallback` if unset/empty.
std::uint64_t env_u64(const char* name, std::uint64_t fallback) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') return fallback;
  return std::strtoull(value, nullptr, 10);
}

// Record-size sweep: comma-separated bytes from ZEROBUS_BENCH_SIZES, else the
// compiled-in default. Lets you push past 16 KiB (e.g. "16384,65536,262144,
// 1048576") without recompiling.
std::vector<std::size_t> record_sizes() {
  const char* value = std::getenv("ZEROBUS_BENCH_SIZES");
  if (value == nullptr || *value == '\0') {
    return std::vector<std::size_t>(
        std::begin(zerobus_bench::kRecordSizesBytes),
        std::end(zerobus_bench::kRecordSizesBytes));
  }
  std::vector<std::size_t> sizes;
  const std::string s(value);
  std::size_t pos = 0;
  while (pos < s.size()) {
    std::size_t comma = s.find(',', pos);
    if (comma == std::string::npos) comma = s.size();
    const std::string tok = s.substr(pos, comma - pos);
    if (!tok.empty()) {
      const unsigned long long v = std::strtoull(tok.c_str(), nullptr, 10);
      if (v > 0) sizes.push_back(static_cast<std::size_t>(v));
    }
    pos = comma + 1;
  }
  return sizes;
}

// Single-message DescriptorProto for the generated AirQuality message (what a
// proto stream needs). Copied from proto_static.cpp.
std::vector<std::uint8_t> air_quality_descriptor_bytes() {
  google::protobuf::DescriptorProto descriptor;
  zerobus_demo::AirQuality::descriptor()->CopyTo(&descriptor);
  std::string serialized;
  descriptor.SerializeToString(&serialized);
  return std::vector<std::uint8_t>(serialized.begin(), serialized.end());
}

// Pad `device_name` so the encoded record lands near `target_bytes`. The other
// two columns and the JSON/proto framing add a small fixed overhead, so the
// actual size is measured after building and used for all accounting.
std::string device_name_of(std::size_t target_bytes, int seed) {
  // Reserve a little for the temp/humidity fields + framing; never negative.
  const std::size_t overhead = 48;
  std::size_t pad = target_bytes > overhead ? target_bytes - overhead : 1;
  std::string name = "dev-" + std::to_string(seed) + "-";
  if (name.size() < pad) name.append(pad - name.size(), 'x');
  return name;
}

std::string make_json_record(std::size_t target_bytes, int seed) {
  const std::string name = device_name_of(target_bytes, seed);
  return "{\"device_name\": \"" + name +
         "\", \"temp\": " + std::to_string(20 + (seed % 15)) +
         ", \"humidity\": " + std::to_string(40 + (seed % 40)) + "}";
}

std::vector<std::uint8_t> make_proto_record(std::size_t target_bytes,
                                            int seed) {
  zerobus_demo::AirQuality msg;
  msg.set_device_name(device_name_of(target_bytes, seed));
  msg.set_temp(20 + (seed % 15));
  msg.set_humidity(40 + (seed % 40));
  std::string serialized;
  msg.SerializeToString(&serialized);
  return std::vector<std::uint8_t>(serialized.begin(), serialized.end());
}

struct RunResult {
  const char* format;
  std::size_t record_size_actual;
  std::size_t records_per_batch;
  std::uint64_t total_records;
  std::uint64_t total_bytes;
  double queued_secs;
  double wall_secs;
  bool ok;
};

double mib_per_s(std::uint64_t bytes, double secs) {
  if (secs <= 0.0) return 0.0;
  return static_cast<double>(bytes) / (1024.0 * 1024.0) / secs;
}
double mb_per_s_decimal(std::uint64_t bytes, double secs) {
  if (secs <= 0.0) return 0.0;
  return static_cast<double>(bytes) / 1e6 / secs;
}

// How many distinct records to pre-encode into the cycled pool. Bounded by
// BOTH a record count (kRecordPoolCount) and a total-memory budget (~256 MiB),
// so large records don't blow up RAM (e.g. 1 MiB * 4096 would be 4 GiB). Always
// at least a handful so cycling still varies the payload.
std::size_t pool_count_for(std::size_t rec_size) {
  const std::size_t kPoolMemBudget = 256ull * 1024 * 1024;
  std::size_t by_mem = kPoolMemBudget / (rec_size > 0 ? rec_size : 1);
  std::size_t n = by_mem < zerobus_bench::kRecordPoolCount
                      ? by_mem
                      : zerobus_bench::kRecordPoolCount;
  if (n < 8) n = 8;
  return n;
}

// records_per_batch capped so a batch's raw encoded bytes stay under the core's
// per-request payload ceiling (with a safety margin for framing).
std::size_t cap_records_per_batch(std::size_t configured,
                                  std::size_t rec_size) {
  std::size_t cap =
      static_cast<std::size_t>((zerobus_bench::kMaxIngestPayloadBytes *
                                zerobus_bench::kBatchSafetyMargin) /
                               (rec_size > 0 ? rec_size : 1));
  if (cap < 1) cap = 1;
  return configured < cap ? configured : cap;
}

// Build the SDK once; reused across all runs.
zerobus::Sdk build_sdk(const std::string& client_id,
                       const std::string& client_secret) {
  (void)client_id;
  (void)client_secret;
  return zerobus::Sdk::builder()
      .endpoint(zerobus_demo::kZerobusEndpoint)
      .unity_catalog_url(zerobus_demo::kWorkspaceUrl)
      .application_name("cpp-benchmark")
      .build();
}

zerobus::StreamOptions make_options(zerobus::RecordType type) {
  zerobus::StreamOptions opts;
  opts.record_type = type;
  opts.max_inflight_requests = static_cast<std::size_t>(env_u64(
      "ZEROBUS_BENCH_MAX_INFLIGHT", zerobus_bench::kMaxInflightRequests));
  return opts;
}

// One worker's timed portion, shared by the proto and JSON paths. Operates on a
// stream the coordinator already created (streams are not thread-safe, so each
// thread owns exactly one). Reads from the shared read-only `pool` and copies
// each batch into its own buffer, so concurrent workers never touch shared
// mutable state. `ingest(stream, batch)` is the format-specific batch call.
//
// Barrier: after its own warmup+flush the worker bumps `ready` and spins until
// `go`, so the coordinator can start the clock only once every stream is warm
// (TLS/OAuth paid). Timestamps are captured locally and aggregated by the
// coordinator.
template <typename Rec, typename IngestFn>
void worker_body(zerobus::Stream* stream, const std::vector<Rec>* pool,
                 std::uint64_t my_records, std::size_t records_per_batch,
                 int flush_every_n, IngestFn ingest, std::atomic<int>* ready,
                 std::atomic<bool>* go, std::uint64_t* out_bytes,
                 clock_type::time_point* out_queued,
                 clock_type::time_point* out_end) {
  std::vector<Rec> batch;
  batch.reserve(records_per_batch);
  auto fill = [&](std::uint64_t start, std::size_t n) {
    batch.clear();
    for (std::size_t k = 0; k < n; ++k)
      batch.push_back((*pool)[(start + k) % pool->size()]);
  };

  // Warmup (UNTIMED), then wait at the barrier.
  for (int w = 0; w < zerobus_bench::kWarmupBatches; ++w) {
    fill(0, records_per_batch);
    ingest(*stream, batch);
  }
  stream->flush();
  ready->fetch_add(1);
  while (!go->load()) std::this_thread::yield();

  std::uint64_t sent = 0;
  std::uint64_t bytes = 0;
  int batch_index = 0;
  while (sent < my_records) {
    std::size_t n = records_per_batch;
    if (sent + n > my_records) n = static_cast<std::size_t>(my_records - sent);
    fill(sent, n);
    for (std::size_t k = 0; k < n; ++k) bytes += batch[k].size();
    ingest(*stream, batch);
    sent += n;
    ++batch_index;
    if (flush_every_n > 0 && batch_index % flush_every_n == 0) stream->flush();
  }
  *out_queued = clock_type::now();
  stream->flush();
  *out_end = clock_type::now();
  *out_bytes = bytes;
  stream->close();
}

// Coordinate `num_streams` workers, each on its own thread and stream,
// splitting the total volume. Returns aggregate throughput: total bytes across
// all streams over the wall span from the shared start (all warm) to the last
// stream's final flush. `pool` is prebuilt; `make_props` yields fresh
// TableProperties per stream; `ingest` is the format-specific batch call.
template <typename Rec, typename MakePropsFn, typename IngestFn>
RunResult run_parallel(const char* fmt, zerobus::Sdk& sdk,
                       const std::string& client_id,
                       const std::string& client_secret,
                       zerobus::RecordType type, const std::vector<Rec>& pool,
                       std::size_t rec_size_actual, std::uint64_t total_bytes,
                       std::size_t configured_rpb, int flush_every_n,
                       int num_streams, MakePropsFn make_props,
                       IngestFn ingest) {
  RunResult r{fmt, rec_size_actual, 0, 0, 0, 0.0, 0.0, false};
  if (num_streams < 1) num_streams = 1;
  r.records_per_batch = cap_records_per_batch(configured_rpb, rec_size_actual);
  r.total_records = total_bytes / (rec_size_actual ? rec_size_actual : 1);
  if (r.total_records < static_cast<std::uint64_t>(num_streams))
    r.total_records = static_cast<std::uint64_t>(num_streams);

  // Create all streams up front, sequentially on this thread (avoids relying on
  // concurrent Sdk::create_stream). reserve() so the vector never reallocates
  // and invalidates the pointers the threads hold.
  std::vector<zerobus::Stream> streams;
  streams.reserve(static_cast<std::size_t>(num_streams));
  for (int i = 0; i < num_streams; ++i) {
    streams.push_back(sdk.create_stream(make_props(), client_id, client_secret,
                                        make_options(type)));
  }

  const std::uint64_t per_stream = r.total_records / num_streams;
  const std::uint64_t remainder = r.total_records % num_streams;

  std::atomic<int> ready{0};
  std::atomic<bool> go{false};
  std::vector<std::uint64_t> bytes(num_streams, 0);
  std::vector<clock_type::time_point> queued(num_streams);
  std::vector<clock_type::time_point> ended(num_streams);
  std::vector<std::thread> threads;
  threads.reserve(static_cast<std::size_t>(num_streams));

  for (int i = 0; i < num_streams; ++i) {
    const std::uint64_t my_records =
        per_stream + (static_cast<std::uint64_t>(i) < remainder ? 1 : 0);
    threads.emplace_back(worker_body<Rec, IngestFn>, &streams[i], &pool,
                         my_records, r.records_per_batch, flush_every_n, ingest,
                         &ready, &go, &bytes[i], &queued[i], &ended[i]);
  }

  // Wait until every stream is warm, then start the shared clock.
  while (ready.load() < num_streams) std::this_thread::yield();
  const auto t0 = clock_type::now();
  go.store(true);
  for (auto& t : threads) t.join();
  const auto t1 = clock_type::now();

  std::uint64_t total = 0;
  clock_type::time_point last_queued = t0;
  for (int i = 0; i < num_streams; ++i) {
    total += bytes[i];
    if (queued[i] > last_queued) last_queued = queued[i];
  }
  r.total_bytes = total;
  r.queued_secs = std::chrono::duration<double>(last_queued - t0).count();
  r.wall_secs = std::chrono::duration<double>(t1 - t0).count();
  r.ok = true;
  return r;
}

// --- Proto run -------------------------------------------------------------
RunResult run_proto(zerobus::Sdk& sdk, const std::string& client_id,
                    const std::string& client_secret, std::size_t target_size,
                    std::uint64_t total_bytes, std::size_t configured_rpb,
                    int flush_every_n, int num_streams) {
  // Pre-encode the shared pool (UNTIMED).
  const std::size_t pool_count = pool_count_for(target_size);
  std::vector<std::vector<std::uint8_t>> pool;
  pool.reserve(pool_count);
  std::uint64_t pool_bytes = 0;
  for (std::size_t i = 0; i < pool_count; ++i) {
    pool.push_back(make_proto_record(target_size, static_cast<int>(i)));
    pool_bytes += pool.back().size();
  }
  const std::size_t rec_size =
      static_cast<std::size_t>(pool_bytes / pool.size());

  const std::vector<std::uint8_t> descriptor = air_quality_descriptor_bytes();
  auto make_props = [&]() {
    zerobus::TableProperties props;
    props.table_name = zerobus_demo::table_name();
    props.descriptor_proto = descriptor;
    return props;
  };
  auto ingest = [](zerobus::Stream& s,
                   const std::vector<std::vector<std::uint8_t>>& b) {
    s.ingest_proto_records(b);
  };

  return run_parallel("proto", sdk, client_id, client_secret,
                      zerobus::RecordType::Proto, pool, rec_size, total_bytes,
                      configured_rpb, flush_every_n, num_streams, make_props,
                      ingest);
}

// --- JSON run --------------------------------------------------------------
RunResult run_json(zerobus::Sdk& sdk, const std::string& client_id,
                   const std::string& client_secret, std::size_t target_size,
                   std::uint64_t total_bytes, std::size_t configured_rpb,
                   int flush_every_n, int num_streams) {
  const std::size_t pool_count = pool_count_for(target_size);
  std::vector<std::string> pool;
  pool.reserve(pool_count);
  std::uint64_t pool_bytes = 0;
  for (std::size_t i = 0; i < pool_count; ++i) {
    pool.push_back(make_json_record(target_size, static_cast<int>(i)));
    pool_bytes += pool.back().size();
  }
  const std::size_t rec_size =
      static_cast<std::size_t>(pool_bytes / pool.size());

  auto make_props = [&]() {
    zerobus::TableProperties props;
    props.table_name = zerobus_demo::table_name();  // empty descriptor => JSON
    return props;
  };
  auto ingest = [](zerobus::Stream& s, const std::vector<std::string>& b) {
    s.ingest_json_records(b);
  };

  return run_parallel("json", sdk, client_id, client_secret,
                      zerobus::RecordType::Json, pool, rec_size, total_bytes,
                      configured_rpb, flush_every_n, num_streams, make_props,
                      ingest);
}

void print_header() {
  std::printf("\n%-6s %10s %8s %12s %10s %12s %12s %11s\n", "format",
              "rec_bytes", "rec/bat", "total_recs", "total_MiB", "queued_MiB/s",
              "acked_MiB/s", "acked_MB/s");
  std::printf(
      "------ ---------- -------- ------------ ---------- ------------ "
      "------------ -----------\n");
}

void print_row(const RunResult& r) {
  if (!r.ok) {
    std::printf("%-6s %10zu %8s %12s %10s %12s %12s %11s\n", r.format,
                r.record_size_actual, "-", "-", "-", "-", "FAILED", "-");
    return;
  }
  const double total_mib =
      static_cast<double>(r.total_bytes) / (1024.0 * 1024.0);
  std::printf("%-6s %10zu %8zu %12llu %10.1f %12.1f %12.1f %11.1f\n", r.format,
              r.record_size_actual, r.records_per_batch,
              static_cast<unsigned long long>(r.total_records), total_mib,
              mib_per_s(r.total_bytes, r.queued_secs),
              mib_per_s(r.total_bytes, r.wall_secs),
              mb_per_s_decimal(r.total_bytes, r.wall_secs));
}

}  // namespace

int main() {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  const std::string client_id = require_env("ZEROBUS_CLIENT_ID");
  const std::string client_secret = require_env("ZEROBUS_CLIENT_SECRET");

  const std::uint64_t total_bytes =
      env_u64("ZEROBUS_BENCH_TOTAL_BYTES", zerobus_bench::kTargetTotalBytes);
  const std::size_t configured_rpb = static_cast<std::size_t>(env_u64(
      "ZEROBUS_BENCH_RECORDS_PER_BATCH", zerobus_bench::kRecordsPerBatch));
  const int flush_every_n = static_cast<int>(env_u64(
      "ZEROBUS_BENCH_FLUSH_EVERY_N", zerobus_bench::kFlushEveryNBatches));
  int num_streams = static_cast<int>(env_u64("ZEROBUS_BENCH_STREAMS", 1));
  if (num_streams < 1) num_streams = 1;

  std::printf("Zerobus C++ throughput benchmark\n");
  std::printf("  table            : %s\n", zerobus_demo::table_name().c_str());
  std::printf("  endpoint         : %s\n", zerobus_demo::kZerobusEndpoint);
  std::printf("  total bytes/run  : %.1f MiB (split across streams)\n",
              static_cast<double>(total_bytes) / (1024.0 * 1024.0));
  std::printf("  parallel streams : %d\n", num_streams);
  std::printf("  flush every N    : %d batch(es) (0 = final flush only)\n",
              flush_every_n);
  std::printf(
      "\n  queued_MiB/s = ingest loop only (upper bound, NOT durable)\n"
      "  acked_*      = includes final flush; every byte durably acked, "
      "aggregate across all streams (the honest number)\n");

  try {
    zerobus::Sdk sdk = build_sdk(client_id, client_secret);
    print_header();
    for (std::size_t size : record_sizes()) {
      RunResult p = run_proto(sdk, client_id, client_secret, size, total_bytes,
                              configured_rpb, flush_every_n, num_streams);
      print_row(p);
      RunResult j = run_json(sdk, client_id, client_secret, size, total_bytes,
                             configured_rpb, flush_every_n, num_streams);
      print_row(j);
    }
  } catch (const zerobus::ZerobusException& e) {
    std::cerr << "\nZerobus error: " << e.what()
              << " (retryable=" << (e.is_retryable() ? "true" : "false")
              << ")\n";
    return 1;
  }

  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
