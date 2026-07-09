// Zerobus C++ SDK — ClickBench (wide-schema) throughput benchmark.
//
// Ingests realistic ClickBench "hits" rows (105 columns) into
// shinkansen.default.clickbench_load_test_zlata, measuring durably-acked
// throughput with the same method and parallel-streams harness as
// benchmark.cpp (the 3-column air_quality benchmark). Kept as a separate file
// so that one stays untouched.
//
// One JSON record builder feeds both formats:
//   * JSON stream ingests the JSON string directly.
//   * Proto stream uses ProtoSchema::from_uc_json() to build the descriptor and
//     encode each JSON record to protobuf bytes — no hand-written 105-column
//     .proto, no protoc, no libprotobuf link.
//
// Env:
//   ZEROBUS_CLIENT_ID / ZEROBUS_CLIENT_SECRET   (required)
//   ZEROBUS_UC_TABLE_JSON                        (enables the proto run; body
//   of
//       GET
//       /api/2.1/unity-catalog/tables/shinkansen.default.clickbench_load_test_zlata)
//   ZEROBUS_BENCH_TOTAL_BYTES, ZEROBUS_BENCH_RECORDS_PER_BATCH,
//   ZEROBUS_BENCH_MAX_INFLIGHT, ZEROBUS_BENCH_FLUSH_EVERY_N,
//   ZEROBUS_BENCH_STREAMS
//
//   export ZEROBUS_CLIENT_ID=... ZEROBUS_CLIENT_SECRET=...
//   ./build/examples/clickbench_benchmark            # JSON only
//   ZEROBUS_UC_TABLE_JSON="$(...)" ./build/examples/clickbench_benchmark  # +
//   proto
//
// Records use realistic per-column values. DATE/TIMESTAMP are encoded as
// INTEGERS (days / micros since epoch) per the proto JSON-mapping rules, not
// strings.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "bench_config.hpp"
#include "demo_config.hpp"
#include "zerobus/zerobus.hpp"

namespace {

using clock_type = std::chrono::steady_clock;

constexpr const char* kTable = "shinkansen.default.clickbench_load_test_zlata";

std::string require_env(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') {
    std::cerr << "error: environment variable " << name << " is not set.\n";
    std::exit(2);
  }
  return value;
}

std::uint64_t env_u64(const char* name, std::uint64_t fallback) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') return fallback;
  return std::strtoull(value, nullptr, 10);
}

// Build one realistic ClickBench "hits" row as a JSON object. Values vary by
// `seed` so rows differ (no trivial wire-side dedup). Column set and types
// match DESCRIBE of the target table (105 columns).
//
// Encoding notes honoring the proto JSON-mapping rules (used by the proto run's
// encode_json; harmless for the JSON run):
//   * timestamp columns (EventTime, ClientEventTime, LocalEventTime) -> micros
//     since epoch, as an integer.
//   * date column (EventDate) -> days since epoch, as an integer.
//   * all integer columns -> JSON numbers; bigint kept within 2^53 so it stays
//     exact as a JSON number.
std::string make_record(int seed) {
  const long long ts_micros = 1'700'000'000'000'000LL + seed * 1'000'000LL;
  const int date_days = 19600 + (seed % 365);
  const int s = seed;

  // A handful of representative string values, sized to keep rows realistic
  // (a real ClickBench row is a few hundred bytes to low KB).
  const std::string url = "http://example.com/path/" + std::to_string(s) +
                          "?q=benchmark&n=" + std::to_string(s % 1000);
  const std::string referer =
      "http://ref.example.com/" + std::to_string(s % 500);
  const std::string title = "Page title number " + std::to_string(s);
  const std::string search = (s % 3 == 0) ? "search phrase example" : "";

  std::string r;
  r.reserve(1024);
  r += "{";
  auto add_i = [&](const char* k, long long v, bool first = false) {
    if (!first) r += ",";
    r += "\"";
    r += k;
    r += "\":";
    r += std::to_string(v);
  };
  auto add_s = [&](const char* k, const std::string& v) {
    r += ",\"";
    r += k;
    r += "\":\"";
    r += v;
    r += "\"";
  };

  add_i("WatchID", 4000000000000000000LL + s, /*first=*/true);
  add_i("JavaEnable", s % 2);
  add_s("Title", title);
  add_i("GoodEvent", 1);
  add_i("EventTime", ts_micros);
  add_i("EventDate", date_days);
  add_i("CounterID", 10000 + (s % 5000));
  add_i("ClientIP", 100000000 + s);
  add_i("RegionID", s % 9000);
  add_i("UserID", 5000000000000000000LL + s);
  add_i("CounterClass", s % 3);
  add_i("OS", s % 100);
  add_i("UserAgent", s % 20);
  add_s("URL", url);
  add_s("Referer", referer);
  add_i("IsRefresh", s % 2);
  add_i("RefererCategoryID", s % 10000);
  add_i("RefererRegionID", s % 9000);
  add_i("URLCategoryID", s % 10000);
  add_i("URLRegionID", s % 9000);
  add_i("ResolutionWidth", 1920);
  add_i("ResolutionHeight", 1080);
  add_i("ResolutionDepth", 24);
  add_i("FlashMajor", 11);
  add_i("FlashMinor", 8);
  add_s("FlashMinor2", "800");
  add_i("NetMajor", 4);
  add_i("NetMinor", 1);
  add_i("UserAgentMajor", 80 + (s % 20));
  add_s("UserAgentMinor", "ab");
  add_i("CookieEnable", 1);
  add_i("JavascriptEnable", 1);
  add_i("IsMobile", s % 2);
  add_i("MobilePhone", s % 10);
  add_s("MobilePhoneModel", (s % 4 == 0) ? "iPhone" : "");
  add_s("Params", "");
  add_i("IPNetworkID", 1000000 + s);
  add_i("TraficSourceID", s % 8);
  add_i("SearchEngineID", s % 40);
  add_s("SearchPhrase", search);
  add_i("AdvEngineID", s % 20);
  add_i("IsArtifical", 0);
  add_i("WindowClientWidth", 1900);
  add_i("WindowClientHeight", 1000);
  add_i("ClientTimeZone", (s % 25) - 12);
  add_i("ClientEventTime", ts_micros);
  add_i("SilverlightVersion1", 0);
  add_i("SilverlightVersion2", 0);
  add_i("SilverlightVersion3", 0);
  add_i("SilverlightVersion4", 0);
  add_s("PageCharset", "utf-8");
  add_i("CodeVersion", 1000000 + s);
  add_i("IsLink", s % 2);
  add_i("IsDownload", s % 2);
  add_i("IsNotBounce", s % 2);
  add_i("FUniqID", 6000000000000000000LL + s);
  add_s("OriginalURL", url);
  add_i("HID", 100000 + s);
  add_i("IsOldCounter", 0);
  add_i("IsEvent", s % 2);
  add_i("IsParameter", s % 2);
  add_i("DontCountHits", 0);
  add_i("WithHash", 0);
  add_s("HitColor", "5");
  add_i("LocalEventTime", ts_micros);
  add_i("Age", s % 100);
  add_i("Sex", s % 2);
  add_i("Income", s % 5);
  add_i("Interests", s % 256);
  add_i("Robotness", 0);
  add_i("RemoteIP", 200000000 + s);
  add_i("WindowName", -s);
  add_i("OpenerName", -s);
  add_i("HistoryLength", s % 20);
  add_s("BrowserLanguage", "en");
  add_s("BrowserCountry", "US");
  add_s("SocialNetwork", "");
  add_s("SocialAction", "");
  add_i("HTTPError", 0);
  add_i("SendTiming", s % 1000);
  add_i("DNSTiming", s % 100);
  add_i("ConnectTiming", s % 200);
  add_i("ResponseStartTiming", s % 300);
  add_i("ResponseEndTiming", s % 400);
  add_i("FetchTiming", s % 500);
  add_i("SocialSourceNetworkID", s % 10);
  add_s("SocialSourcePage", "");
  add_i("ParamPrice", 1000LL * s);
  add_s("ParamOrderID", "");
  add_s("ParamCurrency", "USD");
  add_i("ParamCurrencyID", 840);
  add_s("OpenstatServiceName", "");
  add_s("OpenstatCampaignID", "");
  add_s("OpenstatAdID", "");
  add_s("OpenstatSourceID", "");
  add_s("UTMSource", "");
  add_s("UTMMedium", "");
  add_s("UTMCampaign", "");
  add_s("UTMContent", "");
  add_s("UTMTerm", "");
  add_s("FromTag", "");
  add_i("HasGCLID", 0);
  add_i("RefererHash", 7000000000000000000LL + s);
  add_i("URLHash", 8000000000000000000LL + s);
  add_i("CLID", s);
  r += "}";
  return r;
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
  return secs <= 0.0 ? 0.0
                     : static_cast<double>(bytes) / (1024.0 * 1024.0) / secs;
}
double mb_per_s_decimal(std::uint64_t bytes, double secs) {
  return secs <= 0.0 ? 0.0 : static_cast<double>(bytes) / 1e6 / secs;
}

std::size_t pool_count_for(std::size_t rec_size) {
  const std::size_t kPoolMemBudget = 256ull * 1024 * 1024;
  std::size_t by_mem = kPoolMemBudget / (rec_size > 0 ? rec_size : 1);
  std::size_t n = by_mem < zerobus_bench::kRecordPoolCount
                      ? by_mem
                      : zerobus_bench::kRecordPoolCount;
  if (n < 8) n = 8;
  return n;
}

std::size_t cap_records_per_batch(std::size_t configured,
                                  std::size_t rec_size) {
  std::size_t cap =
      static_cast<std::size_t>((zerobus_bench::kMaxIngestPayloadBytes *
                                zerobus_bench::kBatchSafetyMargin) /
                               (rec_size > 0 ? rec_size : 1));
  if (cap < 1) cap = 1;
  return configured < cap ? configured : cap;
}

zerobus::Sdk build_sdk() {
  return zerobus::Sdk::builder()
      .endpoint(zerobus_demo::kZerobusEndpoint)
      .unity_catalog_url(zerobus_demo::kWorkspaceUrl)
      .application_name("cpp-clickbench-benchmark")
      .build();
}

zerobus::StreamOptions make_options(zerobus::RecordType type) {
  zerobus::StreamOptions opts;
  opts.record_type = type;
  opts.max_inflight_requests = static_cast<std::size_t>(env_u64(
      "ZEROBUS_BENCH_MAX_INFLIGHT", zerobus_bench::kMaxInflightRequests));
  return opts;
}

// One worker's timed portion (see benchmark.cpp for the full rationale). Each
// thread owns one stream (streams are not thread-safe) and reads the shared
// read-only pool, copying each batch into its own buffer.
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

  for (int w = 0; w < zerobus_bench::kWarmupBatches; ++w) {
    fill(0, records_per_batch);
    ingest(*stream, batch);
  }
  stream->flush();
  ready->fetch_add(1);
  while (!go->load()) std::this_thread::yield();

  std::uint64_t sent = 0, bytes = 0;
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

template <typename Rec, typename IngestFn>
RunResult run_parallel(const char* fmt, zerobus::Sdk& sdk,
                       const std::string& client_id,
                       const std::string& client_secret,
                       zerobus::RecordType type,
                       const std::vector<std::uint8_t>& descriptor,
                       const std::vector<Rec>& pool,
                       std::size_t rec_size_actual, std::uint64_t total_bytes,
                       std::size_t configured_rpb, int flush_every_n,
                       int num_streams, IngestFn ingest) {
  RunResult r{fmt, rec_size_actual, 0, 0, 0, 0.0, 0.0, false};
  if (num_streams < 1) num_streams = 1;
  r.records_per_batch = cap_records_per_batch(configured_rpb, rec_size_actual);
  r.total_records = total_bytes / (rec_size_actual ? rec_size_actual : 1);
  if (r.total_records < static_cast<std::uint64_t>(num_streams))
    r.total_records = static_cast<std::uint64_t>(num_streams);

  std::vector<zerobus::Stream> streams;
  streams.reserve(static_cast<std::size_t>(num_streams));
  for (int i = 0; i < num_streams; ++i) {
    zerobus::TableProperties props;
    props.table_name = kTable;
    if (!descriptor.empty()) props.descriptor_proto = descriptor;
    streams.push_back(
        sdk.create_stream(props, client_id, client_secret, make_options(type)));
  }

  const std::uint64_t per_stream = r.total_records / num_streams;
  const std::uint64_t remainder = r.total_records % num_streams;

  std::atomic<int> ready{0};
  std::atomic<bool> go{false};
  std::vector<std::uint64_t> bytes(num_streams, 0);
  std::vector<clock_type::time_point> queued(num_streams), ended(num_streams);
  std::vector<std::thread> threads;
  threads.reserve(static_cast<std::size_t>(num_streams));
  for (int i = 0; i < num_streams; ++i) {
    const std::uint64_t my_records =
        per_stream + (static_cast<std::uint64_t>(i) < remainder ? 1 : 0);
    threads.emplace_back(worker_body<Rec, IngestFn>, &streams[i], &pool,
                         my_records, r.records_per_batch, flush_every_n, ingest,
                         &ready, &go, &bytes[i], &queued[i], &ended[i]);
  }

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

// Build the shared JSON-string pool once (untimed).
std::vector<std::string> build_pool(std::size_t& rec_size_out) {
  std::vector<std::string> probe;
  probe.push_back(make_record(0));
  const std::size_t approx = probe[0].size();
  const std::size_t pool_count = pool_count_for(approx);
  std::vector<std::string> pool;
  pool.reserve(pool_count);
  std::uint64_t bytes = 0;
  for (std::size_t i = 0; i < pool_count; ++i) {
    pool.push_back(make_record(static_cast<int>(i)));
    bytes += pool.back().size();
  }
  rec_size_out = static_cast<std::size_t>(bytes / pool.size());
  return pool;
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
  const std::string client_id = require_env("ZEROBUS_CLIENT_ID");
  const std::string client_secret = require_env("ZEROBUS_CLIENT_SECRET");
  const char* uc_json_env = std::getenv("ZEROBUS_UC_TABLE_JSON");

  const std::uint64_t total_bytes =
      env_u64("ZEROBUS_BENCH_TOTAL_BYTES", zerobus_bench::kTargetTotalBytes);
  const std::size_t configured_rpb = static_cast<std::size_t>(env_u64(
      "ZEROBUS_BENCH_RECORDS_PER_BATCH", zerobus_bench::kRecordsPerBatch));
  const int flush_every_n = static_cast<int>(env_u64(
      "ZEROBUS_BENCH_FLUSH_EVERY_N", zerobus_bench::kFlushEveryNBatches));
  int num_streams = static_cast<int>(env_u64("ZEROBUS_BENCH_STREAMS", 1));
  if (num_streams < 1) num_streams = 1;

  std::printf("Zerobus C++ ClickBench throughput benchmark\n");
  std::printf("  table            : %s\n", kTable);
  std::printf("  endpoint         : %s\n", zerobus_demo::kZerobusEndpoint);
  std::printf("  total bytes/run  : %.1f MiB (split across streams)\n",
              static_cast<double>(total_bytes) / (1024.0 * 1024.0));
  std::printf("  parallel streams : %d\n", num_streams);
  std::printf("  flush every N    : %d batch(es) (0 = final flush only)\n",
              flush_every_n);
  std::printf("  proto run        : %s\n",
              uc_json_env ? "enabled (ZEROBUS_UC_TABLE_JSON set)"
                          : "disabled (set ZEROBUS_UC_TABLE_JSON to enable)");
  std::printf(
      "\n  queued_MiB/s = ingest loop only (upper bound, NOT durable)\n"
      "  acked_*      = includes final flush; every byte durably acked, "
      "aggregate across all streams (the honest number)\n");

  try {
    zerobus::Sdk sdk = build_sdk();

    std::size_t rec_size = 0;
    std::vector<std::string> pool = build_pool(rec_size);

    print_header();

    // JSON run (always). Empty descriptor => JSON stream.
    auto ingest_json = [](zerobus::Stream& s,
                          const std::vector<std::string>& b) {
      s.ingest_json_records(b);
    };
    RunResult j = run_parallel<std::string>(
        "json", sdk, client_id, client_secret, zerobus::RecordType::Json,
        /*descriptor=*/{}, pool, rec_size, total_bytes, configured_rpb,
        flush_every_n, num_streams, ingest_json);
    print_row(j);

    // Proto run (only if UC metadata is available). Encode each JSON record to
    // protobuf bytes with the UC-derived schema, into a parallel proto pool.
    if (uc_json_env) {
      zerobus::ProtoSchema schema =
          zerobus::ProtoSchema::from_uc_json(uc_json_env);
      std::vector<std::vector<std::uint8_t>> proto_pool;
      proto_pool.reserve(pool.size());
      std::uint64_t proto_bytes = 0;
      for (const std::string& rec : pool) {
        proto_pool.push_back(schema.encode_json(rec));
        proto_bytes += proto_pool.back().size();
      }
      const std::size_t proto_rec_size =
          static_cast<std::size_t>(proto_bytes / proto_pool.size());
      auto ingest_proto = [](zerobus::Stream& s,
                             const std::vector<std::vector<std::uint8_t>>& b) {
        s.ingest_proto_records(b);
      };
      RunResult p = run_parallel<std::vector<std::uint8_t>>(
          "proto", sdk, client_id, client_secret, zerobus::RecordType::Proto,
          schema.descriptor_bytes(), proto_pool, proto_rec_size, total_bytes,
          configured_rpb, flush_every_n, num_streams, ingest_proto);
      print_row(p);
    }
  } catch (const zerobus::ZerobusException& e) {
    std::cerr << "\nZerobus error: " << e.what()
              << " (retryable=" << (e.is_retryable() ? "true" : "false")
              << ")\n";
    return 1;
  }

  return 0;
}
