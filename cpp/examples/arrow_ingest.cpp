// Zerobus C++ SDK — Arrow Flight ingestion (Beta).
//
// Streams Arrow record batches to Zerobus via ArrowStream. Unlike proto/JSON
// streams, batches are supplied as Arrow IPC bytes (schema message + one record
// batch message, in Arrow's IPC stream format), which the Apache Arrow C++
// library builds for us.
//
// Structure:
//   * Build an arrow::Schema matching the target table's columns.
//   * Build one RecordBatch per iteration (populated with fresh values so rows
//     differ on the wire).
//   * Serialize each batch as a self-contained Arrow IPC stream (schema +
//     batch) via arrow::ipc::MakeStreamWriter.
//   * Ingest the IPC bytes with stream.ingest_batch — queues only, does NOT
//     wait for the server ack. Loop and flush() once at the end (or
//     periodically), never wait per batch — see the cardinal rule in
//     zerobus.hpp.
//
// Target table: shinkansen.default.air_quality_zlata (device_name STRING,
// temp INT, humidity INT), reusing the demo_config.hpp connection info.
//
// Environment:
//   ZEROBUS_CLIENT_ID / ZEROBUS_CLIENT_SECRET   (required)
//
//   export ZEROBUS_CLIENT_ID=... ZEROBUS_CLIENT_SECRET=...
//   ./build/examples/arrow_ingest
//
// Dependencies: Apache Arrow C++ (found via `find_package(Arrow)` in the
// example's CMake target). If Arrow is not installed the target is skipped and
// the other examples still build.

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "demo_config.hpp"
#include "zerobus/zerobus.hpp"

namespace {

std::string require_env(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') {
    std::cerr << "error: environment variable " << name << " is not set.\n";
    std::exit(2);
  }
  return value;
}

// The Arrow schema mirrors the target table exactly (column names and types
// must match; STRING -> utf8, INT -> int32). The server pairs record-batch
// columns to table columns by name.
std::shared_ptr<arrow::Schema> air_quality_schema() {
  return arrow::schema({
      arrow::field("device_name", arrow::utf8()),
      arrow::field("temp", arrow::int32()),
      arrow::field("humidity", arrow::int32()),
  });
}

// Build one RecordBatch containing `n` rows. Each row's values are derived from
// `start_seed` so successive batches carry distinct data on the wire.
std::shared_ptr<arrow::RecordBatch> make_batch(int start_seed, int n) {
  arrow::StringBuilder device_b;
  arrow::Int32Builder temp_b;
  arrow::Int32Builder humidity_b;
  for (int i = 0; i < n; ++i) {
    const int s = start_seed + i;
    const std::string name = "device-" + std::to_string(s);
    // Arrow builder Append() returns arrow::Status; if it fails we abort with
    // a clear message rather than silently ingesting partial data.
    auto ok = device_b.Append(name);
    if (!ok.ok()) throw std::runtime_error(ok.ToString());
    ok = temp_b.Append(20 + (s % 15));
    if (!ok.ok()) throw std::runtime_error(ok.ToString());
    ok = humidity_b.Append(40 + (s % 40));
    if (!ok.ok()) throw std::runtime_error(ok.ToString());
  }
  std::shared_ptr<arrow::Array> device_arr, temp_arr, humidity_arr;
  auto ok = device_b.Finish(&device_arr);
  if (!ok.ok()) throw std::runtime_error(ok.ToString());
  ok = temp_b.Finish(&temp_arr);
  if (!ok.ok()) throw std::runtime_error(ok.ToString());
  ok = humidity_b.Finish(&humidity_arr);
  if (!ok.ok()) throw std::runtime_error(ok.ToString());
  return arrow::RecordBatch::Make(air_quality_schema(), n,
                                  {device_arr, temp_arr, humidity_arr});
}

// Serialize a RecordBatch into a self-contained Arrow IPC stream (schema
// message + one record-batch message). That is exactly what
// ArrowStream::ingest_batch expects — each ingest carries its own schema so no
// prior state is required.
std::vector<std::uint8_t> serialize_ipc(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  auto out_r = arrow::io::BufferOutputStream::Create();
  if (!out_r.ok()) throw std::runtime_error(out_r.status().ToString());
  auto out = *out_r;
  auto writer_r = arrow::ipc::MakeStreamWriter(out, batch->schema());
  if (!writer_r.ok()) throw std::runtime_error(writer_r.status().ToString());
  auto writer = *writer_r;
  auto ok = writer->WriteRecordBatch(*batch);
  if (!ok.ok()) throw std::runtime_error(ok.ToString());
  ok = writer->Close();
  if (!ok.ok()) throw std::runtime_error(ok.ToString());
  auto buf_r = out->Finish();
  if (!buf_r.ok()) throw std::runtime_error(buf_r.status().ToString());
  auto buf = *buf_r;
  return std::vector<std::uint8_t>(buf->data(), buf->data() + buf->size());
}

// Build the Arrow IPC schema-only header: a stream containing just the schema
// message and no record batches. That is what
// Sdk::create_arrow_stream(schema_ipc_bytes, ...) expects.
std::vector<std::uint8_t> serialize_schema_ipc(
    const std::shared_ptr<arrow::Schema>& schema) {
  auto out_r = arrow::io::BufferOutputStream::Create();
  if (!out_r.ok()) throw std::runtime_error(out_r.status().ToString());
  auto out = *out_r;
  auto writer_r = arrow::ipc::MakeStreamWriter(out, schema);
  if (!writer_r.ok()) throw std::runtime_error(writer_r.status().ToString());
  auto writer = *writer_r;
  auto ok = writer->Close();
  if (!ok.ok()) throw std::runtime_error(ok.ToString());
  auto buf_r = out->Finish();
  if (!buf_r.ok()) throw std::runtime_error(buf_r.status().ToString());
  auto buf = *buf_r;
  return std::vector<std::uint8_t>(buf->data(), buf->data() + buf->size());
}

}  // namespace

int main() {
  const std::string client_id = require_env("ZEROBUS_CLIENT_ID");
  const std::string client_secret = require_env("ZEROBUS_CLIENT_SECRET");

  try {
    // 1. Build the SDK.
    zerobus::Sdk sdk = zerobus::Sdk::builder()
                           .endpoint(zerobus_demo::kZerobusEndpoint)
                           .unity_catalog_url(zerobus_demo::kWorkspaceUrl)
                           .application_name("arrow-ingest")
                           .build();

    // 2. Open an Arrow stream. The schema-only IPC bytes tell the server what
    //    the record batches will look like.
    //
    //    Optional IPC compression trades client CPU for fewer bytes on the
    //    wire; enable it only when network bandwidth limits throughput. Here we
    //    turn on Zstd via ArrowStreamOptions (Lz4Frame is the other codec) and
    //    pass the options to create_arrow_stream.
    const std::shared_ptr<arrow::Schema> schema = air_quality_schema();
    const std::vector<std::uint8_t> schema_ipc = serialize_schema_ipc(schema);

    zerobus::ArrowStreamOptions arrow_options;
    arrow_options.ipc_compression = zerobus::IpcCompression::Zstd;

    zerobus::ArrowStream stream =
        sdk.create_arrow_stream(zerobus_demo::table_name(), schema_ipc,
                                client_id, client_secret, arrow_options);

    // 3. Ingest a series of batches. Each ingest_batch queues one Arrow IPC
    //    stream (schema + one record batch) and returns immediately with the
    //    assigned offset. Never wait per batch — the loop-then-flush pattern
    //    from zerobus.hpp is the whole point of the async API.
    constexpr int kBatches = 10;
    constexpr int kRowsPerBatch = 100;
    std::int64_t last_offset = -1;
    for (int b = 0; b < kBatches; ++b) {
      std::shared_ptr<arrow::RecordBatch> batch =
          make_batch(b * kRowsPerBatch, kRowsPerBatch);
      last_offset = stream.ingest_batch(serialize_ipc(batch));
    }
    std::cout << "Queued " << kBatches << " batches ("
              << kBatches * kRowsPerBatch
              << " rows); last offset = " << last_offset << "\n";

    // 4. One flush drains every pending batch to a durable server ack, then
    //    close at a controlled point.
    stream.flush();
    stream.close();
    std::cout << "Flushed and closed cleanly.\n";
  } catch (const zerobus::ZerobusException& e) {
    std::cerr << "Zerobus error: " << e.what()
              << " (retryable=" << (e.is_retryable() ? "true" : "false")
              << ")\n";
    return 1;
  } catch (const std::exception& e) {
    std::cerr << "Arrow error: " << e.what() << "\n";
    return 1;
  }
  return 0;
}
