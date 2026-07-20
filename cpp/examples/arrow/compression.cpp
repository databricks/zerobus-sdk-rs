// Arrow Flight ingestion with IPC compression (Beta).
//
// This is arrow/arrow_ingest.cpp with one change: ArrowStreamOptions::
// ipc_compression is set so the SDK compresses each Arrow IPC batch before it
// goes on the wire. Compression trades client CPU for fewer bytes sent; it is
// worth it when network bandwidth (not CPU) limits throughput — wide batches,
// highly repetitive columns, or a constrained uplink. Leave it off
// (NoCompression, the default) when CPU is the bottleneck or batches are small.
//
// Codecs (zerobus::IpcCompression):
//   - NoCompression (default) — no compression.
//   - Lz4Frame — fast, modest ratio; a good default when you do want it.
//   - Zstd — better ratio, more CPU. This example uses Zstd.
//
// Compression is entirely client-side and transparent to the server: the same
// batches land in the table either way, so you can flip the codec without any
// schema or table change. The rest of the flow — schema-only IPC to open the
// stream, loop ingest_batch(), flush() once — is identical to arrow_ingest.cpp.
//
// Configuration — every connection setting is read from the environment. Export
// these before running (see ../README.md for what each one is and the full
// copy-pasteable block):
//   ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//   DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
//
//       ./build/examples/arrow_compression
//
// Target table (see ../README.md for the CREATE TABLE statement):
//   orders(id INT, customer_name STRING, product_name STRING, quantity INT,
//          price DOUBLE, status STRING, created_at TIMESTAMP, updated_at
//          TIMESTAMP)
//
// Dependencies: Apache Arrow C++ (found via find_package(Arrow) in the
// example's CMake target). If Arrow is not installed the target is skipped and
// the other examples still build.

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "zerobus/zerobus.hpp"

namespace {

constexpr int kBatches = 10;
constexpr int kRowsPerBatch = 1000;

std::string require_env(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') {
    std::cerr << "error: environment variable " << name << " is not set.\n"
              << "See the header of this file for the required variables.\n";
    std::exit(2);
  }
  return value;
}

std::int64_t now_micros() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::shared_ptr<arrow::Schema> orders_schema() {
  auto utc_micros = arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
  return arrow::schema({
      arrow::field("id", arrow::int32()),
      arrow::field("customer_name", arrow::large_utf8()),
      arrow::field("product_name", arrow::large_utf8()),
      arrow::field("quantity", arrow::int32()),
      arrow::field("price", arrow::float64()),
      arrow::field("status", arrow::large_utf8()),
      arrow::field("created_at", utc_micros),
      arrow::field("updated_at", utc_micros),
  });
}

void check(const arrow::Status& status) {
  if (!status.ok()) throw std::runtime_error(status.ToString());
}

// Build one RecordBatch of `n` rows. Values repeat heavily (a handful of
// product names, one status), which is exactly the shape compression pays off
// on.
std::shared_ptr<arrow::RecordBatch> make_batch(
    const std::shared_ptr<arrow::Schema>& schema, int start_seed, int n,
    std::int64_t ts) {
  arrow::Int32Builder id_b;
  arrow::LargeStringBuilder customer_b;
  arrow::LargeStringBuilder product_b;
  arrow::Int32Builder quantity_b;
  arrow::DoubleBuilder price_b;
  arrow::LargeStringBuilder status_b;
  arrow::TimestampBuilder created_b(schema->field(6)->type(),
                                    arrow::default_memory_pool());
  arrow::TimestampBuilder updated_b(schema->field(7)->type(),
                                    arrow::default_memory_pool());

  for (int i = 0; i < n; ++i) {
    const int s = start_seed + i;
    check(id_b.Append(s));
    check(customer_b.Append("Customer " + std::to_string(s)));
    check(product_b.Append("Product " + std::to_string(s % 7)));
    check(quantity_b.Append(1 + (s % 5)));
    check(price_b.Append(9.99 + (s % 100)));
    check(status_b.Append("pending"));
    check(created_b.Append(ts));
    check(updated_b.Append(ts));
  }

  std::vector<std::shared_ptr<arrow::Array>> columns(8);
  check(id_b.Finish(&columns[0]));
  check(customer_b.Finish(&columns[1]));
  check(product_b.Finish(&columns[2]));
  check(quantity_b.Finish(&columns[3]));
  check(price_b.Finish(&columns[4]));
  check(status_b.Finish(&columns[5]));
  check(created_b.Finish(&columns[6]));
  check(updated_b.Finish(&columns[7]));

  return arrow::RecordBatch::Make(schema, n, columns);
}

// Serialize a RecordBatch into a self-contained Arrow IPC stream. NOTE: this is
// the plaintext Arrow IPC the SDK expects as input — the SDK applies the
// ipc_compression codec itself when sending. Do not pre-compress here.
std::vector<std::uint8_t> serialize_ipc(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  auto out_r = arrow::io::BufferOutputStream::Create();
  check(out_r.status());
  auto out = *out_r;
  auto writer_r = arrow::ipc::MakeStreamWriter(out, batch->schema());
  check(writer_r.status());
  auto writer = *writer_r;
  check(writer->WriteRecordBatch(*batch));
  check(writer->Close());
  auto buf_r = out->Finish();
  check(buf_r.status());
  auto buf = *buf_r;
  return std::vector<std::uint8_t>(buf->data(), buf->data() + buf->size());
}

std::vector<std::uint8_t> serialize_schema_ipc(
    const std::shared_ptr<arrow::Schema>& schema) {
  auto out_r = arrow::io::BufferOutputStream::Create();
  check(out_r.status());
  auto out = *out_r;
  auto writer_r = arrow::ipc::MakeStreamWriter(out, schema);
  check(writer_r.status());
  auto writer = *writer_r;
  check(writer->Close());
  auto buf_r = out->Finish();
  check(buf_r.status());
  auto buf = *buf_r;
  return std::vector<std::uint8_t>(buf->data(), buf->data() + buf->size());
}

}  // namespace

int main() {
  const std::string server_endpoint = require_env("ZEROBUS_SERVER_ENDPOINT");
  const std::string workspace_url = require_env("DATABRICKS_WORKSPACE_URL");
  const std::string table_name = require_env("ZEROBUS_TABLE_NAME");
  const std::string client_id = require_env("DATABRICKS_CLIENT_ID");
  const std::string client_secret = require_env("DATABRICKS_CLIENT_SECRET");

  try {
    zerobus::Sdk sdk = zerobus::Sdk::builder()
                           .endpoint(server_endpoint)
                           .unity_catalog_url(workspace_url)
                           .application_name("arrow-compression")
                           .build();

    // The one line that turns compression on. Everything else matches
    // arrow_ingest.cpp. Try Lz4Frame instead of Zstd to trade ratio for CPU.
    zerobus::ArrowStreamOptions options;
    options.ipc_compression = zerobus::IpcCompression::Zstd;

    const std::shared_ptr<arrow::Schema> schema = orders_schema();
    const std::vector<std::uint8_t> schema_ipc = serialize_schema_ipc(schema);

    zerobus::ArrowStream stream = sdk.create_arrow_stream(
        table_name, schema_ipc, client_id, client_secret, options);

    const std::int64_t ts = now_micros();
    std::int64_t last_offset = -1;
    for (int b = 0; b < kBatches; ++b) {
      std::shared_ptr<arrow::RecordBatch> batch =
          make_batch(schema, b * kRowsPerBatch, kRowsPerBatch, ts);
      last_offset = stream.ingest_batch(serialize_ipc(batch));
    }
    std::cout << "Queued " << kBatches << " Zstd-compressed batches ("
              << kBatches * kRowsPerBatch
              << " rows); last offset ID: " << last_offset << "\n";

    stream.flush();
    std::cout << "Flushed — all batches acknowledged.\n";

    stream.close();
    std::cout << "Stream closed successfully.\n";
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
