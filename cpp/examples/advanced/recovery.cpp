// Recovering unacknowledged records after a stream failure.
//
// The SDK recovers transparently from transient disconnects (StreamOptions
// recovery* fields). But if a stream fails terminally — recovery is exhausted,
// or close() itself fails — records that were queued but never acknowledged are
// not lost: the SDK keeps them so you can re-ingest them on a fresh stream.
//
// Stream::get_unacked_records() returns those records. It stays callable after
// a FAILED close() precisely because a failed close keeps the handle alive for
// recovery (a successful close frees it, and calling this afterwards throws).
// Each UnackedRecord carries the original payload bytes and whether it was JSON
// or protobuf, so you can re-ingest it verbatim.
//
// This example wraps the normal ingest/flush/close flow in a try/catch. On
// failure it drains the unacked records, opens a new stream, and re-ingests
// them. The re-ingest itself follows the cardinal rule: loop, then flush()
// once.
//
// Configuration — every connection setting is read from the environment. Export
// these before running (see ../README.md for what each one is and the full
// copy-pasteable block):
//   ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//   DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
//
//       ./build/examples/advanced_recovery
//
// Target table (see ../README.md for the CREATE TABLE statement):
//   orders(id INT, customer_name STRING, product_name STRING, quantity INT,
//          price DOUBLE, status STRING, created_at TIMESTAMP, updated_at
//          TIMESTAMP)

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "zerobus/zerobus.hpp"

namespace {

constexpr int kRecords = 50;

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

std::string make_order_json(int id, const std::string& customer,
                            const std::string& product, int quantity,
                            double price, const std::string& status,
                            std::int64_t ts) {
  return "{\"id\": " + std::to_string(id) + ", \"customer_name\": \"" +
         customer + "\", \"product_name\": \"" + product +
         "\", \"quantity\": " + std::to_string(quantity) +
         ", \"price\": " + std::to_string(price) + ", \"status\": \"" + status +
         "\", \"created_at\": " + std::to_string(ts) +
         ", \"updated_at\": " + std::to_string(ts) + "}";
}

zerobus::Stream open_stream(zerobus::Sdk& sdk, const std::string& table_name,
                            const std::string& client_id,
                            const std::string& client_secret) {
  zerobus::TableProperties props;
  props.table_name = table_name;
  zerobus::StreamOptions options;
  options.record_type = zerobus::RecordType::Json;
  return sdk.create_stream(props, client_id, client_secret, options);
}

// Re-ingest recovered records on a fresh stream. Loop-then-flush, as always.
void reingest(zerobus::Stream& stream,
              const std::vector<zerobus::UnackedRecord>& records) {
  for (const zerobus::UnackedRecord& record : records) {
    // These were JSON records; re-ingest them as the JSON strings they were.
    // (A proto stream would re-ingest record.data() via ingest_proto_record.)
    stream.ingest_json_record(record.as_string());
  }
  stream.flush();
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
                           .application_name("advanced-recovery")
                           .build();

    zerobus::Stream stream =
        open_stream(sdk, table_name, client_id, client_secret);

    const std::int64_t now = now_micros();
    for (int i = 1; i <= kRecords; ++i) {
      stream.ingest_json_record(
          make_order_json(i, "Customer " + std::to_string(i), "Widget",
                          1 + (i % 5), 9.99 + i, "pending", now));
    }

    // Guard the durability barrier. If the stream has failed terminally,
    // flush() (or close()) throws — and only then are there unacked records to
    // recover.
    try {
      stream.flush();
      stream.close();
      std::cout << "All " << kRecords << " records acknowledged.\n";
    } catch (const zerobus::ZerobusException& e) {
      std::cerr << "Stream failed: " << e.what() << "\n";

      // Drain whatever the failed stream never got acknowledged. Safe here
      // because the failed close() kept the handle alive for exactly this.
      std::vector<zerobus::UnackedRecord> unacked =
          stream.get_unacked_records();
      std::cout << "Recovering " << unacked.size()
                << " unacknowledged records on a fresh stream.\n";

      if (!unacked.empty()) {
        zerobus::Stream retry =
            open_stream(sdk, table_name, client_id, client_secret);
        reingest(retry, unacked);
        retry.close();
        std::cout << "Recovered records re-ingested and acknowledged.\n";
      }
    }
  } catch (const zerobus::ZerobusException& e) {
    std::cerr << "Zerobus error: " << e.what()
              << " (retryable=" << (e.is_retryable() ? "true" : "false")
              << ")\n";
    return 1;
  }

  return 0;
}
