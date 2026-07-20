// Batch JSON ingestion with the Zerobus C++ SDK.
//
// This example opens a JSON stream and ingests records with the BATCH API,
// ingest_json_records(), which hands a whole vector of records to the SDK in a
// single FFI crossing. Batching is the right choice in hot paths: each FFI
// crossing has a fixed cost that batching amortizes, and a batch is
// acknowledged all-or-nothing as a unit.
//
// The batch call returns the offset of the LAST record in the batch. Because
// acks are monotonic, waiting on that single offset confirms the whole batch.
//
// Configuration — every connection setting is read from the environment. Export
// these before running (see ../README.md for what each one is and the full
// copy-pasteable block):
//   ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//   DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
//
//       ./build/examples/json_batch
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

}  // namespace

int main() {
  const std::string server_endpoint = require_env("ZEROBUS_SERVER_ENDPOINT");
  const std::string workspace_url = require_env("DATABRICKS_WORKSPACE_URL");
  const std::string table_name = require_env("ZEROBUS_TABLE_NAME");
  const std::string client_id = require_env("DATABRICKS_CLIENT_ID");
  const std::string client_secret = require_env("DATABRICKS_CLIENT_SECRET");

  try {
    // 1. Build the SDK.
    zerobus::Sdk sdk = zerobus::Sdk::builder()
                           .endpoint(server_endpoint)
                           .unity_catalog_url(workspace_url)
                           .application_name("json-batch")
                           .build();

    // 2. Open a JSON stream.
    zerobus::TableProperties props;
    props.table_name = table_name;

    zerobus::StreamOptions options;
    options.record_type = zerobus::RecordType::Json;

    zerobus::Stream stream =
        sdk.create_stream(props, client_id, client_secret, options);

    const std::int64_t now = now_micros();

    // 3. Build a batch and hand it over in one call. ingest_json_records()
    //    queues the whole vector and returns the offset of the LAST record.
    const std::vector<std::string> batch = {
        make_order_json(1, "Alice Smith", "Wireless Mouse", 2, 25.99, "pending",
                        now),
        make_order_json(2, "Bob Johnson", "Mechanical Keyboard", 1, 89.99,
                        "shipped", now),
        make_order_json(3, "Carol Williams", "USB-C Hub", 3, 45.00, "delivered",
                        now),
    };

    const std::int64_t last_offset = stream.ingest_json_records(batch);
    std::cout << "Batch of " << batch.size()
              << " records queued; last offset ID: " << last_offset << "\n";

    // 4. Confirm the batch. Waiting on the last offset is enough — acks are
    //    monotonic, so offset N acked implies every offset <= N is acked. In a
    //    hot path you would queue many batches and flush() once instead of
    //    waiting after each.
    if (last_offset >= 0) {
      stream.wait_for_offset(last_offset);
      std::cout << "Batch acknowledged through offset ID: " << last_offset
                << "\n";
    }

    // 5. flush() drains anything still pending, then close at a controlled
    //    point.
    stream.flush();
    stream.close();
    std::cout << "Stream closed successfully.\n";
  } catch (const zerobus::ZerobusException& e) {
    std::cerr << "Zerobus error: " << e.what()
              << " (retryable=" << (e.is_retryable() ? "true" : "false")
              << ")\n";
    return 1;
  }

  return 0;
}
