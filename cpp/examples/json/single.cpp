// Single-record JSON ingestion with the Zerobus C++ SDK.
//
// This example opens a JSON stream to a Delta table and ingests a handful of
// records ONE AT A TIME with ingest_json_record(), then flushes ONCE at the
// end. That is the correct pattern: ingest_json_record() returns as soon as the
// record is queued; sending and acknowledgement happen on background tasks.
// Calling wait_for_offset()/flush() after every record would force a full
// server round-trip per record and collapse throughput. For high volume, prefer
// the batch API in batch.cpp.
//
// Configuration — every connection setting is read from the environment, so no
// value is ever baked into source. Export these before running (see
// ../README.md for what each one is and the full copy-pasteable block):
//   ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//   DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
//
//       ./build/examples/json_single
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

#include "zerobus/zerobus.hpp"

namespace {

// Read a required environment variable or exit with a clear message. Exiting
// (rather than throwing) keeps a misconfigured environment distinct from a
// genuine SDK ZerobusException below.
std::string require_env(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') {
    std::cerr << "error: environment variable " << name << " is not set.\n"
              << "See the header of this file for the required variables.\n";
    std::exit(2);
  }
  return value;
}

// Delta TIMESTAMP is an int64 count of microseconds since the Unix epoch (UTC).
std::int64_t now_micros() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

// Build one order record as a JSON string matching the table columns.
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
    // 1. Build the SDK — an authenticated connection factory. TLS is on by
    //    default; the builder is consumed by build().
    zerobus::Sdk sdk = zerobus::Sdk::builder()
                           .endpoint(server_endpoint)
                           .unity_catalog_url(workspace_url)
                           .application_name("json-single")
                           .build();

    // 2. Open a JSON stream. record_type must be Json to match the payloads,
    //    and descriptor_proto is left empty (no schema needed for JSON — the
    //    server maps each record's fields onto the table's columns by name).
    zerobus::TableProperties props;
    props.table_name = table_name;

    zerobus::StreamOptions options;
    options.record_type = zerobus::RecordType::Json;

    zerobus::Stream stream =
        sdk.create_stream(props, client_id, client_secret, options);

    const std::int64_t now = now_micros();

    // 3. Ingest records one at a time. Each call queues the record and returns
    //    immediately with the assigned offset — there is NO per-record wait
    //    here. The single wait point is the flush() below.
    std::int64_t offset = stream.ingest_json_record(make_order_json(
        1, "Alice Smith", "Wireless Mouse", 2, 25.99, "pending", now));
    std::cout << "Record 1 queued with offset ID: " << offset << "\n";

    offset = stream.ingest_json_record(make_order_json(
        2, "Bob Johnson", "Mechanical Keyboard", 1, 89.99, "shipped", now));
    std::cout << "Record 2 queued with offset ID: " << offset << "\n";

    // A raw JSON literal works exactly the same — any UTF-8 JSON string that
    // matches the table schema is accepted.
    offset = stream.ingest_json_record(
        R"({"id": 3, "customer_name": "Carol Williams", "product_name": "USB-C Hub", )"
        R"("quantity": 3, "price": 45.00, "status": "delivered", )"
        "\"created_at\": " +
        std::to_string(now) + ", \"updated_at\": " + std::to_string(now) + "}");
    std::cout << "Record 3 queued with offset ID: " << offset << "\n";

    // 4. Flush once at the end: block until every queued record is durably
    //    acknowledged by the server. This is the right place to wait — not
    //    after each individual ingest above.
    stream.flush();
    std::cout << "All records acknowledged.\n";

    // 5. Close at a controlled point rather than leaving it to the destructor.
    //    close() surfaces any final error by throwing; ~Stream() would swallow
    //    it.
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
