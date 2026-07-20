// Single-record protobuf ingestion with the Zerobus C++ SDK (dynamic schema).
//
// This example builds a protobuf descriptor for the target table straight from
// its Unity Catalog metadata via ProtoSchema::from_uc_json() — no hand-written
// .proto file and no protoc required. The same ProtoSchema also encodes each
// record's JSON into protobuf bytes for ingestion.
//
// Records are ingested ONE AT A TIME with ingest_proto_record(), then flushed
// ONCE at the end. ingest_proto_record() returns as soon as the record is
// queued; sending and acknowledgement happen on background tasks. Calling
// wait_for_offset()/flush() after every record would force a full server
// round-trip per record and collapse throughput. For high volume, prefer the
// batch API in batch.cpp.
//
// Configuration — every connection setting, plus the Unity Catalog table
// metadata JSON, is read from the environment so no value is baked into source.
// Export these before running (see ../README.md for what each one is and the
// full copy-pasteable block, including the ZEROBUS_UC_TABLE_JSON curl command):
//   ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//   DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET, ZEROBUS_UC_TABLE_JSON
//
//       ./build/examples/proto_single
//
// Target table (see ../README.md and orders.proto):
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

// Build one order record as a JSON string. ProtoSchema::encode_json shapes it
// into protobuf bytes; per-column value rules (DATE/TIMESTAMP as integers,
// BINARY as base64, etc.) are documented in the FFI README / zerobus.h.
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
  const std::string uc_table_json = require_env("ZEROBUS_UC_TABLE_JSON");

  try {
    // 1. Build a protobuf schema for the table from its Unity Catalog metadata.
    //    This yields both the descriptor (for stream creation) and a
    //    JSON->proto encoder — no .proto file required.
    zerobus::ProtoSchema schema =
        zerobus::ProtoSchema::from_uc_json(uc_table_json);

    // 2. Build the SDK.
    zerobus::Sdk sdk = zerobus::Sdk::builder()
                           .endpoint(server_endpoint)
                           .unity_catalog_url(workspace_url)
                           .application_name("proto-single")
                           .build();

    // 3. Open a proto stream, passing the descriptor built above.
    zerobus::TableProperties props;
    props.table_name = table_name;
    props.descriptor_proto = schema.descriptor_bytes();

    zerobus::StreamOptions options;
    options.record_type = zerobus::RecordType::Proto;

    zerobus::Stream stream =
        sdk.create_stream(props, client_id, client_secret, options);

    const std::int64_t now = now_micros();

    // 4. Ingest records one at a time. Each iteration encodes the record's JSON
    //    to protobuf bytes and queues them — with NO per-record wait. The
    //    single wait point is the flush() below.
    std::vector<std::uint8_t> encoded = schema.encode_json(make_order_json(
        1, "Alice Smith", "Wireless Mouse", 2, 25.99, "pending", now));
    std::int64_t offset = stream.ingest_proto_record(encoded);
    std::cout << "Record 1 queued with offset ID: " << offset << "\n";

    encoded = schema.encode_json(make_order_json(
        2, "Bob Johnson", "Mechanical Keyboard", 1, 89.99, "shipped", now));
    offset = stream.ingest_proto_record(encoded);
    std::cout << "Record 2 queued with offset ID: " << offset << "\n";

    encoded = schema.encode_json(make_order_json(
        3, "Carol Williams", "USB-C Hub", 3, 45.00, "delivered", now));
    offset = stream.ingest_proto_record(encoded);
    std::cout << "Record 3 queued with offset ID: " << offset << "\n";

    // 5. Flush once at the end: block until every queued record is durably
    //    acknowledged. This is the right place to wait — not after each ingest.
    stream.flush();
    std::cout << "All records acknowledged.\n";

    // 6. Close at a controlled point rather than leaving it to the destructor.
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
