// Batch protobuf ingestion with the Zerobus C++ SDK (dynamic schema).
//
// Like proto/single.cpp, this builds the table's descriptor from Unity Catalog
// metadata via ProtoSchema::from_uc_json() (no .proto file, no protoc) and uses
// the same ProtoSchema to encode records. Here records are ingested with the
// BATCH API, ingest_proto_records(), which hands a whole vector of encoded
// records to the SDK in a single FFI crossing.
//
// Batching is the right choice in hot paths: each FFI crossing has a fixed cost
// that batching amortizes, and a batch is acknowledged all-or-nothing as a
// unit. The call returns a single logical offset assigned to the whole batch;
// waiting on that one offset confirms the entire batch.
//
// Two ways to hand a batch over are shown: a vector of encoded records, and
// ProtoRecordViews borrowing records that already live in a caller-owned arena.
//
// Configuration — every connection setting, plus the Unity Catalog table
// metadata JSON, is read from the environment. Export these before running (see
// ../README.md for what each one is and the full copy-pasteable block,
// including the ZEROBUS_UC_TABLE_JSON curl command):
//   ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//   DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET, ZEROBUS_UC_TABLE_JSON
//
//       ./build/examples/proto_batch
//
// Target table (see ../README.md and orders.proto):
//   orders(id INT, customer_name STRING, product_name STRING, quantity INT,
//          price DOUBLE, status STRING, created_at TIMESTAMP, updated_at
//          TIMESTAMP)

#include <chrono>
#include <cstddef>
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
  const std::string uc_table_json = require_env("ZEROBUS_UC_TABLE_JSON");

  try {
    // 1. Build a protobuf schema for the table from its Unity Catalog metadata.
    zerobus::ProtoSchema schema =
        zerobus::ProtoSchema::from_uc_json(uc_table_json);

    // 2. Build the SDK.
    zerobus::Sdk sdk = zerobus::Sdk::builder()
                           .endpoint(server_endpoint)
                           .unity_catalog_url(workspace_url)
                           .application_name("proto-batch")
                           .build();

    // 3. Open a proto stream, passing the descriptor.
    zerobus::TableProperties props;
    props.table_name = table_name;
    props.descriptor_proto = schema.descriptor_bytes();

    zerobus::StreamOptions options;
    options.record_type = zerobus::RecordType::Proto;

    zerobus::Stream stream =
        sdk.create_stream(props, client_id, client_secret, options);

    const std::int64_t now = now_micros();

    // 4. Encode each record's JSON to protobuf bytes, collect them into a
    //    batch, then hand the whole batch over in a single call.
    const std::vector<std::vector<std::uint8_t>> batch = {
        schema.encode_json(make_order_json(1, "Alice Smith", "Wireless Mouse",
                                           2, 25.99, "pending", now)),
        schema.encode_json(make_order_json(
            2, "Bob Johnson", "Mechanical Keyboard", 1, 89.99, "shipped", now)),
        schema.encode_json(make_order_json(3, "Carol Williams", "USB-C Hub", 3,
                                           45.00, "delivered", now)),
    };

    const std::int64_t batch_offset = stream.ingest_proto_records(batch);
    std::cout << "Batch of " << batch.size()
              << " records queued; batch offset ID: " << batch_offset << "\n";

    // 5. Confirm the batch. Waiting on the batch's single offset confirms every
    //    record in it. In a hot path you would queue many batches and flush()
    //    once instead of waiting after each.
    if (batch_offset >= 0) {
      stream.wait_for_offset(batch_offset);
      std::cout << "Batch acknowledged at offset ID: " << batch_offset << "\n";
    }

    // 6. A second batch, for records the SDK does not own.
    //
    //    encode_json() returns a vector per record, so the batch above was
    //    already a natural vector-of-vectors. When your records live elsewhere
    //    — here, packed into one arena — describe them with ProtoRecordView
    //    instead of copying each payload into that container to pass it.
    const std::vector<std::string> more_orders = {
        make_order_json(4, "Dan Brown", "Laptop Stand", 1, 34.50, "pending",
                        now),
        make_order_json(5, "Erin Page", "HD Webcam", 2, 59.99, "pending", now),
    };

    // Where each encoded record starts in the arena, and how long it is.
    struct Span {
      std::size_t offset;
      std::size_t size;
    };
    std::vector<std::uint8_t> arena;
    std::vector<Span> spans;
    for (const std::string& order : more_orders) {
      const std::vector<std::uint8_t> encoded = schema.encode_json(order);
      spans.push_back({arena.size(), encoded.size()});
      arena.insert(arena.end(), encoded.begin(), encoded.end());
    }

    // Take the pointers only now the arena has stopped growing: a reallocating
    // insert invalidates any taken earlier.
    std::vector<zerobus::ProtoRecordView> views;
    views.reserve(spans.size());
    for (const Span& span : spans) {
      views.push_back({arena.data() + span.offset, span.size});
    }

    // arena must outlive this call — the views only borrow it.
    const std::int64_t arena_offset =
        stream.ingest_proto_records(views.data(), views.size());
    std::cout << "Arena batch of " << views.size()
              << " records queued; batch offset ID: " << arena_offset << "\n";

    // 7. flush() drains anything still pending, then close at a controlled
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
