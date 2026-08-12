// Batch JSON ingestion with the Zerobus C++ SDK.
//
// This example opens a JSON stream and ingests records with the BATCH API,
// ingest_json_records(), which hands a whole vector of records to the SDK in a
// single FFI crossing. Batching is the right choice in hot paths: each FFI
// crossing has a fixed cost that batching amortizes, and a batch is
// acknowledged all-or-nothing as a unit.
//
// The batch call returns a single logical offset assigned to the whole batch.
// Waiting on that one offset confirms the entire batch.
//
// It also demonstrates two optional features:
//   - An async ack callback (StreamOptions::ack_callback) that observes
//     acknowledgements on a background task without blocking the ingest loop.
//   - A custom HeadersProvider for authentication (shown commented out at
//     stream creation), the alternative to OAuth client credentials.
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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
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

// A custom HeadersProvider is the alternative to OAuth client credentials: the
// core calls get_headers() whenever it needs fresh headers (possibly from
// another thread), and you return whatever the endpoint expects — at minimum an
// "authorization" bearer token and "x-databricks-zerobus-table-name". Throwing
// surfaces the message to the core as a headers-provider error. Provider
// ownership is handed to the FFI, so the caller does not need to retain its own
// shared_ptr after stream creation. See include/zerobus/headers_provider.hpp
// for the full contract. Used in the commented-out create_stream() call below.
class BearerTokenProvider : public zerobus::HeadersProvider {
 public:
  BearerTokenProvider(std::string table_name, std::string token)
      : table_name_(std::move(table_name)), token_(std::move(token)) {}

  std::map<std::string, std::string> get_headers() override {
    return {
        {"authorization", "Bearer " + token_},
        {"x-databricks-zerobus-table-name", table_name_},
    };
  }

 private:
  std::string table_name_;
  std::string token_;
};

}  // namespace

int main() {
  const std::string server_endpoint = require_env("ZEROBUS_SERVER_ENDPOINT");
  const std::string workspace_url = require_env("DATABRICKS_WORKSPACE_URL");
  const std::string table_name = require_env("ZEROBUS_TABLE_NAME");
  const std::string client_id = require_env("DATABRICKS_CLIENT_ID");
  const std::string client_secret = require_env("DATABRICKS_CLIENT_SECRET");

  // Counter the ack callback updates. Declared before the Stream so it outlives
  // every callback invocation (see the lifetime note below).
  std::atomic<std::int64_t> acked{0};

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

    // Optional: an async ack callback observes acknowledgements on a background
    // task, so you can track durability without ever blocking the ingest loop.
    // AckCallback::from() adapts lambdas; both handlers must be noexcept, run
    // on another thread (synchronize shared state — here a std::atomic), and
    // must not call back into the Stream. forever() makes close() wait for
    // every in-flight callback, so none can touch `acked` after it goes out of
    // scope. See include/zerobus/ack_callback.hpp for the full contract.
    options.ack_callback = zerobus::AckCallback::from(
        [&acked](std::int64_t offset) noexcept {
          acked.fetch_add(1, std::memory_order_relaxed);
          (void)offset;
        },
        [](std::int64_t offset, const std::string& msg) noexcept {
          std::cerr << "record at offset " << offset << " failed: " << msg
                    << "\n";
        });
    options.callback_wait_policy = zerobus::CallbackWaitPolicy::forever();

    zerobus::Stream stream =
        sdk.create_stream(props, client_id, client_secret, options);

    // Alternative: authenticate with a custom HeadersProvider instead of OAuth
    // client credentials. The provider supplies auth itself, so the builder's
    // unity_catalog_url() is optional in that path.
    //   auto provider =
    //       std::make_shared<BearerTokenProvider>(table_name, my_token);
    //   zerobus::Stream stream = sdk.create_stream(props, provider, options);

    const std::int64_t now = now_micros();

    // 3. Build a batch and hand it over in one call. ingest_json_records()
    //    queues the whole vector and returns the single offset assigned to the
    //    batch.
    const std::vector<std::string> batch = {
        make_order_json(1, "Alice Smith", "Wireless Mouse", 2, 25.99, "pending",
                        now),
        make_order_json(2, "Bob Johnson", "Mechanical Keyboard", 1, 89.99,
                        "shipped", now),
        make_order_json(3, "Carol Williams", "USB-C Hub", 3, 45.00, "delivered",
                        now),
    };

    const std::int64_t batch_offset = stream.ingest_json_records(batch);
    std::cout << "Batch of " << batch.size()
              << " records queued; batch offset ID: " << batch_offset << "\n";

    // 4. Confirm the batch. Waiting on the batch's single offset confirms every
    //    record in it. In a hot path you would queue many batches and flush()
    //    once instead of waiting after each.
    if (batch_offset >= 0) {
      stream.wait_for_offset(batch_offset);
      std::cout << "Batch acknowledged at offset ID: " << batch_offset << "\n";
    }

    // 5. flush() drains anything still pending, then close at a controlled
    //    point. The ack callback keeps firing during flush()/close(); forever()
    //    ensures close() waits for the last one.
    stream.flush();
    stream.close();
    std::cout << "Stream closed successfully. Callback observed "
              << acked.load() << " logical submission acknowledgement(s).\n";
  } catch (const zerobus::ZerobusException& e) {
    std::cerr << "Zerobus error: " << e.what()
              << " (retryable=" << (e.is_retryable() ? "true" : "false")
              << ")\n";
    return 1;
  }

  return 0;
}
