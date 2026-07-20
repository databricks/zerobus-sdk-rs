// Async acknowledgement tracking with the Zerobus C++ SDK.
//
// The cardinal ingestion rule (see ../README.md) is to queue records in a loop
// and flush() once, never waiting per record. But sometimes you want to KNOW
// when each record becomes durable without ever blocking the ingest loop — for
// progress reporting, per-record bookkeeping, or reacting to failures. That is
// what an ack callback is for.
//
// Register an AckCallback via StreamOptions::ack_callback and the SDK invokes
// it from a background task: on_ack(offset) once each record is durable (in
// monotonic offset order), on_error(offset, msg) if a record fails terminally.
// The ingest loop stays non-blocking; the callback observes acks as they land.
//
// Callback contract (see include/zerobus/ack_callback.hpp for the full text):
//   - Both handlers are noexcept — an exception escaping across the FFI
//     boundary calls std::terminate. Handle your own errors in the handler.
//   - Handlers run serialized on another thread. Synchronize any shared state
//     (this example uses std::atomic) and keep them light.
//   - Never call back into the owning Stream from a handler (that is concurrent
//     use of a non-thread-safe object).
//   - Lifetime: whatever the handlers capture must outlive the callback. Here
//     the counters are declared before the Stream and the stream uses
//     CallbackWaitPolicy::forever(), so close() blocks until every in-flight
//     callback has finished — no callback can touch a freed counter.
//
// Configuration — every connection setting is read from the environment. Export
// these before running (see ../README.md for what each one is and the full
// copy-pasteable block):
//   ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//   DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
//
//       ./build/examples/advanced_ack_callback
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
#include <string>

#include "zerobus/zerobus.hpp"

namespace {

constexpr int kRecords = 100;

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

  // Shared state the callback updates. Declared BEFORE the Stream so it
  // outlives every callback invocation (see the lifetime note in the file
  // header).
  std::atomic<std::int64_t> acked{0};
  std::atomic<std::int64_t> failed{0};

  try {
    zerobus::Sdk sdk = zerobus::Sdk::builder()
                           .endpoint(server_endpoint)
                           .unity_catalog_url(workspace_url)
                           .application_name("advanced-ack-callback")
                           .build();

    zerobus::TableProperties props;
    props.table_name = table_name;

    zerobus::StreamOptions options;
    options.record_type = zerobus::RecordType::Json;

    // Register the callback. AckCallback::from() adapts two lambdas into an
    // AckCallback, so there is no subclass to write. Both must be noexcept.
    options.ack_callback = zerobus::AckCallback::from(
        [&acked](std::int64_t offset) noexcept {
          // Durable through `offset` (acks are monotonic). Keep this light.
          acked.fetch_add(1, std::memory_order_relaxed);
          (void)offset;
        },
        [&failed](std::int64_t offset, const std::string& msg) noexcept {
          failed.fetch_add(1, std::memory_order_relaxed);
          // Writing to std::cerr from a background thread is fine; formatting
          // into shared C++ state would need its own synchronization.
          std::cerr << "record at offset " << offset << " failed: " << msg
                    << "\n";
        });

    // forever(): close() blocks until every in-flight callback has finished, so
    // no callback can run after the counters go out of scope. It is the only
    // policy that gives that guarantee (see CallbackWaitPolicy).
    options.callback_wait_policy = zerobus::CallbackWaitPolicy::forever();

    zerobus::Stream stream =
        sdk.create_stream(props, client_id, client_secret, options);

    // Queue records in a loop — NO per-record wait. Acks arrive asynchronously
    // and drive the callback on a background thread while this loop runs.
    const std::int64_t now = now_micros();
    std::int64_t last_offset = -1;
    for (int i = 1; i <= kRecords; ++i) {
      last_offset = stream.ingest_json_record(
          make_order_json(i, "Customer " + std::to_string(i), "Widget",
                          1 + (i % 5), 9.99 + i, "pending", now));
    }
    std::cout << "Queued " << kRecords
              << " records; last offset ID: " << last_offset << "\n";

    // flush() drains the pending records to durable acks. The callback keeps
    // firing during and after this call; forever() ensures close() (below)
    // waits for the last one.
    stream.flush();
    stream.close();

    std::cout << "Done. acked=" << acked.load() << " failed=" << failed.load()
              << " of " << kRecords << " records.\n";
  } catch (const zerobus::ZerobusException& e) {
    std::cerr << "Zerobus error: " << e.what()
              << " (retryable=" << (e.is_retryable() ? "true" : "false")
              << ")\n";
    return 1;
  }

  return 0;
}
