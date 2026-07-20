// Custom authentication with a HeadersProvider.
//
// By default the SDK authenticates with OAuth client credentials: you hand
// create_stream() a client id/secret and it mints short-lived, table-scoped
// Unity Catalog tokens for you. When you need a different auth scheme — a token
// broker, a rotating bearer token, a pre-minted token from elsewhere —
// implement HeadersProvider instead and pass it to the create_stream() overload
// that takes a provider (no client id/secret).
//
// The core calls get_headers() whenever it needs fresh headers for the stream,
// possibly from an internal worker thread. Return the headers the Zerobus
// endpoint expects — at minimum an "authorization" bearer token and the
// "x-databricks-zerobus-table-name" header (the same headers the built-in OAuth
// provider produces).
//
// Contract (see include/zerobus/headers_provider.hpp):
//   - get_headers() may be called from another thread. The core serializes
//     calls, but implementations must be thread-safe w.r.t. their own state.
//   - Throwing from get_headers() surfaces the message to the core as a
//     headers-provider error and fails the pending operation (it does NOT cross
//     the FFI boundary as a C++ exception).
//   - Lifetime: the provider must outlive the Stream. The Stream holds a
//     shared_ptr to it, which is necessary but not always sufficient — a
//     get_headers() call still running when close() times out (~1s) can touch a
//     freed provider. Keep get_headers() well under that budget, or keep the
//     provider alive past the Stream.
//
// Because the provider supplies auth directly, unity_catalog_url() is optional
// on the builder. This example reads a ready-made bearer token from the
// environment (DATABRICKS_TOKEN) — replace fetch_token() with your real token
// source (a broker call, a refresh, etc.).
//
// Configuration — read from the environment (no OAuth client id/secret needed
// here; the token stands in for them):
//   ZEROBUS_SERVER_ENDPOINT, ZEROBUS_TABLE_NAME, DATABRICKS_TOKEN
//
//       ./build/examples/advanced_headers_provider
//
// Target table (see ../README.md for the CREATE TABLE statement):
//   orders(id INT, customer_name STRING, product_name STRING, quantity INT,
//          price DOUBLE, status STRING, created_at TIMESTAMP, updated_at
//          TIMESTAMP)

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "zerobus/zerobus.hpp"

namespace {

constexpr int kRecords = 20;

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

// A provider that authenticates with a bearer token from a custom source.
//
// Real implementations typically cache a token and refresh it before expiry;
// this one just demonstrates the shape. The mutex makes get_headers()
// thread-safe with respect to the cached token, as the contract requires.
class BearerTokenProvider : public zerobus::HeadersProvider {
 public:
  BearerTokenProvider(std::string table_name, std::string token)
      : table_name_(std::move(table_name)), token_(std::move(token)) {}

  std::map<std::string, std::string> get_headers() override {
    std::lock_guard<std::mutex> lock(mutex_);
    // Refresh here if the token is near expiry (fetch_token()), then return it.
    // Throwing from this method fails the pending operation with the message.
    if (token_.empty()) {
      throw std::runtime_error("no bearer token available");
    }
    return {
        {"authorization", "Bearer " + token_},
        {"x-databricks-zerobus-table-name", table_name_},
    };
  }

 private:
  std::mutex mutex_;
  std::string table_name_;
  std::string token_;
};

}  // namespace

int main() {
  const std::string server_endpoint = require_env("ZEROBUS_SERVER_ENDPOINT");
  const std::string table_name = require_env("ZEROBUS_TABLE_NAME");
  const std::string token = require_env("DATABRICKS_TOKEN");

  try {
    // No unity_catalog_url(): the provider supplies authentication itself.
    zerobus::Sdk sdk = zerobus::Sdk::builder()
                           .endpoint(server_endpoint)
                           .application_name("advanced-headers-provider")
                           .build();

    zerobus::TableProperties props;
    props.table_name = table_name;

    zerobus::StreamOptions options;
    options.record_type = zerobus::RecordType::Json;

    // Hand the provider to the create_stream() overload that takes one. The
    // Stream holds a shared_ptr to it for its lifetime (see the lifetime note).
    auto provider = std::make_shared<BearerTokenProvider>(table_name, token);
    zerobus::Stream stream = sdk.create_stream(props, provider, options);

    const std::int64_t now = now_micros();
    for (int i = 1; i <= kRecords; ++i) {
      stream.ingest_json_record(
          make_order_json(i, "Customer " + std::to_string(i), "Widget",
                          1 + (i % 5), 9.99 + i, "pending", now));
    }

    stream.flush();
    stream.close();
    std::cout << "Ingested " << kRecords
              << " records using a custom HeadersProvider.\n";
  } catch (const zerobus::ZerobusException& e) {
    std::cerr << "Zerobus error: " << e.what()
              << " (retryable=" << (e.is_retryable() ? "true" : "false")
              << ")\n";
    return 1;
  }

  return 0;
}
