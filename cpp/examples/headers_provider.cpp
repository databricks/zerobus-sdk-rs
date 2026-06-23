// Authenticate a stream with a custom HeadersProvider instead of static OAuth
// client credentials. Implement get_headers() to supply (and refresh) whatever
// authorization headers your environment requires.
//
// See json_single.cpp for the connection environment variables (the client
// id/secret are not used on this path).

#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <string>

#include "zerobus/zerobus.hpp"

namespace {
std::string env(const char* name) {
  const char* v = std::getenv(name);
  return v != nullptr ? std::string(v) : std::string();
}

// Supplies a bearer token. In a real implementation, refresh the token here as
// needed; get_headers() is called by the core whenever fresh headers are
// needed.
class BearerTokenProvider : public zerobus::HeadersProvider {
 public:
  explicit BearerTokenProvider(std::string token) : token_(std::move(token)) {}

  std::map<std::string, std::string> get_headers() override {
    return {{"Authorization", "Bearer " + token_}};
  }

 private:
  std::string token_;
};
}  // namespace

int main() {
  try {
    zerobus::Sdk sdk = zerobus::Sdk::builder()
                           .endpoint(env("ZEROBUS_SERVER_ENDPOINT"))
                           .application_name("zerobus-cpp-headers-example")
                           .build();

    zerobus::TableProperties table;
    table.table_name = env("ZEROBUS_TABLE_NAME");

    zerobus::StreamOptions options;
    options.record_type = zerobus::RecordType::Json;

    auto provider =
        std::make_shared<BearerTokenProvider>(env("DATABRICKS_TOKEN"));

    zerobus::Stream stream = sdk.create_stream(table, provider, options);

    std::int64_t offset =
        stream.ingest_json_record(R"({"id": 1, "payload": "via headers"})");
    std::cout << "ingested at offset " << offset << "\n";

    stream.flush();
    stream.close();
    std::cout << "done\n";
    return 0;
  } catch (const zerobus::ZerobusException& e) {
    std::cerr << "zerobus error: " << e.what()
              << " (retryable=" << (e.is_retryable() ? "true" : "false")
              << ")\n";
    return 1;
  }
}
