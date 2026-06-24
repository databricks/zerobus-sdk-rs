// Ingest a batch of JSON records. Batch APIs amortize the per-call FFI cost and
// should be preferred in hot paths.
//
// See json_single.cpp for the required environment variables.

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "zerobus/zerobus.hpp"

namespace {
std::string env(const char* name) {
  const char* v = std::getenv(name);
  return v != nullptr ? std::string(v) : std::string();
}
}  // namespace

int main() {
  try {
    zerobus::Sdk sdk = zerobus::Sdk::builder()
                           .endpoint(env("ZEROBUS_SERVER_ENDPOINT"))
                           .unity_catalog_url(env("DATABRICKS_WORKSPACE_URL"))
                           .application_name("zerobus-cpp-json-batch-example")
                           .build();

    zerobus::TableProperties table;
    table.table_name = env("ZEROBUS_TABLE_NAME");

    zerobus::StreamOptions options;
    options.record_type = zerobus::RecordType::Json;

    zerobus::Stream stream =
        sdk.create_stream(table, env("DATABRICKS_CLIENT_ID"),
                          env("DATABRICKS_CLIENT_SECRET"), options);

    std::vector<std::string> records;
    for (int i = 0; i < 100; ++i) {
      records.push_back(R"({"id": )" + std::to_string(i) +
                        R"(, "payload": "batch"})");
    }

    std::int64_t last_offset = stream.ingest_json_records(records);
    std::cout << "ingested " << records.size()
              << " records, last offset = " << last_offset << "\n";

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
