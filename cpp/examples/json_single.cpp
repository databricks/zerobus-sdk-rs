// Ingest a single JSON record using OAuth client credentials.
//
// Required environment variables:
//   ZEROBUS_SERVER_ENDPOINT   gRPC endpoint
//   DATABRICKS_WORKSPACE_URL  Unity Catalog / workspace URL
//   DATABRICKS_CLIENT_ID      OAuth client id
//   DATABRICKS_CLIENT_SECRET  OAuth client secret
//   ZEROBUS_TABLE_NAME        Target table: catalog.schema.table

#include <cstdlib>
#include <iostream>
#include <string>

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
                           .application_name("zerobus-cpp-json-single-example")
                           .build();

    zerobus::TableProperties table;
    table.table_name = env("ZEROBUS_TABLE_NAME");
    // Empty descriptor_proto => JSON stream.

    zerobus::StreamOptions options;
    options.record_type = zerobus::RecordType::Json;

    zerobus::Stream stream =
        sdk.create_stream(table, env("DATABRICKS_CLIENT_ID"),
                          env("DATABRICKS_CLIENT_SECRET"), options);

    std::int64_t offset =
        stream.ingest_json_record(R"({"id": 1, "payload": "hello from C++"})");
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
