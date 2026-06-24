// Ingest protobuf records whose descriptor is built directly from Unity Catalog
// table metadata — no pre-generated .proto file or protoc needed.
//
// Fetch GET /api/2.1/unity-catalog/tables/{name} and pass its JSON body via the
// ZEROBUS_UC_TABLE_JSON environment variable (or adapt this to read a file).
// See json_single.cpp for the connection environment variables.

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
    // Build the protobuf schema (descriptor + encoder) from UC table metadata.
    zerobus::ProtoSchema schema =
        zerobus::ProtoSchema::from_uc_json(env("ZEROBUS_UC_TABLE_JSON"));

    zerobus::Sdk sdk = zerobus::Sdk::builder()
                           .endpoint(env("ZEROBUS_SERVER_ENDPOINT"))
                           .unity_catalog_url(env("DATABRICKS_WORKSPACE_URL"))
                           .application_name("zerobus-cpp-proto-example")
                           .build();

    zerobus::TableProperties table;
    table.table_name = env("ZEROBUS_TABLE_NAME");
    table.descriptor_proto = schema.descriptor_bytes();  // => proto stream

    zerobus::StreamOptions options;  // record_type defaults to Proto
    zerobus::Stream stream =
        sdk.create_stream(table, env("DATABRICKS_CLIENT_ID"),
                          env("DATABRICKS_CLIENT_SECRET"), options);

    // Encode JSON records into protobuf bytes using the UC-derived schema.
    std::vector<std::vector<std::uint8_t>> batch;
    for (int i = 0; i < 10; ++i) {
      batch.push_back(schema.encode_json(R"({"id": )" + std::to_string(i) +
                                         R"(, "payload": "proto"})"));
    }

    std::int64_t last_offset = stream.ingest_proto_records(batch);
    std::cout << "ingested " << batch.size()
              << " proto records, last offset = " << last_offset << "\n";

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
