// Zerobus C++ SDK — JSON quickstart.
//
// The shortest path to a working stream: build an Sdk, open a JSON stream to a
// table, ingest a batch of records, and flush once. No .proto file and no
// descriptor are needed for a JSON stream — the server maps each record's JSON
// fields onto the table's columns by name.
//
// Non-secret connection info (endpoint, workspace URL, table) lives in
// demo_config.hpp so it can be checked in. Only the two secrets come from the
// environment, so no credential is ever baked into source:
//
//   export ZEROBUS_CLIENT_ID="<oauth-client-id>"
//   export ZEROBUS_CLIENT_SECRET="<oauth-client-secret>"
//
//   ./build/examples/json_quickstart
//
// The record targets a table with three columns:
//   device_name STRING, temp INT, humidity INT
// (shinkansen.default.air_quality_zlata). Edit demo_config.hpp to point at your
// own table/workspace and change the record.

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "demo_config.hpp"
#include "zerobus/zerobus.hpp"

namespace {

// Read a required environment variable or exit with a clear message. Returning
// on the error path (rather than throwing) keeps the misconfiguration case
// distinct from a genuine SDK ZerobusException below.
std::string require_env(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') {
    std::cerr << "error: environment variable " << name << " is not set.\n"
              << "See the header of this file for the required variables.\n";
    std::exit(2);
  }
  return value;
}

}  // namespace

int main() {
  // Secrets from the environment; everything else from demo_config.hpp.
  const std::string client_id = require_env("ZEROBUS_CLIENT_ID");
  const std::string client_secret = require_env("ZEROBUS_CLIENT_SECRET");
  const std::string endpoint = zerobus_demo::kZerobusEndpoint;
  const std::string uc_url = zerobus_demo::kWorkspaceUrl;
  const std::string table = zerobus_demo::table_name();

  try {
    // 1. Build the SDK — an authenticated connection factory. TLS is on by
    //    default; the builder is consumed by build().
    zerobus::Sdk sdk = zerobus::Sdk::builder()
                           .endpoint(endpoint)
                           .unity_catalog_url(uc_url)
                           .application_name("json-quickstart")
                           .build();

    // 2. Open a JSON stream to the table. record_type must be Json to match the
    //    JSON payloads we ingest below.
    zerobus::TableProperties props;
    props.table_name = table;  // descriptor_proto left empty for JSON.

    zerobus::StreamOptions options;
    options.record_type = zerobus::RecordType::Json;

    zerobus::Stream stream =
        sdk.create_stream(props, client_id, client_secret, options);

    // 3. Ingest. Build the batch, then hand it over in one call. Note there is
    //    NO per-record wait here: ingest queues the records and returns; the
    //    SDK sends and awaits acks on background tasks. Waiting after each
    //    record would force a full server round-trip per record and collapse
    //    throughput. Prefer the batch API in hot paths — it also amortizes the
    //    per-call FFI crossing. Here the batch is a single configured record.
    const std::vector<std::string> records{zerobus_demo::kJsonRecord};

    const std::int64_t last_offset = stream.ingest_json_records(records);
    std::cout << "Queued " << records.size()
              << " record(s); last offset = " << last_offset << "\n";

    // 4. Flush once at the end: block until every queued record is durably
    //    acknowledged by the server. This is the single wait point.
    stream.flush();
    std::cout << "Flushed — all records acknowledged.\n";

    // 5. Close at a controlled point rather than leaving it to the destructor.
    //    close() surfaces any final error by throwing; ~Stream() would swallow
    //    it.
    stream.close();
    std::cout << "Stream closed cleanly.\n";
  } catch (const zerobus::ZerobusException& e) {
    std::cerr << "Zerobus error: " << e.what()
              << " (retryable=" << (e.is_retryable() ? "true" : "false")
              << ")\n";
    return 1;
  }

  return 0;
}
