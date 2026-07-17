// Live integration test: create-stream -> ingest -> flush -> close against a
// real endpoint, mirroring the Java/TypeScript suites. Gated on env vars;
// SKIPS (exits 0) when unset, so `make test` and CI stay hermetic.
//
// Required: ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL,
// ZEROBUS_TABLE_NAME, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET.
// Optional: ZEROBUS_TEST_RECORD_JSON — a record matching the table schema
// (the default almost certainly won't match; set it for a real run).

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "zerobus/error.hpp"
#include "zerobus/sdk.hpp"

namespace {

int g_failures = 0;

void fail(const char* msg) {
  std::fprintf(stderr, "FAIL: %s\n", msg);
  ++g_failures;
}

// Returns the env var value, or an empty string if unset/empty.
std::string env(const char* name) {
  const char* v = std::getenv(name);
  return (v != nullptr) ? std::string(v) : std::string();
}

}  // namespace

int main() {
  const std::string endpoint = env("ZEROBUS_SERVER_ENDPOINT");
  const std::string workspace_url = env("DATABRICKS_WORKSPACE_URL");
  const std::string table_name = env("ZEROBUS_TABLE_NAME");
  const std::string client_id = env("DATABRICKS_CLIENT_ID");
  const std::string client_secret = env("DATABRICKS_CLIENT_SECRET");
  // Required too: without a record matching the table, flush() would fail with
  // a schema mismatch, so a half-configured env should skip rather than go red.
  const std::string record_json = env("ZEROBUS_TEST_RECORD_JSON");

  // Skip (not fail) when any required variable is missing.
  if (endpoint.empty() || workspace_url.empty() || table_name.empty() ||
      client_id.empty() || client_secret.empty() || record_json.empty()) {
    std::printf(
        "SKIP integration_test: set ZEROBUS_SERVER_ENDPOINT, "
        "DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME, DATABRICKS_CLIENT_ID, "
        "DATABRICKS_CLIENT_SECRET, and ZEROBUS_TEST_RECORD_JSON (a record "
        "matching ZEROBUS_TABLE_NAME) to run it.\n");
    return 0;
  }

  try {
    zerobus::Sdk sdk = zerobus::Sdk::builder()
                           .endpoint(endpoint)
                           .unity_catalog_url(workspace_url)
                           .build();

    // Empty descriptor_proto => JSON stream.
    zerobus::TableProperties table;
    table.table_name = table_name;

    zerobus::Stream stream = sdk.create_stream(table, client_id, client_secret);

    // Queue in a loop, flush once — never wait per record.
    constexpr int kRecordCount = 10;
    std::int64_t last_offset = -1;
    for (int i = 0; i < kRecordCount; ++i) {
      last_offset = stream.ingest_json_record(record_json);
    }
    if (last_offset < 0) {
      fail("ingest returned a negative offset for a non-empty record");
    }

    // One flush covers every offset (acks are monotonic), so a single flush()
    // confirms all 10 records are durable. close() flushes again internally,
    // but calling flush() explicitly first surfaces an ack failure here rather
    // than folded into close()'s teardown.
    stream.flush();
    stream.close();

    std::printf("integration_test: ingested %d records, flushed, closed\n",
                kRecordCount);
  } catch (const zerobus::ZerobusException& e) {
    fail("live ingestion path threw ZerobusException");
    std::fprintf(stderr, "  what(): %s (retryable=%s)\n", e.what(),
                 e.is_retryable() ? "true" : "false");
  }

  if (g_failures != 0) {
    std::fprintf(stderr, "%d check(s) failed.\n", g_failures);
    return 1;
  }
  return 0;
}
