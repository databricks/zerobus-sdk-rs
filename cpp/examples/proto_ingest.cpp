// Zerobus C++ SDK — proto ingestion with a schema built from Unity Catalog.
//
// This is the realistic production path, run as a continuous stream:
//   * Build a protobuf descriptor for the table straight from Unity Catalog
//     table metadata via ProtoSchema::from_uc_json() — no hand-written .proto.
//   * Open a *proto* stream using that descriptor.
//   * Emit one reading per tick for a fixed duration (see demo_config.hpp),
//     encoding each record's JSON into protobuf bytes with the same schema.
//   * flush() periodically rather than per record — the pattern for
//     continuous/unbounded streams.
//
// It also demonstrates two optional features:
//   * An async ack callback (StreamOptions::ack_callback) that observes
//     acknowledgements on a background task without blocking the ingest loop.
//   * A custom HeadersProvider for authentication (shown commented out at stream
//     creation), the alternative to OAuth client credentials.
//
// Non-secret connection info (endpoint, workspace URL, table, record) lives in
// demo_config.hpp. Two secrets plus the Unity Catalog table metadata come from
// the environment, so no credential is baked into source:
//
//   export ZEROBUS_CLIENT_ID="<oauth-client-id>"
//   export ZEROBUS_CLIENT_SECRET="<oauth-client-secret>"
//   # JSON body of GET /api/2.1/unity-catalog/tables/{full_name}:
//   export ZEROBUS_UC_TABLE_JSON="$(curl -s \
//       -H "Authorization: Bearer $DATABRICKS_TOKEN" \
//       "$WORKSPACE_URL/api/2.1/unity-catalog/tables/shinkansen.default.air_quality_zlata")"
//
//   ./build/examples/proto_ingest
//
// The record targets a table with three columns:
//   device_name STRING, temp INT, humidity INT
// (shinkansen.default.air_quality_zlata). Edit demo_config.hpp to change it.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "demo_config.hpp"
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

// A custom HeadersProvider is the alternative to OAuth client credentials: the
// core calls get_headers() whenever it needs fresh headers (possibly from
// another thread), and you return whatever the endpoint expects — at minimum an
// "authorization" bearer token and "x-databricks-zerobus-table-name". Throwing
// surfaces the message to the core as a headers-provider error. The provider
// must outlive the Stream (which holds a shared_ptr to it). See
// include/zerobus/headers_provider.hpp for the full contract. Used in the
// commented-out create_stream() call below.
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
  // Secrets + UC metadata from the environment; the rest from demo_config.hpp.
  const std::string client_id = require_env("ZEROBUS_CLIENT_ID");
  const std::string client_secret = require_env("ZEROBUS_CLIENT_SECRET");
  const std::string uc_table_json = require_env("ZEROBUS_UC_TABLE_JSON");
  const std::string endpoint = zerobus_demo::kZerobusEndpoint;
  const std::string uc_url = zerobus_demo::kWorkspaceUrl;
  const std::string table = zerobus_demo::table_name();

  try {
    // 1. Build a protobuf schema for the table from its Unity Catalog metadata.
    //    This yields both the descriptor (for stream creation) and a
    //    JSON->proto encoder — no .proto file required.
    zerobus::ProtoSchema schema =
        zerobus::ProtoSchema::from_uc_json(uc_table_json);

    // 2. Build the SDK.
    zerobus::Sdk sdk = zerobus::Sdk::builder()
                           .endpoint(endpoint)
                           .unity_catalog_url(uc_url)
                           .application_name("proto-ingest")
                           .build();

    // 3. Open a proto stream, passing the descriptor.
    zerobus::TableProperties props;
    props.table_name = table;
    props.descriptor_proto = schema.descriptor_bytes();

    zerobus::StreamOptions options;
    options.record_type = zerobus::RecordType::Proto;

    // Optional: an async ack callback observes acknowledgements on a background
    // task, so durability is tracked without ever blocking the ingest loop.
    // AckCallback::from() adapts lambdas; both handlers must be noexcept, run on
    // another thread (synchronize shared state — here a std::atomic), and must
    // not call back into the Stream. forever() makes close() wait for every
    // in-flight callback, so none can touch `acked` after it goes out of scope.
    // See include/zerobus/ack_callback.hpp for the full contract.
    std::atomic<std::int64_t> acked{0};
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
    //       std::make_shared<BearerTokenProvider>(table, my_token);
    //   zerobus::Stream stream = sdk.create_stream(props, provider, options);

    // 4. Stream continuously: emit one reading per tick for the configured
    //    duration. Each iteration encodes the record's JSON to proto and
    //    ingests it — with NO per-record wait. flush() every N records bounds
    //    how far records lag behind durability without paying a round-trip per
    //    record, which is the correct pattern for a continuous/unbounded
    //    stream.
    using clock = std::chrono::steady_clock;
    const auto deadline = clock::now() + std::chrono::milliseconds(
                                             zerobus_demo::kStreamDurationMs);
    const auto tick = std::chrono::milliseconds(zerobus_demo::kTickIntervalMs);

    int sent = 0;
    std::int64_t last_offset = -1;
    while (clock::now() < deadline) {
      const std::vector<std::uint8_t> encoded =
          schema.encode_json(zerobus_demo::make_record(sent));
      last_offset = stream.ingest_proto_record(encoded);
      ++sent;

      // Periodic flush: bounds how far acks lag behind ingestion without
      // paying a round-trip per record.
      if (sent % zerobus_demo::kFlushEveryNRecords == 0) {
        stream.flush();
        std::cout << "... sent " << sent << " (last offset " << last_offset
                  << ")\n";
      }

      std::this_thread::sleep_for(tick);
    }

    // 5. Final flush to block until every remaining record is acknowledged,
    //    then close. The ack callback keeps firing during flush()/close();
    //    forever() ensures close() waits for the last one before `acked` dies.
    stream.flush();
    stream.close();

    std::cout << "Done. sent " << sent << " records; last offset "
              << last_offset << "; callback observed " << acked.load()
              << " acknowledgements.\n";
  } catch (const zerobus::ZerobusException& e) {
    std::cerr << "Zerobus error: " << e.what()
              << " (retryable=" << (e.is_retryable() ? "true" : "false")
              << ")\n";
    return 1;
  }

  return 0;
}
