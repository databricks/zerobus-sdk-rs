#ifndef ZEROBUS_EXAMPLES_DEMO_CONFIG_HPP
#define ZEROBUS_EXAMPLES_DEMO_CONFIG_HPP

// Shared, non-secret configuration for the Zerobus C++ SDK examples.
//
// Everything here is safe to check in. The two genuinely secret values — the
// OAuth client id and client secret — are deliberately NOT here: they are read
// from the environment at runtime (see require_env / client credentials in each
// example), so no credential is ever baked into source.
//
// Edit the values below to point the examples at your own table/workspace.

#include <cstdint>
#include <string>

namespace zerobus_demo {

// --- Table location (composed into the fully-qualified catalog.schema.table
//     name the SDK expects) ------------------------------------------------
inline constexpr const char* kCatalog = "shinkansen";
inline constexpr const char* kSchema = "default";
inline constexpr const char* kTable = "air_quality_zlata";

// --- Workspace / endpoints -------------------------------------------------
// Zerobus gRPC endpoint. The https:// scheme is optional (the SDK prepends it
// if absent) and the workspace id is auto-extracted from this host, so it need
// not be passed separately.
inline constexpr const char* kZerobusEndpoint =
    "https://6051921418418893.zerobus.us-west-2.staging.cloud.databricks.com";

// Unity Catalog / workspace URL. Used for the OAuth client-credentials token
// exchange ({url}/oidc/v1/token).
inline constexpr const char* kWorkspaceUrl =
    "https://e2-dogfood.staging.cloud.databricks.com";

// Workspace id — informational only (the SDK derives it from the endpoint).
inline constexpr const char* kWorkspaceId = "6051921418418893";

// --- Demo record -----------------------------------------------------------
// One record matching the table columns: device_name STRING, temp INT,
// humidity INT. Used verbatim by the JSON example and encoded to proto by the
// proto example.
inline constexpr const char* kJsonRecord =
    R"({ "device_name": "device_num_1", "temp": 28, "humidity": 60 })";

// --- Continuous-stream demo tuning (proto_ingest) --------------------------
// proto_ingest simulates a live sensor: it emits one reading every
// kTickIntervalMs for kStreamDurationMs total, calling flush() every
// kFlushEveryNRecords records rather than waiting per record. The async ack
// callback reports durability as acks arrive.
inline constexpr std::uint64_t kStreamDurationMs = 30'000;  // ~30s of streaming
inline constexpr std::uint64_t kTickIntervalMs = 200;       // ~5 records/sec
inline constexpr int kFlushEveryNRecords = 25;              // periodic flush

// The fully-qualified table name: catalog.schema.table.
inline std::string table_name() {
  return std::string(kCatalog) + "." + kSchema + "." + kTable;
}

// Build one JSON record for tick `n`, varying temp/humidity so the rows differ.
// Matches the table columns: device_name STRING, temp INT, humidity INT.
inline std::string make_record(int n) {
  const int temp = 20 + (n % 15);      // 20..34
  const int humidity = 40 + (n % 40);  // 40..79
  return "{ \"device_name\": \"device_num_1\", \"temp\": " +
         std::to_string(temp) + ", \"humidity\": " + std::to_string(humidity) +
         " }";
}

}  // namespace zerobus_demo

#endif  // ZEROBUS_EXAMPLES_DEMO_CONFIG_HPP
