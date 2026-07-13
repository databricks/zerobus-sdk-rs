// Unit tests for ProtoSchema. Using an inline Unity Catalog table-metadata
// fixture (no network), they exercise the descriptor + JSON-encode round trip,
// the error paths (invalid UC JSON, and a record missing a required column),
// and move semantics — including that a moved-from schema is left with a null
// handle but stays safe to use (the FFI null-handle contract, which the Rust
// FFI's own tests cover for zerobus_proto_schema_descriptor_bytes(null)).
//
// Dependency-free: exercises only the JSON+FFI path, no live server.

#include "zerobus/proto_schema.hpp"

#include <cstdint>
#include <cstdio>
#include <string>
#include <utility>
#include <vector>

#include "zerobus/error.hpp"

namespace {

int g_failures = 0;

void fail(const char* msg) {
  std::fprintf(stderr, "FAIL: %s\n", msg);
  ++g_failures;
}

// A minimal Unity Catalog table-metadata JSON. The descriptor it yields has
// three fields (id, payload, ts). Mirrors the Rust FFI's own round-trip
// fixture; exercises the full proto-schema FFI path with no network.
const char* kUcTableJson = R"({
  "name": "events",
  "catalog_name": "main",
  "schema_name": "analytics",
  "columns": [
    {"name": "id", "type_name": "BIGINT", "type_text": "bigint", "nullable": false, "position": 0},
    {"name": "payload", "type_name": "STRING", "type_text": "string", "nullable": true, "position": 1},
    {"name": "ts", "type_name": "TIMESTAMP", "type_text": "timestamp", "nullable": true, "position": 2}
  ]
})";

}  // namespace

int main() {
  using zerobus::ProtoSchema;
  using zerobus::ZerobusException;

  // Round trip: build from UC JSON, get a non-empty descriptor, and encode a
  // record (unknown keys such as "extra" are ignored) to non-empty bytes.
  {
    try {
      ProtoSchema schema = ProtoSchema::from_uc_json(kUcTableJson);
      if (schema.descriptor_bytes().empty()) {
        fail("descriptor_bytes() was empty for a valid schema");
      }
      std::vector<std::uint8_t> encoded = schema.encode_json(
          R"({"id": 7, "payload": "hello", "ts": 1700000000000000, "extra": "x"})");
      if (encoded.empty()) {
        fail("encode_json() produced no bytes for a valid record");
      }
    } catch (const ZerobusException& e) {
      fail("valid schema round trip threw unexpectedly");
      std::fprintf(stderr, "  what(): %s\n", e.what());
    }
  }

  // Invalid UC JSON throws a non-retryable ZerobusException with a message.
  {
    bool threw = false;
    try {
      ProtoSchema::from_uc_json("not json");
    } catch (const ZerobusException& e) {
      threw = true;
      if (e.is_retryable()) {
        fail("invalid UC JSON should be non-retryable");
      }
      if (std::string(e.what()).empty()) {
        fail("invalid UC JSON exception had an empty message");
      }
    }
    if (!threw) {
      fail("invalid UC JSON did not throw");
    }
  }

  // Encoding a record that omits the required (non-nullable) 'id' column
  // throws.
  {
    ProtoSchema schema = ProtoSchema::from_uc_json(kUcTableJson);
    bool threw = false;
    try {
      schema.encode_json(R"({"payload": "x"})");
    } catch (const ZerobusException&) {
      threw = true;
    }
    if (!threw) {
      fail("encoding a record missing a required column did not throw");
    }
  }

  // Move leaves the source with a null handle but safe to use: the moved-to
  // schema owns the descriptor; the moved-from schema returns empty rather
  // than crashing.
  {
    ProtoSchema schema = ProtoSchema::from_uc_json(kUcTableJson);
    ProtoSchema moved = std::move(schema);
    if (moved.descriptor_bytes().empty()) {
      fail("moved-to schema lost its descriptor");
    }
    // NOLINTNEXTLINE(bugprone-use-after-move) — deliberately exercising the
    // documented moved-from null-handle contract.
    if (!schema.descriptor_bytes().empty()) {
      fail("moved-from schema should return an empty descriptor");
    }
  }

  if (g_failures != 0) {
    std::fprintf(stderr, "%d check(s) failed.\n", g_failures);
    return 1;
  }
  std::printf("ProtoSchema round trip, error paths, and move semantics hold\n");
  return 0;
}
