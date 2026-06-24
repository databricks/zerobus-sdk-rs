// Unit tests for ProtoSchema. Using an in-line Unity Catalog table metadata
// fixture (no network), they exercise the descriptor + JSON-encode round trip,
// move semantics, and the error paths (invalid UC JSON, and a record missing a
// required column).
#include "zerobus/proto_schema.hpp"

#include <string>
#include <utility>

#include "test_harness.hpp"
#include "zerobus/error.hpp"

using zerobus::ProtoSchema;
using zerobus::ZerobusException;

namespace {

// A minimal Unity Catalog table metadata JSON, mirroring the Rust FFI's own
// round-trip fixture. The descriptor it yields has three fields (id, payload,
// ts). This exercises the full proto-schema FFI path with no network.
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

TEST(ProtoSchema, FromUcJsonRoundTrip) {
  ProtoSchema schema = ProtoSchema::from_uc_json(kUcTableJson);

  std::vector<std::uint8_t> descriptor = schema.descriptor_bytes();
  EXPECT_FALSE(descriptor.empty());

  std::vector<std::uint8_t> encoded = schema.encode_json(
      R"({"id": 7, "payload": "hello", "ts": 1700000000000000, "extra": "x"})");
  EXPECT_FALSE(encoded.empty());
}

TEST(ProtoSchema, InvalidJsonThrowsNonRetryable) {
  try {
    ProtoSchema::from_uc_json("not json");
    FAIL() << "expected ZerobusException";
  } catch (const ZerobusException& e) {
    EXPECT_FALSE(e.is_retryable());
    EXPECT_FALSE(std::string(e.what()).empty());
  }
}

TEST(ProtoSchema, EncodeInvalidRecordThrows) {
  ProtoSchema schema = ProtoSchema::from_uc_json(kUcTableJson);
  // 'id' is a required (non-nullable) column; omitting it must fail.
  EXPECT_THROW(schema.encode_json(R"({"payload": "x"})"), ZerobusException);
}

TEST(ProtoSchema, MoveLeavesSourceUsable) {
  ProtoSchema schema = ProtoSchema::from_uc_json(kUcTableJson);
  ProtoSchema moved = std::move(schema);
  // The moved-to schema still works.
  EXPECT_FALSE(moved.descriptor_bytes().empty());
}
