#ifndef ZEROBUS_PROTO_SCHEMA_HPP
#define ZEROBUS_PROTO_SCHEMA_HPP

#include <cstdint>
#include <string>
#include <vector>

namespace zerobus {

struct CZerobusProtoSchema;  // opaque FFI handle (defined in zerobus.h)

/// Builds a protobuf descriptor and encodes JSON records straight from Unity
/// Catalog table metadata — no pre-generated `.proto` file required.
///
/// Construct one with `from_uc_json()` passing the JSON body of
/// `GET /api/2.1/unity-catalog/tables/{name}`. Use `descriptor_bytes()` as the
/// `TableProperties::descriptor_proto` for a proto stream, then `encode_json()`
/// to turn each record's JSON into protobuf bytes for ingestion.
///
/// See the FFI README / `zerobus.h` for the JSON value shaping rules per Unity
/// Catalog column type (DATE/TIMESTAMP as integers, BINARY as base64, etc.).
///
/// Thread safety: a single handle may be used by concurrent readers
/// (`descriptor_bytes()`, `encode_json()`). Destruction must not race any such
/// call; this class is move-only and not copyable.
class ProtoSchema {
 public:
  /// Build a schema from Unity Catalog table metadata JSON. Throws
  /// `ZerobusException` on parse/build failure.
  static ProtoSchema from_uc_json(const std::string& uc_table_json);

  ~ProtoSchema();

  ProtoSchema(ProtoSchema&& other) noexcept;
  ProtoSchema& operator=(ProtoSchema&& other) noexcept;
  ProtoSchema(const ProtoSchema&) = delete;
  ProtoSchema& operator=(const ProtoSchema&) = delete;

  /// The serialized protobuf `DescriptorProto` for this table. Pass to
  /// `TableProperties::descriptor_proto`.
  std::vector<std::uint8_t> descriptor_bytes() const;

  /// Encode a single JSON record into protobuf bytes. Unknown keys are ignored.
  /// Throws `ZerobusException` if the record cannot be encoded (e.g. a missing
  /// required column or a malformed value).
  std::vector<std::uint8_t> encode_json(const std::string& record_json) const;

 private:
  explicit ProtoSchema(CZerobusProtoSchema* handle) : handle_(handle) {}

  CZerobusProtoSchema* handle_;
};

}  // namespace zerobus

#endif  // ZEROBUS_PROTO_SCHEMA_HPP
