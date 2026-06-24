// Implementation of ProtoSchema (declared in zerobus/proto_schema.hpp).
//
// Wraps the zerobus_proto_schema_* C FFI: from_uc_json builds the schema handle
// from Unity Catalog table metadata, descriptor_bytes copies out the borrowed
// descriptor bytes, and encode_json copies out (then frees, via
// zerobus_free_proto_bytes) the FFI-allocated protobuf buffer. Public API
// documentation lives on the header.
#include "zerobus/proto_schema.hpp"

#include <utility>

#include "detail/ffi_util.hpp"

namespace zerobus {

ProtoSchema ProtoSchema::from_uc_json(const std::string& uc_table_json) {
  detail::ResultGuard guard;
  CZerobusProtoSchema* handle =
      zerobus_proto_schema_from_uc_json(uc_table_json.c_str(), guard.ptr());
  if (handle == nullptr) {
    guard.throw_if_error();
    throw ZerobusException("failed to build proto schema from UC JSON", false);
  }
  return ProtoSchema(handle);
}

ProtoSchema::~ProtoSchema() {
  if (handle_ != nullptr) {
    zerobus_proto_schema_free(handle_);
    handle_ = nullptr;
  }
}

ProtoSchema::ProtoSchema(ProtoSchema&& other) noexcept
    : handle_(other.handle_) {
  other.handle_ = nullptr;
}

ProtoSchema& ProtoSchema::operator=(ProtoSchema&& other) noexcept {
  if (this != &other) {
    if (handle_ != nullptr) {
      zerobus_proto_schema_free(handle_);
    }
    handle_ = other.handle_;
    other.handle_ = nullptr;
  }
  return *this;
}

std::vector<std::uint8_t> ProtoSchema::descriptor_bytes() const {
  std::uintptr_t len = 0;
  const std::uint8_t* bytes =
      zerobus_proto_schema_descriptor_bytes(handle_, &len);
  if (bytes == nullptr || len == 0) {
    return {};
  }
  // Copy out of the SDK-owned buffer; the borrow is only valid until free.
  return std::vector<std::uint8_t>(bytes, bytes + len);
}

std::vector<std::uint8_t> ProtoSchema::encode_json(
    const std::string& record_json) const {
  detail::ResultGuard guard;
  std::uint8_t* out_data = nullptr;
  std::uintptr_t out_len = 0;
  bool ok = zerobus_proto_schema_encode_json(handle_, record_json.c_str(),
                                             &out_data, &out_len, guard.ptr());
  if (!ok) {
    guard.throw_if_error();
    throw ZerobusException("failed to encode JSON record", false);
  }
  std::vector<std::uint8_t> result;
  if (out_data != nullptr && out_len > 0) {
    result.assign(out_data, out_data + out_len);
  }
  zerobus_free_proto_bytes(out_data, out_len);
  return result;
}

}  // namespace zerobus
