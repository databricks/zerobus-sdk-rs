#ifndef ZEROBUS_RECORD_HPP
#define ZEROBUS_RECORD_HPP

#include <cstdint>
#include <string>
#include <vector>

namespace zerobus {

/// A record recovered from a stream that was closed or failed before all
/// records were acknowledged. Returned by `Stream::get_unacked_records()`.
///
/// Owns its payload bytes (copied out of the FFI-owned buffer before the
/// underlying `CRecordArray` is freed).
class UnackedRecord {
 public:
  UnackedRecord(bool is_json, std::vector<std::uint8_t> data)
      : is_json_(is_json), data_(std::move(data)) {}

  /// Whether this record was a JSON record (otherwise protobuf bytes).
  bool is_json() const noexcept { return is_json_; }

  /// The raw record payload: UTF-8 JSON bytes if `is_json()`, otherwise
  /// protobuf-encoded bytes.
  const std::vector<std::uint8_t>& data() const noexcept { return data_; }

  /// Convenience: the payload interpreted as a JSON string. Only meaningful
  /// when `is_json()` is true.
  std::string as_string() const {
    return std::string(data_.begin(), data_.end());
  }

 private:
  bool is_json_;
  std::vector<std::uint8_t> data_;
};

}  // namespace zerobus

#endif  // ZEROBUS_RECORD_HPP
