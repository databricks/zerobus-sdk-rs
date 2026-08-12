#ifndef ZEROBUS_DETAIL_PROTO_BATCH_HPP
#define ZEROBUS_DETAIL_PROTO_BATCH_HPP

// Builds the parallel pointer/length arrays zerobus_stream_ingest_proto_records
// expects, without copying payloads.
//
// Kept out of stream.cpp so tests can reach it: a Stream needs a live server,
// but the invariants here — aliasing the caller's bytes, never handing the FFI
// a null payload — are testable alone (tests/proto_batch_test.cpp). Free of
// zerobus.h; the JSON equivalent stays in stream.cpp, where checked_c_str is.

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "zerobus/error.hpp"
#include "zerobus/record.hpp"

namespace zerobus {
namespace detail {

// An empty payload still crosses the FFI as a non-null pointer with length 0,
// rather than nullptr or a dangling data() result.
inline constexpr std::uint8_t kEmptyPayloadSentinel = 0;

inline const std::uint8_t* ptr_or_sentinel(
    const std::vector<std::uint8_t>& bytes) {
  return bytes.empty() ? &kEmptyPayloadSentinel : bytes.data();
}

// Same sentinel for the raw form, so {nullptr, 0} is a valid empty record.
inline const std::uint8_t* ptr_or_sentinel(const std::uint8_t* data,
                                           std::size_t len) {
  return len == 0 ? &kEmptyPayloadSentinel : data;
}

// The pointers alias the caller's record bytes, so a ProtoBatchView must not
// outlive the records it was built from.
struct ProtoBatchView {
  std::vector<const std::uint8_t*> ptrs;
  std::vector<std::uintptr_t> lens;
};

inline ProtoBatchView make_proto_batch(
    const std::vector<std::vector<std::uint8_t>>& records) {
  ProtoBatchView v;
  v.ptrs.reserve(records.size());
  v.lens.reserve(records.size());
  for (const auto& r : records) {
    v.ptrs.push_back(ptr_or_sentinel(r));
    v.lens.push_back(r.size());
  }
  return v;
}

// Borrowing form. Its own loop rather than materialising the views into a
// vector<vector<uint8_t>> and delegating above: those copies are the cost it
// exists to avoid.
inline ProtoBatchView make_proto_batch(const ProtoRecordView* records,
                                       std::size_t num_records) {
  if (records == nullptr && num_records != 0) {
    throw ZerobusException(
        "ingest_proto_records called with a null record array and a non-zero "
        "record count",
        false);
  }
  ProtoBatchView v;
  v.ptrs.reserve(num_records);
  v.lens.reserve(num_records);
  for (std::size_t i = 0; i < num_records; ++i) {
    // The core would dereference a sized null payload. Name the index so the
    // offending record is identifiable in a large batch.
    if (records[i].data == nullptr && records[i].size != 0) {
      throw ZerobusException(
          "proto record at index " + std::to_string(i) +
              " has a null data pointer with a non-zero size",
          false);
    }
    v.ptrs.push_back(ptr_or_sentinel(records[i].data, records[i].size));
    v.lens.push_back(records[i].size);
  }
  return v;
}

}  // namespace detail
}  // namespace zerobus

#endif  // ZEROBUS_DETAIL_PROTO_BATCH_HPP
