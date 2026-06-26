// Implementation of Stream (declared in zerobus/stream.hpp).
//
// A thin forwarding layer over the zerobus_stream_* C FFI entry points. The
// file-local helpers build the small parallel pointer/length arrays the batch
// entry points expect, and every fallible call routes its CResult through
// detail::ResultGuard. The destructor and move-assignment close best-effort
// (swallowing errors), whereas close() surfaces them. Public API documentation
// lives on the header; comments here cover only implementation details.
#include "zerobus/stream.hpp"

#include <cstddef>
#include <utility>

#include "detail/ffi_util.hpp"

namespace zerobus {

namespace {

// An empty payload still needs a valid, non-null pointer to pass across the FFI
// (paired with length 0); hand out the address of a static sentinel byte rather
// than nullptr or a dangling data() result.
const std::uint8_t* ptr_or_sentinel(const std::vector<std::uint8_t>& bytes) {
  static const std::uint8_t kEmptyPayloadSentinel = 0;
  return bytes.empty() ? &kEmptyPayloadSentinel : bytes.data();
}

// Build the parallel pointer/length arrays the batch FFI entry points expect.
struct ProtoBatchView {
  std::vector<const std::uint8_t*> ptrs;
  std::vector<std::uintptr_t> lens;
};

ProtoBatchView make_proto_batch(
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

// JSON records cross the FFI as an array of NUL-terminated C strings, so unlike
// the proto path there is no parallel length array — only the pointers.
struct JsonBatchView {
  std::vector<const char*> ptrs;
};

JsonBatchView make_json_batch(const std::vector<std::string>& records) {
  JsonBatchView v;
  v.ptrs.reserve(records.size());
  for (const auto& r : records) {
    v.ptrs.push_back(r.c_str());
  }
  return v;
}

}  // namespace

Stream::~Stream() {
  if (handle_ != nullptr) {
    // Best-effort graceful close; never throw from a destructor.
    detail::ResultGuard guard;
    zerobus_stream_close(handle_, guard.ptr());
    zerobus_stream_free(handle_);
    handle_ = nullptr;
  }
}

// Move transfers both the handle and the headers-provider that must outlive it,
// nulling the source handle so only one Stream ever closes/frees it.
Stream::Stream(Stream&& other) noexcept
    : handle_(other.handle_), provider_(std::move(other.provider_)) {
  other.handle_ = nullptr;
}

Stream& Stream::operator=(Stream&& other) noexcept {
  if (this != &other) {
    // Close + free any stream we currently hold before adopting other's; the
    // close is best-effort here since assignment cannot throw usefully.
    if (handle_ != nullptr) {
      detail::ResultGuard guard;
      zerobus_stream_close(handle_, guard.ptr());
      zerobus_stream_free(handle_);
    }
    handle_ = other.handle_;
    provider_ = std::move(other.provider_);
    other.handle_ = nullptr;
  }
  return *this;
}

// The ingest methods below share one shape: route a CResult through a
// ResultGuard, call the matching zerobus_stream_* entry point, then
// throw_if_error(). The blocking variants return the server-assigned offset;
// the _nowait variants return void and only report argument-validation errors.

std::int64_t Stream::ingest_proto_record(const std::uint8_t* data,
                                         std::size_t len) {
  detail::ResultGuard guard;
  std::int64_t offset =
      zerobus_stream_ingest_proto_record(handle_, data, len, guard.ptr());
  guard.throw_if_error();
  return offset;
}

// Vector overload: adapt to the (pointer, length) form, substituting the
// sentinel so an empty record still passes a non-null pointer.
std::int64_t Stream::ingest_proto_record(
    const std::vector<std::uint8_t>& data) {
  return ingest_proto_record(ptr_or_sentinel(data), data.size());
}

std::int64_t Stream::ingest_json_record(const std::string& json) {
  detail::ResultGuard guard;
  std::int64_t offset =
      zerobus_stream_ingest_json_record(handle_, json.c_str(), guard.ptr());
  guard.throw_if_error();
  return offset;
}

std::int64_t Stream::ingest_proto_records(
    const std::vector<std::vector<std::uint8_t>>& records) {
  // Reject empty batches explicitly. An empty vector yields a (possibly null)
  // data() pointer, which the FFI either rejects as "Invalid records pointer"
  // or accepts and answers with its internal -2 sentinel — neither is a real
  // offset. Fail clearly here instead of returning a misleading value.
  if (records.empty()) {
    throw ZerobusException("cannot ingest an empty record batch", false);
  }
  ProtoBatchView v = make_proto_batch(records);
  detail::ResultGuard guard;
  std::int64_t offset = zerobus_stream_ingest_proto_records(
      handle_, v.ptrs.data(), v.lens.data(), v.ptrs.size(), guard.ptr());
  guard.throw_if_error();
  return offset;
}

std::int64_t Stream::ingest_json_records(
    const std::vector<std::string>& records) {
  // See ingest_proto_records: reject empty batches rather than returning the
  // FFI's -2 empty-batch sentinel as if it were a real offset.
  if (records.empty()) {
    throw ZerobusException("cannot ingest an empty record batch", false);
  }
  JsonBatchView v = make_json_batch(records);
  detail::ResultGuard guard;
  std::int64_t offset = zerobus_stream_ingest_json_records(
      handle_, v.ptrs.data(), v.ptrs.size(), guard.ptr());
  guard.throw_if_error();
  return offset;
}

void Stream::ingest_proto_record_nowait(const std::uint8_t* data,
                                        std::size_t len) {
  detail::ResultGuard guard;
  zerobus_stream_ingest_proto_record_nowait(handle_, data, len, guard.ptr());
  guard.throw_if_error();
}

void Stream::ingest_proto_record_nowait(const std::vector<std::uint8_t>& data) {
  ingest_proto_record_nowait(ptr_or_sentinel(data), data.size());
}

void Stream::ingest_json_record_nowait(const std::string& json) {
  detail::ResultGuard guard;
  zerobus_stream_ingest_json_record_nowait(handle_, json.c_str(), guard.ptr());
  guard.throw_if_error();
}

void Stream::ingest_proto_records_nowait(
    const std::vector<std::vector<std::uint8_t>>& records) {
  // Nothing to enqueue; skip the FFI call (and its null-pointer ambiguity).
  if (records.empty()) {
    return;
  }
  ProtoBatchView v = make_proto_batch(records);
  detail::ResultGuard guard;
  zerobus_stream_ingest_proto_records_nowait(
      handle_, v.ptrs.data(), v.lens.data(), v.ptrs.size(), guard.ptr());
  guard.throw_if_error();
}

void Stream::ingest_json_records_nowait(
    const std::vector<std::string>& records) {
  // Nothing to enqueue; skip the FFI call (and its null-pointer ambiguity).
  if (records.empty()) {
    return;
  }
  JsonBatchView v = make_json_batch(records);
  detail::ResultGuard guard;
  zerobus_stream_ingest_json_records_nowait(handle_, v.ptrs.data(),
                                            v.ptrs.size(), guard.ptr());
  guard.throw_if_error();
}

void Stream::wait_for_offset(std::int64_t offset) {
  detail::ResultGuard guard;
  zerobus_stream_wait_for_offset(handle_, offset, guard.ptr());
  guard.throw_if_error();
}

void Stream::flush() {
  detail::ResultGuard guard;
  zerobus_stream_flush(handle_, guard.ptr());
  guard.throw_if_error();
}

// Copy each record out of the FFI-owned array into owning UnackedRecords, then
// free the array — the borrowed buffers are only valid until that free, so the
// copy must happen first.
std::vector<UnackedRecord> Stream::get_unacked_records() {
  detail::ResultGuard guard;
  CRecordArray array = zerobus_stream_get_unacked_records(handle_, guard.ptr());
  // On error the array is empty; surface the error first.
  guard.throw_if_error();

  std::vector<UnackedRecord> out;
  if (array.records != nullptr && array.len > 0) {
    out.reserve(array.len);
    for (std::size_t i = 0; i < array.len; ++i) {
      const CRecord& rec = array.records[i];
      std::vector<std::uint8_t> data;
      if (rec.data != nullptr && rec.data_len > 0) {
        data.assign(rec.data, rec.data + rec.data_len);
      }
      out.emplace_back(rec.is_json, std::move(data));
    }
  }
  zerobus_free_record_array(array);
  return out;
}

// Explicit close: unlike the destructor, this surfaces a failed flush/close by
// throwing. Idempotent via the null-handle guard.
//
// On failure the handle is deliberately *kept* (not freed, handle_ left
// non-null) so the caller can still recover buffered data via
// get_unacked_records() and retry close(); the destructor frees it as a last
// resort. Only a successful close frees the handle and marks the stream closed.
void Stream::close() {
  if (handle_ == nullptr) {
    return;
  }
  detail::ResultGuard guard;
  bool ok = zerobus_stream_close(handle_, guard.ptr());
  if (!ok) {
    // Keep handle_ alive for recovery, then surface the error.
    guard.throw_if_error();
    // Fallback if close reported failure without setting an error message.
    throw ZerobusException("failed to close stream", false);
  }
  zerobus_stream_free(handle_);
  handle_ = nullptr;
}

}  // namespace zerobus
