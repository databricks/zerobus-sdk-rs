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

// Reject operations on a stream whose handle has already been released — after
// a successful close() (which nulls handle_) or on a moved-from Stream. Without
// this, the null handle reaches the FFI and bubbles back as the low-level
// "Stream pointer is null"; checking here instead surfaces the same clear
// "Stream has been closed" message that Go (and, modulo wording, Java's
// ensureOpen()) report. get_unacked_records() is deliberately *not* blocked
// after a failed close(), which keeps handle_ alive precisely so recovery
// stays possible.
void ensure_open(const CZerobusStream* handle) {
  if (handle == nullptr) {
    throw ZerobusException("Stream has been closed", false);
  }
}

// Validate a server-assigned offset returned by an FFI ingest call. A real
// offset is non-negative; the FFI uses negative values only as sentinels (-1
// error, -2 empty batch) that should always travel with a failed CResult. Once
// throw_if_error() has cleared the failure path, a negative offset would mean
// the FFI handed back a sentinel with success set — never a usable offset, and
// dangerous if passed on to wait_for_offset(). Reject it, mirroring Go's
// explicit `offset < 0` guard. (The batch APIs short-circuit the empty case to
// -1 before the FFI call, so that path never reaches here.)
std::int64_t checked_offset(std::int64_t offset) {
  if (offset < 0) {
    throw ZerobusException("unexpected negative offset from Zerobus FFI",
                           false);
  }
  return offset;
}

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
    // Whole batch fails fast if any record has an embedded NUL.
    v.ptrs.push_back(detail::checked_c_str(r, "JSON record"));
  }
  return v;
}

// Releases the FFI-owned CRecordArray on scope exit, so the array is freed even
// if copying the records into owning UnackedRecords throws (e.g. std::bad_alloc
// while growing the output vector) — not only on the straight-line return.
struct RecordArrayGuard {
  CRecordArray array;
  ~RecordArrayGuard() { zerobus_free_record_array(array); }
  RecordArrayGuard(const RecordArrayGuard&) = delete;
  RecordArrayGuard& operator=(const RecordArrayGuard&) = delete;
};

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
  ensure_open(handle_);
  detail::ResultGuard guard;
  std::int64_t offset =
      zerobus_stream_ingest_proto_record(handle_, data, len, guard.ptr());
  guard.throw_if_error();
  return checked_offset(offset);
}

// Vector overload: adapt to the (pointer, length) form, substituting the
// sentinel so an empty record still passes a non-null pointer.
std::int64_t Stream::ingest_proto_record(
    const std::vector<std::uint8_t>& data) {
  return ingest_proto_record(ptr_or_sentinel(data), data.size());
}

std::int64_t Stream::ingest_json_record(const std::string& json) {
  ensure_open(handle_);
  detail::ResultGuard guard;
  std::int64_t offset = zerobus_stream_ingest_json_record(
      handle_, detail::checked_c_str(json, "JSON record"), guard.ptr());
  guard.throw_if_error();
  return checked_offset(offset);
}

std::int64_t Stream::ingest_proto_records(
    const std::vector<std::vector<std::uint8_t>>& records) {
  ensure_open(handle_);
  // An empty batch is a no-op, not an error: there are no records to ingest and
  // no last-record offset to return. Match the other SDKs (Rust core returns
  // Ok(None); the FFI returns its -2 sentinel; Go returns -1) by returning -1
  // without crossing the FFI, rather than throwing.
  if (records.empty()) {
    return -1;
  }
  ProtoBatchView v = make_proto_batch(records);
  detail::ResultGuard guard;
  std::int64_t offset = zerobus_stream_ingest_proto_records(
      handle_, v.ptrs.data(), v.lens.data(), v.ptrs.size(), guard.ptr());
  guard.throw_if_error();
  return checked_offset(offset);
}

std::int64_t Stream::ingest_json_records(
    const std::vector<std::string>& records) {
  ensure_open(handle_);
  // See ingest_proto_records: an empty batch is a no-op returning -1, not a
  // throw.
  if (records.empty()) {
    return -1;
  }
  JsonBatchView v = make_json_batch(records);
  detail::ResultGuard guard;
  std::int64_t offset = zerobus_stream_ingest_json_records(
      handle_, v.ptrs.data(), v.ptrs.size(), guard.ptr());
  guard.throw_if_error();
  return checked_offset(offset);
}

void Stream::ingest_proto_record_nowait(const std::uint8_t* data,
                                        std::size_t len) {
  ensure_open(handle_);
  detail::ResultGuard guard;
  zerobus_stream_ingest_proto_record_nowait(handle_, data, len, guard.ptr());
  guard.throw_if_error();
}

void Stream::ingest_proto_record_nowait(const std::vector<std::uint8_t>& data) {
  ingest_proto_record_nowait(ptr_or_sentinel(data), data.size());
}

void Stream::ingest_json_record_nowait(const std::string& json) {
  ensure_open(handle_);
  detail::ResultGuard guard;
  zerobus_stream_ingest_json_record_nowait(
      handle_, detail::checked_c_str(json, "JSON record"), guard.ptr());
  guard.throw_if_error();
}

void Stream::ingest_proto_records_nowait(
    const std::vector<std::vector<std::uint8_t>>& records) {
  ensure_open(handle_);
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
  ensure_open(handle_);
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
  ensure_open(handle_);
  detail::ResultGuard guard;
  zerobus_stream_wait_for_offset(handle_, offset, guard.ptr());
  guard.throw_if_error();
}

void Stream::flush() {
  ensure_open(handle_);
  detail::ResultGuard guard;
  zerobus_stream_flush(handle_, guard.ptr());
  guard.throw_if_error();
}

// Copy each record out of the FFI-owned array into owning UnackedRecords, then
// free the array — the borrowed buffers are only valid until that free, so the
// copy must happen first.
std::vector<UnackedRecord> Stream::get_unacked_records() {
  // A failed close() keeps handle_ alive, so recovery still passes this guard;
  // only a successful close (or moved-from stream) is rejected, where there is
  // nothing left to recover.
  ensure_open(handle_);
  detail::ResultGuard guard;
  CRecordArray array = zerobus_stream_get_unacked_records(handle_, guard.ptr());
  // On error the array is empty; surface the error first.
  guard.throw_if_error();
  // Own the array before the copy, so it is freed even if a copy below throws.
  RecordArrayGuard array_guard{array};

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
