// Implementation of ArrowStream (declared in zerobus/arrow_stream.hpp), the
// Beta Arrow Flight ingestion path.
//
// Each method forwards to the zerobus_arrow_stream_* C FFI entry points and
// routes failures through detail::ResultGuard. get_unacked_batches copies the
// FFI-owned batch payloads out before freeing the returned array. The
// destructor closes best-effort (swallowing errors); close() surfaces them.
// Public API documentation lives on the header.
#include "zerobus/arrow_stream.hpp"

#include <cstddef>
#include <utility>

#include "detail/ffi_util.hpp"

namespace zerobus {

namespace {

// Reject use of a released handle (after a successful close() or a move) with a
// clear message instead of the low-level FFI null-pointer error. (Mirrors
// stream.cpp.)
void ensure_open(const CArrowStream* handle) {
  if (handle == nullptr) {
    throw ZerobusException("Stream has been closed", false);
  }
}

// Reject a negative offset from an ingest call: it means the FFI returned a
// sentinel with success set, never a usable offset. (Mirrors stream.cpp.)
std::int64_t checked_offset(std::int64_t offset) {
  if (offset < 0) {
    throw ZerobusException("unexpected negative offset from Zerobus FFI",
                           false);
  }
  return offset;
}

// Frees the FFI-owned array on scope exit, so it is released even if a copy
// below throws. (Mirrors RecordArrayGuard in stream.cpp.)
struct BatchArrayGuard {
  CArrowBatchArray array;
  ~BatchArrayGuard() { zerobus_arrow_free_batch_array(array); }
  BatchArrayGuard(const BatchArrayGuard&) = delete;
  BatchArrayGuard& operator=(const BatchArrayGuard&) = delete;
};

}  // namespace

// Best-effort graceful close in the destructor (errors are swallowed — a
// destructor must not throw), then free the Rust-owned handle.
ArrowStream::~ArrowStream() {
  if (handle_ != nullptr) {
    detail::ResultGuard guard;
    zerobus_arrow_stream_close(handle_, guard.ptr());
    zerobus_arrow_stream_free(handle_);
    handle_ = nullptr;
  }
}

// Move carries the handle, nulling the source so the handle is closed/freed
// exactly once. The headers provider is owned by the FFI, not the ArrowStream.
ArrowStream::ArrowStream(ArrowStream&& other) noexcept
    : handle_(other.handle_) {
  other.handle_ = nullptr;
}

ArrowStream& ArrowStream::operator=(ArrowStream&& other) noexcept {
  if (this != &other) {
    // Close + free any stream we already hold before adopting other's.
    if (handle_ != nullptr) {
      detail::ResultGuard guard;
      zerobus_arrow_stream_close(handle_, guard.ptr());
      zerobus_arrow_stream_free(handle_);
    }
    handle_ = other.handle_;
    other.handle_ = nullptr;
  }
  return *this;
}

// Ingest one batch (schema + records as Arrow IPC bytes); returns the assigned
// offset. Like the Stream ingest path, failure comes back via the ResultGuard.
std::int64_t ArrowStream::ingest_batch(const std::uint8_t* ipc_bytes,
                                       std::size_t len) {
  ensure_open(handle_);
  detail::ResultGuard guard;
  std::int64_t offset =
      zerobus_arrow_stream_ingest_batch(handle_, ipc_bytes, len, guard.ptr());
  guard.throw_if_error();
  return checked_offset(offset);
}

// Vector overload forwarding to the (pointer, length) form.
std::int64_t ArrowStream::ingest_batch(
    const std::vector<std::uint8_t>& ipc_bytes) {
  return ingest_batch(ipc_bytes.data(), ipc_bytes.size());
}

void ArrowStream::wait_for_offset(std::int64_t offset) {
  ensure_open(handle_);
  // Reject negative offsets before the FFI (matches Stream).
  if (offset < 0) {
    throw ZerobusException("wait_for_offset called with a negative offset",
                           false);
  }
  detail::ResultGuard guard;
  zerobus_arrow_stream_wait_for_offset(handle_, offset, guard.ptr());
  guard.throw_if_error();
}

void ArrowStream::flush() {
  ensure_open(handle_);
  detail::ResultGuard guard;
  zerobus_arrow_stream_flush(handle_, guard.ptr());
  guard.throw_if_error();
}

// Copy each unacked batch out of the FFI-owned array (parallel batches/lengths
// arrays) into owning vectors, then free the array — the borrowed bytes are
// only valid until that free.
std::vector<std::vector<std::uint8_t>> ArrowStream::get_unacked_batches() {
  // Still callable after a failed close() (handle_ kept alive) so recovery
  // works; only a successful close or a move is rejected.
  ensure_open(handle_);
  detail::ResultGuard guard;
  CArrowBatchArray array =
      zerobus_arrow_stream_get_unacked_batches(handle_, guard.ptr());
  guard.throw_if_error();
  // Own the array before the copy so it is freed on any exit path.
  BatchArrayGuard array_guard{array};

  std::vector<std::vector<std::uint8_t>> out;
  if (array.batches != nullptr && array.count > 0) {
    out.reserve(array.count);
    for (std::size_t i = 0; i < array.count; ++i) {
      const std::uint8_t* bytes = array.batches[i];
      std::size_t len = array.lengths[i];
      std::vector<std::uint8_t> batch;
      if (bytes != nullptr && len > 0) {
        batch.assign(bytes, bytes + len);
      }
      out.push_back(std::move(batch));
    }
  }
  return out;
}

// Explicit close: surfaces a failed flush/close by throwing (the destructor
// does not). Idempotent via the null-handle guard.
//
// On failure the handle is deliberately kept (not freed, handle_ left non-null)
// so the caller can still recover buffered batches via get_unacked_batches()
// and retry close(); the destructor frees it as a last resort. Only a
// successful close frees the handle and marks the stream closed. (Mirrors
// Stream::close().)
void ArrowStream::close() {
  if (handle_ == nullptr) {
    return;
  }
  detail::ResultGuard guard;
  bool ok = zerobus_arrow_stream_close(handle_, guard.ptr());
  if (!ok) {
    // Keep handle_ alive for recovery, then surface the error.
    guard.throw_if_error();
    // Fallback if close reported failure without setting an error message.
    throw ZerobusException("failed to close Arrow stream", false);
  }
  zerobus_arrow_stream_free(handle_);
  handle_ = nullptr;
}

// Once close()/destruction has nulled the handle the stream is closed; while it
// is still live, defer to the core's own closed state via the FFI.
bool ArrowStream::is_closed() const noexcept {
  if (handle_ == nullptr) {
    return true;
  }
  return zerobus_arrow_stream_is_closed(handle_);
}

}  // namespace zerobus
