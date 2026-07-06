#ifndef ZEROBUS_ARROW_STREAM_HPP
#define ZEROBUS_ARROW_STREAM_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "zerobus/headers_provider.hpp"

namespace zerobus {

struct CArrowStream;  // opaque FFI handle (defined in zerobus.h)
class Sdk;

/// An Arrow Flight ingestion stream (Beta).
///
/// Records are supplied as Arrow IPC stream bytes (schema + one record batch).
/// Created via `Sdk::create_arrow_stream`. Move-only; the destructor closes and
/// frees the stream. The API is stabilising but may still change before GA.
///
/// As with `Stream`, prefer calling `close()` explicitly: it surfaces close
/// errors (the destructor swallows them) and flushes synchronously, which can
/// block up to `flush_timeout_ms` (default 5 minutes) if the server is
/// unresponsive. Letting the object fall out of scope drags that blocking close
/// into the destructor.
///
/// Thread safety: not safe for concurrent use from multiple threads.
///
/// Only the IPC-bytes ingest path is wrapped. The FFI's
/// `zerobus_arrow_stream_ingest_batch_via_record_batch` takes an opaque
/// `RecordBatch*` no C++ caller can construct, so it is intentionally omitted.
class ArrowStream {
 public:
  ~ArrowStream();

  ArrowStream(ArrowStream&& other) noexcept;
  ArrowStream& operator=(ArrowStream&& other) noexcept;
  ArrowStream(const ArrowStream&) = delete;
  ArrowStream& operator=(const ArrowStream&) = delete;

  /// Ingest one Arrow RecordBatch as a self-contained Arrow IPC stream (schema
  /// message + one record batch message). Returns the assigned logical offset.
  ///
  /// Asynchronous: this only queues the batch. Queue many, then `flush()` once;
  /// do not call `wait_for_offset` per batch (one round-trip each kills
  /// throughput). The offset is a handle to wait on later, not a cue to wait.
  ///
  /// @code
  ///   for (const auto& batch : batches) {
  ///     stream.ingest_batch(batch);  // queue only — do NOT wait here
  ///   }
  ///   stream.flush();                // wait once for all pending acks
  /// @endcode
  std::int64_t ingest_batch(const std::uint8_t* ipc_bytes, std::size_t len);
  std::int64_t ingest_batch(const std::vector<std::uint8_t>& ipc_bytes);

  /// Block until the batch at `offset` has been acknowledged. Reserve for
  /// low-volume cases; in hot paths `flush()` once instead (see
  /// `ingest_batch`).
  void wait_for_offset(std::int64_t offset);

  /// Flush all pending batches and wait for their acknowledgment.
  void flush();

  /// Return all unacknowledged batches from a closed or failed stream, each as
  /// a self-contained Arrow IPC stream (schema + one batch). Remains callable
  /// after a failed `close()`, which keeps the handle alive so recovery is
  /// possible.
  std::vector<std::vector<std::uint8_t>> get_unacked_batches();

  /// Gracefully close the stream, flushing pending batches first. Idempotent.
  ///
  /// On success the stream becomes unusable. If the close fails it throws
  /// `ZerobusException` but keeps the handle alive, so the caller can still
  /// recover buffered batches via `get_unacked_batches()` and/or retry
  /// `close()`; the destructor frees the handle as a last resort.
  void close();

  /// Whether the stream has been closed. Unlike `Stream::is_closed()` (a pure
  /// `handle == nullptr` check), while the handle is live this also queries the
  /// core, so it can report `true` for a stream the core closed on its own. The
  /// query is conservative: it reports `true` if it cannot answer.
  bool is_closed() const noexcept;

 private:
  friend class Sdk;
  ArrowStream(CArrowStream* handle, std::shared_ptr<HeadersProvider> provider)
      : handle_(handle), provider_(std::move(provider)) {}

  CArrowStream* handle_;
  std::shared_ptr<HeadersProvider> provider_;
};

}  // namespace zerobus

#endif  // ZEROBUS_ARROW_STREAM_HPP
