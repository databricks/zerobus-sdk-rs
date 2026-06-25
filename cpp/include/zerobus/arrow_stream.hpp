#ifndef ZEROBUS_ARROW_STREAM_HPP
#define ZEROBUS_ARROW_STREAM_HPP

#include <cstdint>
#include <memory>
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
class ArrowStream {
 public:
  ~ArrowStream();

  ArrowStream(ArrowStream&& other) noexcept;
  ArrowStream& operator=(ArrowStream&& other) noexcept;
  ArrowStream(const ArrowStream&) = delete;
  ArrowStream& operator=(const ArrowStream&) = delete;

  /// Ingest one Arrow RecordBatch supplied as Arrow IPC stream bytes (a
  /// self-contained IPC stream: schema message + one record batch message).
  /// Returns the logical offset assigned to the batch.
  std::int64_t ingest_batch(const std::uint8_t* ipc_bytes, std::size_t len);
  std::int64_t ingest_batch(const std::vector<std::uint8_t>& ipc_bytes);

  /// Block until the batch at `offset` has been acknowledged.
  void wait_for_offset(std::int64_t offset);

  /// Flush all pending batches and wait for their acknowledgment.
  void flush();

  /// Return all unacknowledged batches from a closed or failed stream, each as
  /// a self-contained Arrow IPC stream (schema + one batch).
  std::vector<std::vector<std::uint8_t>> get_unacked_batches();

  /// Gracefully close the stream, flushing pending batches first. Idempotent.
  void close();

  /// Whether the stream has been closed (queried from the FFI when live).
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
