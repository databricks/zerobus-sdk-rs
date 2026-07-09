#ifndef ZEROBUS_STREAM_HPP
#define ZEROBUS_STREAM_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zerobus/ack_callback.hpp"
#include "zerobus/headers_provider.hpp"
#include "zerobus/record.hpp"

namespace zerobus {

struct CZerobusStream;  // opaque FFI handle (defined in zerobus.h)
class Sdk;

/// A unidirectional ingestion stream to a single Zerobus table.
///
/// Created via `Sdk::create_stream`. Move-only; the destructor closes the
/// stream gracefully (best effort) and frees the underlying FFI resources.
///
/// Prefer calling `close()` explicitly rather than relying on the destructor:
/// - `close()` flushes pending records and surfaces any error by throwing,
///   whereas the destructor swallows it.
/// - Closing flushes synchronously and can block up to the stream's
///   `flush_timeout_ms` (default 5 minutes) if the server is unresponsive, and
///   then drains any registered `ack_callback` task per
///   `StreamOptions::callback_wait_policy` (which may be unbounded). A `Stream`
///   that falls out of scope therefore drags that blocking close into the
///   destructor at an unpredictable point. Call `close()` at a controlled point
///   in your code instead.
///
/// Thread safety: not safe for concurrent use from multiple threads. Serialize
/// access externally, matching the Java and Rust core contracts.
class Stream {
 public:
  ~Stream();

  Stream(Stream&& other) noexcept;
  Stream& operator=(Stream&& other) noexcept;
  Stream(const Stream&) = delete;
  Stream& operator=(const Stream&) = delete;

  /// Ingest a single protobuf-encoded record, blocking until it is queued.
  ///
  /// @param data Pointer to the protobuf-encoded record bytes.
  /// @param len Number of bytes in @p data.
  /// @return The logical offset assigned to the record.
  /// @throws ZerobusException if the stream is closed or ingestion fails.
  std::int64_t ingest_proto_record(const std::uint8_t* data, std::size_t len);

  /// @overload
  /// @param data The protobuf-encoded record bytes.
  std::int64_t ingest_proto_record(const std::vector<std::uint8_t>& data);

  /// Ingest a single JSON record, blocking until it is queued.
  ///
  /// @param json The record as a UTF-8 JSON string.
  /// @return The logical offset assigned to the record.
  /// @throws ZerobusException if the stream is closed or ingestion fails.
  std::int64_t ingest_json_record(const std::string& json);

  /// Ingest a batch of protobuf records, blocking until they are queued.
  ///
  /// Prefer the batch APIs over per-record calls in hot paths: each FFI
  /// crossing has a fixed cost that batching amortizes.
  ///
  /// @param records The protobuf-encoded records to ingest.
  /// @return The logical offset of the last record in the batch, or -1 if
  ///         @p records is empty (a no-op).
  /// @throws ZerobusException if the stream is closed or ingestion fails.
  std::int64_t ingest_proto_records(
      const std::vector<std::vector<std::uint8_t>>& records);

  /// Ingest a batch of JSON records, blocking until they are queued.
  ///
  /// @param records The records, each a UTF-8 JSON string.
  /// @return The logical offset of the last record in the batch, or -1 if
  ///         @p records is empty (a no-op).
  /// @throws ZerobusException if the stream is closed or ingestion fails.
  std::int64_t ingest_json_records(const std::vector<std::string>& records);

  /// Block until the record at @p offset has been acknowledged by the server.
  ///
  /// @param offset A logical offset returned by an ingest call. Must be
  ///        non-negative; the -1 returned by an empty ingest_*_records() batch
  ///        is rejected rather than forwarded to the server.
  /// @throws ZerobusException if @p offset is negative, the stream is closed,
  ///         or the wait fails.
  void wait_for_offset(std::int64_t offset);

  /// Flush all pending records and block until they are acknowledged.
  ///
  /// @throws ZerobusException if the stream is closed or the flush fails.
  void flush();

  /// Return all unacknowledged records from a closed or failed stream, for the
  /// caller to re-ingest on a fresh stream. Remains callable after a failed
  /// `close()` (which keeps the handle alive precisely so recovery is
  /// possible).
  ///
  /// @return The records that were ingested but not acknowledged.
  /// @throws ZerobusException if the records cannot be retrieved.
  std::vector<UnackedRecord> get_unacked_records();

  /// Gracefully close the stream, flushing pending records first. Idempotent:
  /// safe to call more than once.
  ///
  /// Blocks until the flush completes or the stream's `flush_timeout_ms`
  /// elapses (default 5 minutes), then drains any registered `ack_callback`
  /// task before returning, per `StreamOptions::callback_wait_policy`
  /// (`forever()` can block `close()` on a wedged callback). Call it at a
  /// controlled point rather than leaving it to the destructor.
  ///
  /// On success the stream becomes unusable. If the close fails it keeps the
  /// stream handle alive, so the caller can still recover buffered data via
  /// `get_unacked_records()` and/or retry `close()`; the destructor frees the
  /// handle as a last resort.
  ///
  /// @throws ZerobusException if the flush or close fails. The handle is kept
  ///         alive for recovery (see above).
  void close();

  /// Whether `close()` has already been called (locally observed).
  bool is_closed() const noexcept { return handle_ == nullptr; }

 private:
  friend class Sdk;
  Stream(CZerobusStream* handle, std::shared_ptr<HeadersProvider> provider,
         std::shared_ptr<AckCallback> ack_callback)
      : handle_(handle),
        provider_(std::move(provider)),
        ack_callback_(std::move(ack_callback)) {}

  CZerobusStream* handle_;
  // Kept alive for the stream's lifetime; the core holds a raw pointer to it.
  std::shared_ptr<HeadersProvider> provider_;
  // Also raw-pointed-to by the core (ack user_data), but with a weaker bound: a
  // callback can still run after close(), so dropping this at ~Stream() can
  // free it mid-call (see AckCallback). May be null.
  std::shared_ptr<AckCallback> ack_callback_;
};

}  // namespace zerobus

#endif  // ZEROBUS_STREAM_HPP
