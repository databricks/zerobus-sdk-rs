#ifndef ZEROBUS_STREAM_HPP
#define ZEROBUS_STREAM_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

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
///   `flush_timeout_ms` (default 5 minutes) if the server is unresponsive. A
///   `Stream` that falls out of scope therefore drags that blocking close into
///   the destructor at an unpredictable point. Call `close()` at a controlled
///   point in your code instead.
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

  /// Fire-and-forget single-record ingestion: queues the record on a background
  /// task and returns immediately, without waiting for it to be sent.
  ///
  /// Only argument-validation errors are reported; errors during the background
  /// ingestion itself are silently dropped. The stream must outlive the
  /// background work.
  ///
  /// @param data Pointer to the protobuf-encoded record bytes.
  /// @param len Number of bytes in @p data.
  /// @throws ZerobusException on a closed stream or invalid argument.
  void ingest_proto_record_nowait(const std::uint8_t* data, std::size_t len);

  /// @overload
  /// @param data The protobuf-encoded record bytes.
  void ingest_proto_record_nowait(const std::vector<std::uint8_t>& data);

  /// Fire-and-forget single JSON-record ingestion. See
  /// ingest_proto_record_nowait() for the fire-and-forget semantics.
  ///
  /// @param json The record as a UTF-8 JSON string.
  /// @throws ZerobusException on a closed stream or invalid argument.
  void ingest_json_record_nowait(const std::string& json);

  /// Fire-and-forget batch ingestion: queues the records on a background task
  /// and returns immediately. The payloads are copied before returning, so the
  /// caller's buffers may be released right away. An empty batch is a no-op.
  ///
  /// @param records The protobuf-encoded records to ingest.
  /// @throws ZerobusException on a closed stream or invalid argument.
  void ingest_proto_records_nowait(
      const std::vector<std::vector<std::uint8_t>>& records);

  /// Fire-and-forget batch ingestion of JSON records. See
  /// ingest_proto_records_nowait() for the fire-and-forget semantics.
  ///
  /// @param records The records, each a UTF-8 JSON string.
  /// @throws ZerobusException on a closed stream or invalid argument.
  void ingest_json_records_nowait(const std::vector<std::string>& records);

  /// Block until the record at @p offset has been acknowledged by the server.
  ///
  /// @param offset A logical offset returned by an ingest call.
  /// @throws ZerobusException if the stream is closed or the wait fails.
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
  /// elapses (default 5 minutes), so call it at a controlled point rather than
  /// leaving it to the destructor.
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
  Stream(CZerobusStream* handle, std::shared_ptr<HeadersProvider> provider)
      : handle_(handle), provider_(std::move(provider)) {}

  CZerobusStream* handle_;
  // Kept alive for the stream's lifetime; the FFI callback holds a raw pointer
  // into this object.
  std::shared_ptr<HeadersProvider> provider_;
};

}  // namespace zerobus

#endif  // ZEROBUS_STREAM_HPP
