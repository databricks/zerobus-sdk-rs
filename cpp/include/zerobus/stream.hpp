#ifndef ZEROBUS_STREAM_HPP
#define ZEROBUS_STREAM_HPP

#include <cstdint>
#include <memory>
#include <string>
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
/// Call `close()` explicitly when you need to observe close errors.
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

  /// Ingest a single protobuf-encoded record. Returns the logical offset
  /// assigned to the record. Throws `ZerobusException` on failure.
  std::int64_t ingest_proto_record(const std::uint8_t* data, std::size_t len);
  std::int64_t ingest_proto_record(const std::vector<std::uint8_t>& data);

  /// Ingest a single JSON record. Returns the assigned logical offset.
  std::int64_t ingest_json_record(const std::string& json);

  /// Ingest a batch of protobuf records. Returns the offset of the last record
  /// in the batch. Prefer batch APIs over per-record calls in hot paths — each
  /// FFI crossing has a fixed cost.
  std::int64_t ingest_proto_records(
      const std::vector<std::vector<std::uint8_t>>& records);

  /// Ingest a batch of JSON records. Returns the offset of the last record.
  std::int64_t ingest_json_records(const std::vector<std::string>& records);

  /// Fire-and-forget single-record ingestion. Returns immediately; only
  /// argument-validation errors are reported (as exceptions). Ingestion errors
  /// are silently dropped. The stream must outlive the background work.
  void ingest_proto_record_nowait(const std::uint8_t* data, std::size_t len);
  void ingest_proto_record_nowait(const std::vector<std::uint8_t>& data);
  void ingest_json_record_nowait(const std::string& json);

  /// Fire-and-forget batch ingestion. Copies the payloads before returning, so
  /// the caller's buffers may be released immediately.
  void ingest_proto_records_nowait(
      const std::vector<std::vector<std::uint8_t>>& records);
  void ingest_json_records_nowait(const std::vector<std::string>& records);

  /// Block until the record at `offset` has been acknowledged by the server.
  void wait_for_offset(std::int64_t offset);

  /// Flush all pending records and wait for their acknowledgment.
  void flush();

  /// Return all unacknowledged records from a closed or failed stream, for the
  /// caller to re-ingest on a fresh stream.
  std::vector<UnackedRecord> get_unacked_records();

  /// Gracefully close the stream, flushing pending records first. Idempotent:
  /// safe to call more than once. After this returns the stream is unusable.
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
