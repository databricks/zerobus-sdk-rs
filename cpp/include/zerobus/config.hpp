#ifndef ZEROBUS_CONFIG_HPP
#define ZEROBUS_CONFIG_HPP

#include <cstddef>
#include <cstdint>
#include <optional>

namespace zerobus {

/// Wire format of a record. Matches `CStreamConfigurationOptions.record_type`.
enum class RecordType : std::int32_t {
  Unspecified = 0,
  Proto = 1,
  Json = 2,
};

/// Configuration for an ingestion stream.
///
/// Defaults mirror the Rust core's `zerobus_get_default_config()`. The actual
/// C struct passed to the FFI is seeded from the live FFI defaults and then
/// overridden field-by-field with the values below, so unknown future fields
/// keep their FFI defaults.
struct StreamOptions {
  /// Maximum number of in-flight (unacknowledged) requests.
  std::size_t max_inflight_requests = 1'000'000;
  /// Whether automatic stream recovery is enabled.
  bool recovery = true;
  /// Total time budget for a recovery attempt.
  std::uint64_t recovery_timeout_ms = 15'000;
  /// Backoff between recovery retries.
  std::uint64_t recovery_backoff_ms = 2'000;
  /// Number of recovery retries before giving up.
  std::uint32_t recovery_retries = 4;
  /// How long to wait for a server ack before considering the stream stalled.
  std::uint64_t server_lack_of_ack_timeout_ms = 60'000;
  /// Time budget for `flush()`.
  std::uint64_t flush_timeout_ms = 300'000;
  /// Record wire format. Must match the stream's table (proto vs JSON).
  RecordType record_type = RecordType::Proto;
  /// Max time to wait during a server-initiated pause before recovering.
  /// `nullopt` = wait the full server-specified duration; `0` = recover
  /// immediately; `>0` = wait up to min(this, server duration).
  std::optional<std::uint64_t> stream_paused_max_wait_time_ms;
  /// Max time to wait for a headers-provider callback to return.
  /// `nullopt` leaves the FFI default in place.
  std::optional<std::uint64_t> callback_max_wait_time_ms;
};

}  // namespace zerobus

#endif  // ZEROBUS_CONFIG_HPP
