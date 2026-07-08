#ifndef ZEROBUS_CONFIG_HPP
#define ZEROBUS_CONFIG_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

#include "zerobus/ack_callback.hpp"

namespace zerobus {

/// Wire format of a record. Matches `CStreamConfigurationOptions.record_type`.
enum class RecordType : std::int32_t {
  Unspecified = 0,
  Proto = 1,
  Json = 2,
};

/// Configuration for an ingestion stream.
///
/// The scalar defaults below are hand-kept in sync with the Rust core and sent
/// to the FFI verbatim (see `to_c()`). `config_defaults_test` fails the build
/// if they drift from `zerobus_get_default_config()`.
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
  /// Optional async ack/error callback (default `nullptr` = none). The `Stream`
  /// keeps a `shared_ptr` to it for its lifetime. See `AckCallback` for the
  /// full lifetime and threading contract.
  std::shared_ptr<AckCallback> ack_callback;
};

}  // namespace zerobus

#endif  // ZEROBUS_CONFIG_HPP
