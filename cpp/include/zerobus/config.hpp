#ifndef ZEROBUS_CONFIG_HPP
#define ZEROBUS_CONFIG_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <variant>

#include "zerobus/ack_callback.hpp"

namespace zerobus {

/// Wire format of a record. Matches `CStreamConfigurationOptions.record_type`.
enum class RecordType : std::int32_t {
  Unspecified = 0,
  Proto = 1,
  Json = 2,
};

/// How long `close()` waits for the async ack-callback task to drain before
/// giving up. A callback that outruns the wait can outlive `close()` (see
/// `AckCallback` for the lifetime contract). Exactly one of three states holds,
/// so the finite budget and the "wait forever" flag can never contradict:
///
/// - `use_default()` (the default): the finite budget baked into the FFI.
/// - `duration(ms)`: an explicit finite budget; the task is aborted after `ms`.
/// - `forever()`: no deadline — `close()` blocks until every in-flight callback
///   finishes. The only policy that guarantees no callback runs after `close()`
///   returns; the tradeoff is that a wedged callback blocks `close()`.
class CallbackWaitPolicy {
 public:
  /// Use the finite budget baked into the FFI default.
  CallbackWaitPolicy() = default;

  /// Use the FFI default finite budget (same as the default constructor).
  static CallbackWaitPolicy use_default() { return CallbackWaitPolicy{}; }

  /// Wait up to @p ms milliseconds, then abort the callback task.
  static CallbackWaitPolicy duration(std::uint64_t ms) {
    return CallbackWaitPolicy{Duration{ms}};
  }

  /// Drain with no deadline; `close()` blocks until callbacks finish.
  static CallbackWaitPolicy forever() { return CallbackWaitPolicy{Forever{}}; }

  bool is_default() const { return std::holds_alternative<Default>(state_); }
  bool is_forever() const { return std::holds_alternative<Forever>(state_); }

  /// The finite budget in milliseconds, or `nullopt` unless this is a
  /// `duration(ms)` policy.
  std::optional<std::uint64_t> duration_ms() const {
    if (const auto* d = std::get_if<Duration>(&state_)) {
      return d->ms;
    }
    return std::nullopt;
  }

 private:
  struct Default {};
  struct Forever {};
  struct Duration {
    std::uint64_t ms;
  };
  using State = std::variant<Default, Forever, Duration>;

  explicit CallbackWaitPolicy(State state) : state_(std::move(state)) {}

  State state_{Default{}};
};

/// Configuration for a JSON or Protocol Buffer ingestion stream.
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
  /// How long `close()` waits for the ack-callback task to drain. See
  /// `CallbackWaitPolicy`; defaults to the finite FFI budget.
  CallbackWaitPolicy callback_wait_policy;
  /// Optional async ack/error callback (`nullptr` = none). The `Stream` keeps a
  /// `shared_ptr` for its lifetime; see `AckCallback` for the full contract,
  /// including the lifetime and no-throw rules.
  std::shared_ptr<AckCallback> ack_callback;
};

/// Arrow IPC compression codec. Matches the `ipc_compression` field encoding
/// in `CArrowStreamConfigurationOptions` (-1 = none, 0 = LZ4_FRAME, 1 = ZSTD).
///
/// Named `NoCompression`, not `None`, to avoid X11's `#define None 0L`, which
/// the preprocessor expands even inside an `enum class`.
enum class IpcCompression : std::int32_t {
  NoCompression = -1,
  Lz4Frame = 0,
  Zstd = 1,
};

/// Configuration for an Arrow Flight ingestion stream.
///
/// The scalar defaults below are hand-kept in sync with the Rust core and sent
/// to the FFI verbatim (see `to_c()`). `arrow_config_defaults_test` fails the
/// build if they drift from `zerobus_arrow_get_default_config()`.
struct ArrowStreamOptions {
  /// Maximum number of in-flight (unacknowledged) batches.
  std::size_t max_inflight_batches = 1'000;
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
  /// Connection establishment timeout.
  std::uint64_t connection_timeout_ms = 30'000;
  /// Arrow IPC compression codec.
  IpcCompression ipc_compression = IpcCompression::NoCompression;
  /// Max wait during a server-initiated pause before recovering. `nullopt` =
  /// full server duration; `0` = recover immediately; `x` = min(x, server).
  std::optional<std::uint64_t> stream_paused_max_wait_time_ms;
};

}  // namespace zerobus

#endif  // ZEROBUS_CONFIG_HPP
