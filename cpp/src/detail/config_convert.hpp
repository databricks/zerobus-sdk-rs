#ifndef ZEROBUS_DETAIL_CONFIG_CONVERT_HPP
#define ZEROBUS_DETAIL_CONFIG_CONVERT_HPP

#include <cstdint>
#include <limits>

#include "detail/ack_callback.hpp"
#include "detail/ffi_util.hpp"
#include "zerobus/config.hpp"

namespace zerobus {
namespace detail {

/// Build the C stream-config struct: seed from the live FFI defaults, then
/// overwrite every scalar unconditionally (guarded by `config_defaults_test`).
/// The seed survives only for unknown future fields and for the callback wait
/// budget when the policy is `use_default()` (below). Ack-callback fields are
/// set from `opts.ack_callback` and zeroed explicitly when it is unset, so
/// correctness does not depend on the FFI default seeding them null.
inline CStreamConfigurationOptions to_c(const StreamOptions& opts) {
  CStreamConfigurationOptions c = zerobus_get_default_config();
  c.max_inflight_requests = opts.max_inflight_requests;
  c.recovery = opts.recovery;
  c.recovery_timeout_ms = opts.recovery_timeout_ms;
  c.recovery_backoff_ms = opts.recovery_backoff_ms;
  c.recovery_retries = opts.recovery_retries;
  c.server_lack_of_ack_timeout_ms = opts.server_lack_of_ack_timeout_ms;
  c.flush_timeout_ms = opts.flush_timeout_ms;
  c.record_type = static_cast<std::int32_t>(opts.record_type);
  if (opts.stream_paused_max_wait_time_ms.has_value()) {
    c.has_stream_paused_max_wait_time_ms = true;
    c.stream_paused_max_wait_time_ms = *opts.stream_paused_max_wait_time_ms;
  } else {
    c.has_stream_paused_max_wait_time_ms = false;
    c.stream_paused_max_wait_time_ms = 0;
  }
  // Map the wait policy to the FFI's (presence flag, value) pair. forever()
  // clears the flag so Rust reads the budget as None (drain indefinitely);
  // duration(ms) sets an explicit finite budget; use_default() leaves the
  // finite FFI seed in place.
  if (opts.callback_wait_policy.is_forever()) {
    c.has_callback_max_wait_time_ms = false;
    c.callback_max_wait_time_ms = 0;
  } else if (auto ms = opts.callback_wait_policy.duration_ms()) {
    c.has_callback_max_wait_time_ms = true;
    c.callback_max_wait_time_ms = *ms;
  }
  // Install trampolines only when a callback is set; the Stream keeps the
  // shared_ptr alive so this user_data stays valid. Zero the fields explicitly
  // otherwise rather than trusting the FFI default to leave them null.
  if (opts.ack_callback != nullptr) {
    c.ack_on_ack = zerobus_cpp_ack_on_ack_trampoline;
    c.ack_on_error = zerobus_cpp_ack_on_error_trampoline;
    c.ack_user_data = opts.ack_callback.get();
  } else {
    c.ack_on_ack = nullptr;
    c.ack_on_error = nullptr;
    c.ack_user_data = nullptr;
  }
  return c;
}

/// Build the C Arrow stream-config struct from `ArrowStreamOptions`.
inline CArrowStreamConfigurationOptions to_c(const ArrowStreamOptions& opts) {
  // The core builds a bounded Tokio channel with this capacity
  // (`mpsc::channel(max_inflight_batches)` in
  // `rust/sdk/src/stream/arrow/connection.rs`), which panics
  // on a capacity of 0. Reject it here with an actionable message rather than
  // letting the FFI panic guard surface the opaque panic text.
  if (opts.max_inflight_batches == 0) {
    throw ZerobusException("max_inflight_batches must be at least 1", false);
  }
  CArrowStreamConfigurationOptions c = zerobus_arrow_get_default_config();
  c.max_inflight_batches = opts.max_inflight_batches;
  c.recovery = opts.recovery;
  c.recovery_timeout_ms = opts.recovery_timeout_ms;
  c.recovery_backoff_ms = opts.recovery_backoff_ms;
  c.recovery_retries = opts.recovery_retries;
  c.server_lack_of_ack_timeout_ms = opts.server_lack_of_ack_timeout_ms;
  c.flush_timeout_ms = opts.flush_timeout_ms;
  c.connection_timeout_ms = opts.connection_timeout_ms;
  c.ipc_compression = static_cast<std::int32_t>(opts.ipc_compression);
  // `nullopt` maps to the -1 sentinel ("full server duration"). Reject values
  // above INT64_MAX so they can't wrap to a negative int64 and be misread as
  // -1.
  if (opts.stream_paused_max_wait_time_ms.has_value()) {
    std::uint64_t v = *opts.stream_paused_max_wait_time_ms;
    if (v >
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
      throw ZerobusException(
          "stream_paused_max_wait_time_ms exceeds the maximum supported value",
          false);
    }
    c.stream_paused_max_wait_time_ms = static_cast<std::int64_t>(v);
  } else {
    c.stream_paused_max_wait_time_ms = -1;
  }
  return c;
}

}  // namespace detail
}  // namespace zerobus

#endif  // ZEROBUS_DETAIL_CONFIG_CONVERT_HPP
