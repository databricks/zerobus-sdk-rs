#ifndef ZEROBUS_DETAIL_CONFIG_CONVERT_HPP
#define ZEROBUS_DETAIL_CONFIG_CONVERT_HPP

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

}  // namespace detail
}  // namespace zerobus

#endif  // ZEROBUS_DETAIL_CONFIG_CONVERT_HPP
