#ifndef ZEROBUS_DETAIL_CONFIG_CONVERT_HPP
#define ZEROBUS_DETAIL_CONFIG_CONVERT_HPP

#include "detail/ffi_util.hpp"
#include "zerobus/config.hpp"

namespace zerobus {
namespace detail {

/// Build the C stream-config struct. Seeds from the live FFI defaults (so any
/// field this wrapper doesn't know about keeps its default) then overrides each
/// known field. Unlike the Go wrapper there is no zero-value ambiguity for
/// `recovery`: it is always written explicitly.
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
  // Only override when set. On nullopt we deliberately leave the presence flag
  // and value seeded from zerobus_get_default_config() untouched, so the FFI
  // default stays in place (as the header documents). Clearing the flag here
  // would instead force Rust to interpret it as None (wait indefinitely).
  if (opts.callback_max_wait_time_ms.has_value()) {
    c.has_callback_max_wait_time_ms = true;
    c.callback_max_wait_time_ms = *opts.callback_max_wait_time_ms;
  }
  return c;
}

}  // namespace detail
}  // namespace zerobus

#endif  // ZEROBUS_DETAIL_CONFIG_CONVERT_HPP
