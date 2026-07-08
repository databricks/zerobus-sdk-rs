// Guards against divergence between the three places stream-config defaults
// live: the Rust core `StreamConfigurationOptions::default()`, the FFI
// `zerobus_get_default_config()`, and this SDK's `StreamOptions` member
// initializers (config.hpp).
//
// `to_c()` seeds from the live FFI defaults and then unconditionally overwrites
// every scalar field with the C++ struct's value. So for a default-constructed
// `StreamOptions{}`, the value actually sent across the FFI is the C++ literal,
// NOT the FFI seed — the seed is a no-op for scalars. If someone changes a
// default in the Rust core (which the FFI tracks via shared `defaults::`
// constants) but forgets to update the C++ literal, the wrapper would silently
// keep sending the stale value with no compile error.
//
// This test pins that invariant: a default-constructed `StreamOptions` must
// convert to exactly what `zerobus_get_default_config()` returns. It turns
// silent drift into a build failure. It does NOT guard the two fields the FFI
// itself hardcodes as literals rather than reading from the core `Default`
// impl (`max_inflight_requests`, `record_type`); closing that gap requires an
// FFI-side change (presence flags for scalars) tracked separately.

#include <cstdint>
#include <cstdio>

#include "detail/config_convert.hpp"

namespace {

int g_failures = 0;

// Report but don't abort, so a single run lists every drifted field at once.
template <typename A, typename B>
void check_eq(const char* field, A actual, B expected) {
  if (static_cast<std::uint64_t>(actual) !=
      static_cast<std::uint64_t>(expected)) {
    std::fprintf(stderr,
                 "FAIL: %s: StreamOptions{} default (%llu) != "
                 "zerobus_get_default_config() (%llu)\n",
                 field, static_cast<unsigned long long>(actual),
                 static_cast<unsigned long long>(expected));
    ++g_failures;
  }
}

}  // namespace

int main() {
  const zerobus::StreamOptions opts{};
  const zerobus::CStreamConfigurationOptions from_cpp =
      zerobus::detail::to_c(opts);
  const zerobus::CStreamConfigurationOptions from_ffi =
      zerobus::zerobus_get_default_config();

  check_eq("max_inflight_requests", from_cpp.max_inflight_requests,
           from_ffi.max_inflight_requests);
  check_eq("recovery", from_cpp.recovery, from_ffi.recovery);
  check_eq("recovery_timeout_ms", from_cpp.recovery_timeout_ms,
           from_ffi.recovery_timeout_ms);
  check_eq("recovery_backoff_ms", from_cpp.recovery_backoff_ms,
           from_ffi.recovery_backoff_ms);
  check_eq("recovery_retries", from_cpp.recovery_retries,
           from_ffi.recovery_retries);
  check_eq("server_lack_of_ack_timeout_ms",
           from_cpp.server_lack_of_ack_timeout_ms,
           from_ffi.server_lack_of_ack_timeout_ms);
  check_eq("flush_timeout_ms", from_cpp.flush_timeout_ms,
           from_ffi.flush_timeout_ms);
  check_eq("record_type", from_cpp.record_type, from_ffi.record_type);

  // Optionals: a default StreamOptions leaves both unset. stream_paused is
  // written as absent; callback_max_wait_time_ms is left at the FFI seed
  // (which is present). Either way the presence flag and value must match the
  // FFI default struct.
  check_eq("has_stream_paused_max_wait_time_ms",
           from_cpp.has_stream_paused_max_wait_time_ms,
           from_ffi.has_stream_paused_max_wait_time_ms);
  check_eq("stream_paused_max_wait_time_ms",
           from_cpp.stream_paused_max_wait_time_ms,
           from_ffi.stream_paused_max_wait_time_ms);
  check_eq("has_callback_max_wait_time_ms",
           from_cpp.has_callback_max_wait_time_ms,
           from_ffi.has_callback_max_wait_time_ms);
  check_eq("callback_max_wait_time_ms", from_cpp.callback_max_wait_time_ms,
           from_ffi.callback_max_wait_time_ms);

  // A default StreamOptions has no callback, so the three ack fields stay null.
  if (from_cpp.ack_on_ack != nullptr) {
    std::fprintf(stderr, "FAIL: ack_on_ack: default should be null\n");
    ++g_failures;
  }
  if (from_cpp.ack_on_error != nullptr) {
    std::fprintf(stderr, "FAIL: ack_on_error: default should be null\n");
    ++g_failures;
  }
  if (from_cpp.ack_user_data != nullptr) {
    std::fprintf(stderr, "FAIL: ack_user_data: default should be null\n");
    ++g_failures;
  }

  if (g_failures != 0) {
    std::fprintf(stderr, "%d field(s) diverged from the FFI defaults.\n",
                 g_failures);
    return 1;
  }
  std::printf("config defaults match FFI defaults\n");
  return 0;
}
