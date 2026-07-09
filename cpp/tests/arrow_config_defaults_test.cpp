// Arrow analogue of config_defaults_test: `to_c()` overwrites every scalar with
// the C++ literal, so a stale literal would silently ship a wrong default. This
// pins `to_c(ArrowStreamOptions{})` to `zerobus_arrow_get_default_config()`,
// turning drift into a build failure.

#include <cstdint>
#include <cstdio>

#include "detail/config_convert.hpp"

namespace {

int g_failures = 0;

// Report but don't abort, so one run lists every drifted field.
// Signed int64 (not uint64): the -1 sentinels (ipc_compression,
// stream_paused_max_wait_time_ms) compare cleanly, and every field is
// < INT64_MAX.
template <typename A, typename B>
void check_eq(const char* field, A actual, B expected) {
  if (static_cast<std::int64_t>(actual) !=
      static_cast<std::int64_t>(expected)) {
    std::fprintf(stderr,
                 "FAIL: %s: ArrowStreamOptions{} default (%lld) != "
                 "zerobus_arrow_get_default_config() (%lld)\n",
                 field, static_cast<long long>(actual),
                 static_cast<long long>(expected));
    ++g_failures;
  }
}

}  // namespace

int main() {
  const zerobus::ArrowStreamOptions opts{};
  const zerobus::CArrowStreamConfigurationOptions from_cpp =
      zerobus::detail::to_c(opts);
  const zerobus::CArrowStreamConfigurationOptions from_ffi =
      zerobus::zerobus_arrow_get_default_config();

  check_eq("max_inflight_batches", from_cpp.max_inflight_batches,
           from_ffi.max_inflight_batches);
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
  check_eq("connection_timeout_ms", from_cpp.connection_timeout_ms,
           from_ffi.connection_timeout_ms);
  check_eq("ipc_compression", from_cpp.ipc_compression,
           from_ffi.ipc_compression);
  check_eq("stream_paused_max_wait_time_ms",
           from_cpp.stream_paused_max_wait_time_ms,
           from_ffi.stream_paused_max_wait_time_ms);

  if (g_failures != 0) {
    std::fprintf(stderr, "%d field(s) diverged from the FFI defaults.\n",
                 g_failures);
    return 1;
  }
  std::printf("arrow config defaults match FFI defaults\n");
  return 0;
}
