// Covers to_c(ArrowStreamOptions)'s client-side validation and optional-field
// handling: the stream_paused_max_wait_time_ms set-value round-trip and
// >INT64_MAX rejection, and the max_inflight_batches == 0 rejection
// (arrow_config_defaults_test only pins the nullopt -> -1 default).

#include <cstdint>
#include <cstdio>
#include <limits>
#include <optional>

#include "detail/config_convert.hpp"
#include "zerobus/error.hpp"

namespace {

int g_failures = 0;

void fail(const char* msg) {
  std::fprintf(stderr, "FAIL: %s\n", msg);
  ++g_failures;
}

}  // namespace

int main() {
  // A set value round-trips unchanged.
  {
    zerobus::ArrowStreamOptions opts{};
    opts.stream_paused_max_wait_time_ms = 5000;
    const zerobus::CArrowStreamConfigurationOptions c =
        zerobus::detail::to_c(opts);
    if (c.stream_paused_max_wait_time_ms != 5000) {
      fail("set stream_paused_max_wait_time_ms did not round-trip to 5000");
    }
  }

  // nullopt maps to the -1 sentinel ("full server duration").
  {
    zerobus::ArrowStreamOptions opts{};  // stream_paused_max_wait_time_ms unset
    const zerobus::CArrowStreamConfigurationOptions c =
        zerobus::detail::to_c(opts);
    if (c.stream_paused_max_wait_time_ms != -1) {
      fail("nullopt stream_paused_max_wait_time_ms did not map to -1 sentinel");
    }
  }

  // INT64_MAX is the largest accepted value: round-trips, not rejected.
  {
    zerobus::ArrowStreamOptions opts{};
    opts.stream_paused_max_wait_time_ms =
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
    bool threw = false;
    try {
      const zerobus::CArrowStreamConfigurationOptions c =
          zerobus::detail::to_c(opts);
      if (c.stream_paused_max_wait_time_ms !=
          std::numeric_limits<std::int64_t>::max()) {
        fail("INT64_MAX stream_paused_max_wait_time_ms did not round-trip");
      }
    } catch (const zerobus::ZerobusException&) {
      threw = true;
    }
    if (threw) {
      fail("INT64_MAX stream_paused_max_wait_time_ms was wrongly rejected");
    }
  }

  // Above INT64_MAX must throw, not wrap to a negative int64.
  {
    zerobus::ArrowStreamOptions opts{};
    const auto too_large =
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) +
        1;
    opts.stream_paused_max_wait_time_ms = too_large;
    bool threw = false;
    try {
      zerobus::detail::to_c(opts);
    } catch (const zerobus::ZerobusException&) {
      threw = true;
    }
    if (!threw) {
      fail("stream_paused_max_wait_time_ms > INT64_MAX was NOT rejected");
    }
  }

  // max_inflight_batches == 0 must be rejected client-side: the core builds a
  // bounded Tokio channel with this capacity, which panics on 0.
  {
    zerobus::ArrowStreamOptions opts{};
    opts.max_inflight_batches = 0;
    bool threw = false;
    try {
      zerobus::detail::to_c(opts);
    } catch (const zerobus::ZerobusException&) {
      threw = true;
    }
    if (!threw) {
      fail("max_inflight_batches == 0 was NOT rejected");
    }
  }

  // A positive max_inflight_batches round-trips unchanged.
  {
    zerobus::ArrowStreamOptions opts{};
    opts.max_inflight_batches = 42;
    const zerobus::CArrowStreamConfigurationOptions c =
        zerobus::detail::to_c(opts);
    if (c.max_inflight_batches != 42) {
      fail("set max_inflight_batches did not round-trip to 42");
    }
  }

  if (g_failures != 0) {
    std::fprintf(stderr, "%d check(s) failed.\n", g_failures);
    return 1;
  }
  std::printf("arrow config conversion handles the optional wait-time field\n");
  return 0;
}
