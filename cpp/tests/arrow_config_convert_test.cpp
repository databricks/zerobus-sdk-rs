// Exercises the one piece of real logic in to_c(ArrowStreamOptions): the
// stream_paused_max_wait_time_ms optional handling. arrow_config_defaults_test
// only pins the default-constructed (nullopt -> -1) case, so the "value set"
// round-trip and the ">INT64_MAX rejection" branch are covered here.

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

  // INT64_MAX is the largest accepted value: it must not be rejected and must
  // round-trip (not wrap to a negative int64 misread as the -1 sentinel).
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

  // A value above INT64_MAX must throw rather than wrap to a negative int64.
  {
    zerobus::ArrowStreamOptions opts{};
    opts.stream_paused_max_wait_time_ms =
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) + 1;
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

  if (g_failures != 0) {
    std::fprintf(stderr, "%d check(s) failed.\n", g_failures);
    return 1;
  }
  std::printf("arrow config conversion handles the optional wait-time field\n");
  return 0;
}
