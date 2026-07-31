// Pins zerobus::version() to the CMake project(... VERSION ...) — the release
// source of truth, injected as ZEROBUS_CMAKE_PROJECT_VERSION — and to the
// ZEROBUS_CPP_VERSION macro, catching an accessor/macro/CMake divergence.

#include "zerobus/version.hpp"

#include <cstdio>
#include <cstring>

namespace {

int g_failures = 0;

void fail(const char* msg) {
  std::fprintf(stderr, "FAIL: %s\n", msg);
  ++g_failures;
}

}  // namespace

int main() {
  if (std::strcmp(zerobus::version(), ZEROBUS_CPP_VERSION) != 0) {
    fail("version() does not match ZEROBUS_CPP_VERSION");
  }

#ifdef ZEROBUS_CMAKE_PROJECT_VERSION
  // The runtime version must equal the CMake project(VERSION).
  if (std::strcmp(zerobus::version(), ZEROBUS_CMAKE_PROJECT_VERSION) != 0) {
    fail("version() does not match CMake project(VERSION)");
  }
#endif

  // Guard against a blank define.
  if (std::strlen(zerobus::version()) == 0) {
    fail("version() is empty");
  }

  if (g_failures != 0) {
    std::fprintf(stderr, "%d check(s) failed.\n", g_failures);
    return 1;
  }
  std::printf("version() matches ZEROBUS_CPP_VERSION (%s)\n",
              zerobus::version());
  return 0;
}
