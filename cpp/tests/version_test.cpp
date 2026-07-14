// Pins zerobus::version() to the ZEROBUS_CPP_VERSION macro, catching an
// accessor/macro divergence.

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
