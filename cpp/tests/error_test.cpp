// ZerobusException: carries the message + is_retryable flag, and is catchable
// as a std::exception.

#include "zerobus/error.hpp"

#include <cstdio>
#include <exception>
#include <string>

namespace {

int g_failures = 0;

void fail(const char* msg) {
  std::fprintf(stderr, "FAIL: %s\n", msg);
  ++g_failures;
}

}  // namespace

int main() {
  using zerobus::ZerobusException;

  // Retryable=true.
  {
    ZerobusException e("transient failure", true);
    if (std::string(e.what()) != "transient failure") {
      fail("retryable exception did not preserve its message");
    }
    if (!e.is_retryable()) {
      fail("is_retryable() should be true");
    }
  }

  // Retryable=false.
  {
    ZerobusException e("permanent failure", false);
    if (std::string(e.what()) != "permanent failure") {
      fail("non-retryable exception did not preserve its message");
    }
    if (e.is_retryable()) {
      fail("is_retryable() should be false");
    }
  }

  // Catchable as std::exception.
  {
    bool caught = false;
    try {
      throw ZerobusException("boom", false);
    } catch (const std::exception& e) {
      caught = true;
      if (std::string(e.what()) != "boom") {
        fail("std::exception::what() did not carry the message");
      }
    }
    if (!caught) {
      fail("ZerobusException was not catchable as std::exception");
    }
  }

  if (g_failures != 0) {
    std::fprintf(stderr, "%d check(s) failed.\n", g_failures);
    return 1;
  }
  std::printf("ZerobusException carries message and retryable flag\n");
  return 0;
}
