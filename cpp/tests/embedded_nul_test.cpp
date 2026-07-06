// Exercises detail::checked_c_str, the guard the JSON-ingest and config setters
// route through: a std::string with an embedded NUL must throw rather than be
// silently truncated at the first NUL on the Rust side.

#include <cstdio>
#include <string>

#include "detail/ffi_util.hpp"
#include "zerobus/error.hpp"

namespace {

int g_failures = 0;

void fail(const char* msg) {
  std::fprintf(stderr, "FAIL: %s\n", msg);
  ++g_failures;
}

}  // namespace

int main() {
  // Clean string passes through unchanged.
  {
    const std::string clean("catalog.schema.table");
    bool threw = false;
    try {
      const char* p = zerobus::detail::checked_c_str(clean, "table_name");
      if (std::string(p) != clean) {
        fail("clean string round-tripped to a different value");
      }
    } catch (const zerobus::ZerobusException&) {
      threw = true;
    }
    if (threw) {
      fail("clean string was rejected");
    }
  }

  // Interior NUL (size 11, not a length-5 C string) must be rejected, and the
  // message must name the argument.
  {
    const std::string with_nul("hello\0world", 11);
    if (with_nul.size() != 11) {
      fail("test setup: std::string did not preserve the embedded NUL");
    }
    bool threw = false;
    try {
      zerobus::detail::checked_c_str(with_nul, "JSON record");
    } catch (const zerobus::ZerobusException& e) {
      threw = true;
      if (std::string(e.what()).find("JSON record") == std::string::npos) {
        fail("exception message did not name the offending argument");
      }
    }
    if (!threw) {
      fail(
          "string with embedded NUL was NOT rejected (silent truncation risk)");
    }
  }

  // Trailing NUL counts as embedded too.
  {
    const std::string trailing_nul("abc\0", 4);
    bool threw = false;
    try {
      zerobus::detail::checked_c_str(trailing_nul, "endpoint");
    } catch (const zerobus::ZerobusException&) {
      threw = true;
    }
    if (!threw) {
      fail("string with a trailing embedded NUL was NOT rejected");
    }
  }

  if (g_failures != 0) {
    std::fprintf(stderr, "%d check(s) failed.\n", g_failures);
    return 1;
  }
  std::printf("embedded-NUL guard rejects truncating strings\n");
  return 0;
}
