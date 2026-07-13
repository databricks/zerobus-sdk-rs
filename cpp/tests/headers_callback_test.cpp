// Unit tests for the headers-provider trampoline
// (detail::zerobus_cpp_headers_trampoline): a provider's map is marshalled into
// CHeaders, the empty case yields a null array, and a throwing provider, a
// header with an embedded NUL, or a null user_data become a
// CHeaders.error_message rather than crossing the FFI boundary as an exception
// or corrupt metadata. Each result is freed with the Rust core's
// zerobus_free_headers, exactly as it would be in production, so the suite run
// under a sanitizer also checks the alloc/free pairing.
//
// This mirrors the already-tested ack-callback trampoline; it is the headers
// side of the same extern "C" boundary.

#include "detail/headers_callback.hpp"

#include <cstddef>
#include <cstdio>
#include <map>
#include <stdexcept>
#include <string>

#include "zerobus/headers_provider.hpp"

namespace {

int g_failures = 0;

void fail(const char* msg) {
  std::fprintf(stderr, "FAIL: %s\n", msg);
  ++g_failures;
}

using zerobus::HeadersProvider;

// Returns a caller-set map.
class MapProvider : public HeadersProvider {
 public:
  std::map<std::string, std::string> headers;
  std::map<std::string, std::string> get_headers() override { return headers; }
};

// Throws, to exercise the exception -> error_message path.
class ThrowingProvider : public HeadersProvider {
 public:
  std::map<std::string, std::string> get_headers() override {
    throw std::runtime_error("provider boom");
  }
};

}  // namespace

int main() {
  using zerobus::CHeaders;
  using zerobus::zerobus_free_headers;
  using zerobus::detail::zerobus_cpp_headers_trampoline;

  // A populated map marshals into a CHeaders with matching key/value pairs and
  // no error.
  {
    MapProvider provider;
    provider.headers = {{"Authorization", "Bearer abc"}, {"X-Custom", "v1"}};

    CHeaders out = zerobus_cpp_headers_trampoline(&provider);
    if (out.error_message != nullptr) {
      fail("marshalling valid headers set an error_message");
    }
    if (out.count != 2 || out.headers == nullptr) {
      fail("marshalled header count/array is wrong");
    } else {
      std::map<std::string, std::string> got;
      for (std::size_t i = 0; i < out.count; ++i) {
        if (out.headers[i].key == nullptr || out.headers[i].value == nullptr) {
          fail("marshalled header had a null key or value");
        } else {
          got[out.headers[i].key] = out.headers[i].value;
        }
      }
      if (got["Authorization"] != "Bearer abc" || got["X-Custom"] != "v1") {
        fail("marshalled header values did not round-trip");
      }
    }
    // Freed by the Rust core's allocator, exactly as in production.
    zerobus_free_headers(out);
  }

  // An empty map yields a null array and zero count, with no error.
  {
    MapProvider provider;  // no headers
    CHeaders out = zerobus_cpp_headers_trampoline(&provider);
    if (out.count != 0 || out.headers != nullptr ||
        out.error_message != nullptr) {
      fail("empty provider did not yield an empty, error-free CHeaders");
    }
    zerobus_free_headers(out);
  }

  // A throwing provider is caught and its message surfaces via error_message;
  // the exception never crosses the FFI boundary.
  {
    ThrowingProvider provider;
    CHeaders out = zerobus_cpp_headers_trampoline(&provider);
    if (out.error_message == nullptr) {
      fail("throwing provider did not set an error_message");
    } else if (std::string(out.error_message).find("provider boom") ==
               std::string::npos) {
      fail("error_message did not carry the exception text");
    }
    if (out.headers != nullptr) {
      fail("throwing provider still returned a header array");
    }
    zerobus_free_headers(out);
  }

  // A header value with an embedded NUL is rejected (it would be silently
  // truncated on the Rust side) rather than marshalled.
  {
    MapProvider provider;
    provider.headers = {{"X-Bad", std::string("a\0b", 3)}};
    CHeaders out = zerobus_cpp_headers_trampoline(&provider);
    if (out.error_message == nullptr) {
      fail("embedded-NUL header value was not rejected");
    }
    if (out.headers != nullptr) {
      fail("embedded-NUL header still returned a header array");
    }
    zerobus_free_headers(out);
  }

  // A null user_data (no provider) is reported as an error, not a crash.
  {
    CHeaders out = zerobus_cpp_headers_trampoline(nullptr);
    if (out.error_message == nullptr) {
      fail("null user_data did not set an error_message");
    }
    zerobus_free_headers(out);
  }

  if (g_failures != 0) {
    std::fprintf(stderr, "%d check(s) failed.\n", g_failures);
    return 1;
  }
  std::printf("headers trampoline marshals, guards, and reports errors\n");
  return 0;
}
