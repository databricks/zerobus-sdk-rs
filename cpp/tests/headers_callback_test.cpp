// Headers-provider trampoline (detail::zerobus_cpp_headers_trampoline):
// marshals a provider's map into CHeaders; empty/throwing/embedded-NUL/null
// cases become CHeaders.error_message instead of crossing the boundary. Each
// result is freed via zerobus_free_headers, so a sanitizer run also checks the
// alloc/free pairing.

#include "detail/headers_callback.hpp"

#include <cstddef>
#include <cstdio>
#include <map>
#include <memory>
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

// The trampoline's user_data is a heap std::shared_ptr<HeadersProvider>*, owned
// by the FFI (see Sdk::create_*). Mirror that here: run the trampoline against
// a provider wrapped that way, then release it via the FFI's free trampoline.
zerobus::CHeaders run_trampoline(std::shared_ptr<HeadersProvider> provider) {
  auto* owned = new std::shared_ptr<HeadersProvider>(std::move(provider));
  zerobus::CHeaders out =
      zerobus::detail::zerobus_cpp_headers_trampoline(owned);
  zerobus::detail::zerobus_cpp_headers_free(owned);
  return out;
}

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

  // Populated map: key/value pairs round-trip, no error.
  {
    auto provider = std::make_shared<MapProvider>();
    provider->headers = {{"Authorization", "Bearer abc"}, {"X-Custom", "v1"}};

    CHeaders out = run_trampoline(provider);
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

  // Empty map: null array, zero count, no error.
  {
    CHeaders out =
        run_trampoline(std::make_shared<MapProvider>());  // no headers
    if (out.count != 0 || out.headers != nullptr ||
        out.error_message != nullptr) {
      fail("empty provider did not yield an empty, error-free CHeaders");
    }
    zerobus_free_headers(out);
  }

  // Throwing provider: message surfaces via error_message, no exception
  // escapes.
  {
    CHeaders out = run_trampoline(std::make_shared<ThrowingProvider>());
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

  // Embedded-NUL header value is rejected, not truncated.
  {
    auto provider = std::make_shared<MapProvider>();
    provider->headers = {{"X-Bad", std::string("a\0b", 3)}};
    CHeaders out = run_trampoline(provider);
    if (out.error_message == nullptr) {
      fail("embedded-NUL header value was not rejected");
    }
    if (out.headers != nullptr) {
      fail("embedded-NUL header still returned a header array");
    }
    zerobus_free_headers(out);
  }

  // Null user_data: error, not a crash.
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
