// Verifies the C++ headers-provider FFI wiring: the trampoline derefs a
// heap-owned shared_ptr<HeadersProvider> and dispatches get_headers(), the free
// trampoline releases that heap shared_ptr (so the provider is destroyed), and
// both trampolines tolerate null user_data. Dependency-free, like the other
// tests: returns non-zero on failure.
//
// This covers the ownership-transfer mechanism in isolation (free-once, correct
// dispatch, null tolerance) — the FFI, not the Stream, owns the provider. The
// live recovery-vs-teardown race that motivates it is driven from the Rust
// core, where the supervisor task holds the provider Arc across the callback.

#include "zerobus/headers_provider.hpp"

#include <cstdio>
#include <map>
#include <memory>
#include <string>

#include "detail/headers_callback.hpp"

namespace {

int g_failures = 0;

void check(const char* what, bool ok) {
  if (!ok) {
    std::fprintf(stderr, "FAIL: %s\n", what);
    ++g_failures;
  }
}

// Records whether it was destroyed (via a shared flag) and what it returns.
class RecordingProvider : public zerobus::HeadersProvider {
 public:
  explicit RecordingProvider(std::shared_ptr<bool> destroyed)
      : destroyed_(std::move(destroyed)) {}
  ~RecordingProvider() override { *destroyed_ = true; }

  std::map<std::string, std::string> get_headers() override {
    ++calls;
    return {{"authorization", "Bearer test"}};
  }

  int calls = 0;

 private:
  std::shared_ptr<bool> destroyed_;
};

// The trampoline derefs the heap shared_ptr and dispatches get_headers(); the
// free trampoline then destroys the provider.
void test_trampoline_dispatches_and_free_destroys() {
  auto destroyed = std::make_shared<bool>(false);
  auto provider = std::make_shared<RecordingProvider>(destroyed);
  RecordingProvider* raw = provider.get();

  // Mirror Sdk::create_*: hand a heap shared_ptr to the FFI as user_data.
  void* user_data = new std::shared_ptr<zerobus::HeadersProvider>(provider);
  provider.reset();  // Only the heap copy keeps it alive now.
  check("provider alive while FFI owns it", *destroyed == false);

  zerobus::CHeaders headers =
      zerobus::detail::zerobus_cpp_headers_trampoline(user_data);
  check("trampoline reported no error", headers.error_message == nullptr);
  check("trampoline returned one header", headers.count == 1);
  check("get_headers was called", raw->calls == 1);
  zerobus::zerobus_free_headers(headers);

  // The FFI's free callback destroys the provider (last owner dropped).
  zerobus::detail::zerobus_cpp_headers_free(user_data);
  check("free destroyed the provider", *destroyed == true);
}

// A null user_data must be handled, not dereferenced.
void test_null_user_data() {
  zerobus::CHeaders headers =
      zerobus::detail::zerobus_cpp_headers_trampoline(nullptr);
  check("null user_data => error reported", headers.error_message != nullptr);
  zerobus::zerobus_free_headers(headers);

  // Freeing null is a no-op (delete nullptr); reaching the end is the
  // assertion.
  zerobus::detail::zerobus_cpp_headers_free(nullptr);
}

}  // namespace

int main() {
  test_trampoline_dispatches_and_free_destroys();
  test_null_user_data();

  if (g_failures != 0) {
    std::fprintf(stderr, "%d headers-provider check(s) failed.\n", g_failures);
    return 1;
  }
  std::printf("headers provider wiring OK\n");
  return 0;
}
